package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/nelthaarion/breeze"
	middleware "github.com/nelthaarion/breeze/middlewares"

	"github.com/lib/pq"
)

var db *sql.DB

// Active chat WebSocket connections and their subscribed conversation.
// WSConn.SendText is safe to call from concurrent goroutines in Breeze.
var chatWS = struct {
	sync.RWMutex
	clients map[*breeze.WSConn]string
}{
	clients: make(map[*breeze.WSConn]string),
}

// ============================================================
// Configuration
// ============================================================

const (
	MaxImageSize = 5 * 1024 * 1024  // 5 MB
	MaxFileSize  = 10 * 1024 * 1024 // 10 MB
)

var uuidRegexp = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// isValidUUID avoids Postgres "invalid input syntax for type uuid" (a 500)
// by rejecting malformed ids up front with a 400.
func isValidUUID(s string) bool {
	return uuidRegexp.MatchString(s)
}

// fail writes a JSON error with the right HTTP status.
func fail(ctx *breeze.Context, status int, msg string, err error) {
	ctx.Status(status)
	body := map[string]interface{}{"error": msg}
	if err != nil {
		body["details"] = err.Error()
	}
	ctx.JSON(body)
}

// ============================================================
// Main
// ============================================================

func main() {

	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found")
	}

	if err := InitDB(); err != nil {
		log.Fatal("Database connection failed:", err)
	}
	defer db.Close()

	if err := CreateTables(); err != nil {
		log.Fatal("Database migration failed:", err)
	}

	log.Println("Database connected successfully")

	router := breeze.NewRouter()

	router.Use(middleware.RecoveryMiddleware())
	router.Use(middleware.LoggingMiddleware())

	router.Handle(breeze.GET, "/", func(ctx *breeze.Context) {
		ctx.JSON(map[string]interface{}{
			"message": "Doctor API",
			"status":  "ok",
		})
	})

	router.Handle(breeze.GET, "/health", func(ctx *breeze.Context) {
		ctx.JSON(map[string]interface{}{
			"status": "healthy",
		})
	})

	// ---------- Doctors ----------
	router.HandleBlocking(breeze.POST, "/doctors", CreateDoctor)
	router.HandleBlocking(breeze.GET, "/doctors", GetDoctors)
	router.HandleBlocking(breeze.GET, "/doctors/:id", GetDoctor)
	router.HandleBlocking(breeze.PUT, "/doctors/:id", UpdateDoctor)
	router.HandleBlocking(breeze.DELETE, "/doctors/:id", DeleteDoctor)

	// Static segment, so it wins over "/doctors/:id" for POST lookups.
	router.HandleBlocking(breeze.POST, "/doctors/login", LoginDoctor)

	router.HandleBlocking(breeze.PATCH, "/doctors/:id/status", UpdateDoctorStatus)

	// The SvelteKit dashboard proxies this as POST /api/doctors/:id/chat-image.
	router.HandleBlocking(breeze.POST, "/doctors/:id/chat-image", SaveDoctorChatImage)

	// ---------- Patients ----------
	router.HandleBlocking(breeze.POST, "/patients", CreatePatient)

	// Static segment wins over "/patients/:id" for POST lookups.
	router.HandleBlocking(breeze.POST, "/patients/login", LoginPatient)

	router.HandleBlocking(breeze.GET, "/patients", GetPatients)
	router.HandleBlocking(breeze.GET, "/patients/:id", GetPatient)
	router.HandleBlocking(breeze.PUT, "/patients/:id", UpdatePatient)
	router.HandleBlocking(breeze.DELETE, "/patients/:id", DeletePatient)

	// The SvelteKit dashboard proxies this as POST /api/patients/:id/chat-image.
	router.HandleBlocking(breeze.POST, "/patients/:id/chat-image", SavePatientChatImage)

	// ---------- Work Places ----------
	router.HandleBlocking(breeze.POST, "/work-places", CreateWorkPlace)
	router.HandleBlocking(breeze.GET, "/work-places", GetWorkPlaces)
	router.HandleBlocking(breeze.GET, "/work-places/:id", GetWorkPlace)
	router.HandleBlocking(breeze.PUT, "/work-places/:id", UpdateWorkPlace)
	router.HandleBlocking(breeze.DELETE, "/work-places/:id", DeleteWorkPlace)
	router.HandleBlocking(breeze.POST, "/work-places/:id/schedules", SetWorkSchedules)

	// ---------- Chat ----------
	router.HandleBlocking(breeze.POST, "/conversations", CreateConversation)
	router.HandleBlocking(breeze.GET, "/conversations", GetConversations)
	router.HandleBlocking(breeze.GET, "/conversations/:id/messages", GetMessages)
	router.HandleBlocking(breeze.POST, "/conversations/:id/messages", SendMessage)
	router.HandleBlocking(breeze.POST, "/conversations/:id/messages/image", SendImageMessage)

	// Clients connect to ws://HOST:PORT/ws/chat and first send a "subscribe" message.
	chatHandler := &breeze.WSHandlerFunc{
		Connect: chatWSOnConnect,
		Message: chatWSOnMessage,
		Close:   chatWSOnClose,
	}

	pool := breeze.NewWorkerPool(runtime.NumCPU())
	app := breeze.New(router, pool)
	app.WebSocket("/ws/chat", chatHandler)

	port := 3000
	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			port = p
		}
	}

	log.Printf("Server running on port %d", port)

	if err := app.Run(port, true); err != nil {
		log.Fatal(err)
	}
}

// ============================================================
// Database
// ============================================================

func InitDB() error {

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}

	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil {
		return err
	}

	return db.Ping()
}

// ============================================================
// Create Tables
// ============================================================

// All DDL below is idempotent, so CreateTables can run on every boot without
// touching rows that already exist.
//
// IMPORTANT: "CREATE TABLE IF NOT EXISTS" does nothing when the table already
// exists. Whenever a column is added to a CREATE TABLE statement, a matching
// "ALTER TABLE ... ADD COLUMN IF NOT EXISTS" migration must be added too, or
// older databases will keep the old schema.
var (
	// gen_random_uuid() is built in on PostgreSQL 13+; older versions need pgcrypto.
	// This may fail without superuser rights, so CreateTables only logs the error.
	pgcryptoDDL = `CREATE EXTENSION IF NOT EXISTS pgcrypto;`

	doctorsDDL = `
	CREATE TABLE IF NOT EXISTS doctors (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

		profile_image TEXT,

		full_name VARCHAR(150) NOT NULL,
		phone VARCHAR(20) NOT NULL UNIQUE,
		email VARCHAR(255) UNIQUE,
		national_id VARCHAR(20) NOT NULL UNIQUE,
		medical_syndicate_id VARCHAR(50) NOT NULL UNIQUE,
		birth_date DATE NOT NULL,

		specialty VARCHAR(100) NOT NULL,

		professional_degree VARCHAR(20) NOT NULL
			CHECK (
				professional_degree IN (
					'ممارس',
					'أخصائي',
					'استشاري'
				)
			),

		governorate VARCHAR(100) NOT NULL
			CHECK (
				governorate IN (
					'القاهرة',
					'الجيزة',
					'الإسكندرية',
					'الدقهلية',
					'أسيوط',
					'الشرقية',
					'الغربية',
					'المنوفية',
					'القليوبية',
					'أسوان',
					'الأقصر',
					'بورسعيد',
					'السويس',
					'الإسماعيلية',
					'دمياط',
					'كفر الشيخ',
					'البحيرة',
					'المنيا',
					'بني سويف',
					'الفيوم',
					'سوهاج',
					'قنا',
					'البحر الأحمر',
					'الوادي الجديد',
					'مطروح',
					'شمال سيناء',
					'جنوب سيناء'
				)
			),

		medical_syndicate_card TEXT NOT NULL,
		national_id_card TEXT NOT NULL,
		specialty_certificate TEXT,

		is_online BOOLEAN NOT NULL DEFAULT FALSE,

		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`

	// Idempotent migration: adds is_online to existing doctors tables.
	doctorsAddIsOnlineDDL = `
	ALTER TABLE doctors
		ADD COLUMN IF NOT EXISTS is_online BOOLEAN NOT NULL DEFAULT FALSE;
	`

	patientsDDL = `
	CREATE TABLE IF NOT EXISTS patients (
		id BIGSERIAL PRIMARY KEY,
		username VARCHAR(100) NOT NULL UNIQUE,
		phone VARCHAR(20) NOT NULL UNIQUE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`

	// Idempotent migration: adds photo to existing patients tables.
	patientsPhotoDDL = `
	ALTER TABLE patients
		ADD COLUMN IF NOT EXISTS photo TEXT;
	`

	conversationsDDL = `
	CREATE TABLE IF NOT EXISTS conversations (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		patient_id BIGINT NOT NULL REFERENCES patients(id) ON DELETE CASCADE,
		doctor_id UUID NOT NULL REFERENCES doctors(id) ON DELETE CASCADE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		UNIQUE(patient_id, doctor_id)
	);
	`

	conversationsAddColumnsDDL = `
	ALTER TABLE conversations
		ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
	`

	// first_message holds the JSON payload {"image": "<data URL>"} of the
	// opening picture of the conversation.
	conversationsFirstMessageDDL = `
	ALTER TABLE conversations
		ADD COLUMN IF NOT EXISTS first_message JSONB;
	`

	conversationsUpdatedIndexDDL = `
	CREATE INDEX IF NOT EXISTS idx_conversations_updated
	ON conversations(updated_at DESC);
	`

	messagesDDL = `
	CREATE TABLE IF NOT EXISTS messages (
		id BIGSERIAL PRIMARY KEY,
		conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
		sender_type VARCHAR(20) NOT NULL CHECK (sender_type IN ('patient', 'doctor')),
		sender_id VARCHAR(100) NOT NULL,
		message TEXT,
		image_data TEXT,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		CHECK (NULLIF(TRIM(message), '') IS NOT NULL OR image_data IS NOT NULL)
	);
	`

	messagesAddColumnsDDL = `
	ALTER TABLE messages
		ADD COLUMN IF NOT EXISTS image_data TEXT;
	`

	messagesIndexDDL = `
	CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
	ON messages(conversation_id, created_at, id);
	`

	workPlacesDDL = `
	CREATE TABLE IF NOT EXISTS work_places (
		id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		doctor_id   UUID NOT NULL REFERENCES doctors(id) ON DELETE CASCADE,

		place_type  VARCHAR(30) NOT NULL
			CHECK (place_type IN ('عيادة خاصة', 'مستشفى', 'مركز طبي', 'مستوصف')),

		name        VARCHAR(200) NOT NULL,
		address     TEXT NOT NULL,
		phone       VARCHAR(20),
		exam_price  NUMERIC(10, 2),

		created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`

	// One row = one time slot for one day (0 = السبت ... 6 = الجمعة).
	// A day can have two rows (two periods) or an is_off row (day off).
	workSchedulesDDL = `
	CREATE TABLE IF NOT EXISTS work_schedules (
		id            BIGSERIAL PRIMARY KEY,
		work_place_id UUID NOT NULL REFERENCES work_places(id) ON DELETE CASCADE,

		day_of_week   SMALLINT NOT NULL
			CHECK (day_of_week BETWEEN 0 AND 6),

		is_off        BOOLEAN NOT NULL DEFAULT FALSE,

		from_time     TIME,
		to_time       TIME,

		CONSTRAINT work_schedules_times_check
			CHECK (
				is_off = TRUE
				OR (from_time IS NOT NULL AND to_time IS NOT NULL AND from_time < to_time)
			)
	);
	`

	workSchedulesIndexDDL = `
	CREATE INDEX IF NOT EXISTS idx_work_schedules_place_day
	ON work_schedules(work_place_id, day_of_week);
	`
)

func CreateTables() error {

	// Best effort: only needed on PostgreSQL < 13.
	if _, err := db.Exec(pgcryptoDDL); err != nil {
		log.Println("pgcrypto extension not enabled (ignore on PostgreSQL 13+):", err)
	}

	queries := []string{
		doctorsDDL,
		doctorsAddIsOnlineDDL,
		patientsDDL,
		patientsPhotoDDL,
		conversationsDDL,
		conversationsAddColumnsDDL,
		conversationsFirstMessageDDL,
		conversationsUpdatedIndexDDL,
		messagesDDL,
		messagesAddColumnsDDL,
		messagesIndexDDL,
		workPlacesDDL,
		workSchedulesDDL,
		workSchedulesIndexDDL,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

// ============================================================
// Types
// ============================================================

type Doctor struct {
	ID                   string  `json:"id"`
	ProfileImage         *string `json:"profile_image,omitempty"`
	FullName             string  `json:"full_name"`
	Phone                string  `json:"phone"`
	Email                *string `json:"email,omitempty"`
	NationalID           string  `json:"national_id"`
	MedicalSyndicateID   string  `json:"medical_syndicate_id"`
	BirthDate            string  `json:"birth_date"`
	Specialty            string  `json:"specialty"`
	ProfessionalDegree   string  `json:"professional_degree"`
	Governorate          string  `json:"governorate"`
	MedicalSyndicateCard string  `json:"medical_syndicate_card"`
	NationalIDCard       string  `json:"national_id_card"`
	SpecialtyCertificate *string `json:"specialty_certificate,omitempty"`
	IsOnline             bool    `json:"is_online"`
	OnlineStatus         string  `json:"online_status"`
	CreatedAt            string  `json:"created_at"`
	UpdatedAt            string  `json:"updated_at"`
}

// WorkPlace represents one clinic or hospital a doctor works at.
type WorkPlace struct {
	ID        string         `json:"id"`
	DoctorID  string         `json:"doctor_id"`
	PlaceType string         `json:"place_type"`
	Name      string         `json:"name"`
	Address   string         `json:"address"`
	Phone     *string        `json:"phone,omitempty"`
	ExamPrice *float64       `json:"exam_price,omitempty"`
	Schedules []WorkSchedule `json:"schedules,omitempty"`
	CreatedAt string         `json:"created_at"`
	UpdatedAt string         `json:"updated_at"`
}

// WorkSchedule represents one time slot for a specific day of the week.
type WorkSchedule struct {
	ID          int64   `json:"id"`
	WorkPlaceID string  `json:"work_place_id"`
	DayOfWeek   int     `json:"day_of_week"` // 0=السبت … 6=الجمعة
	DayName     string  `json:"day_name"`
	IsOff       bool    `json:"is_off"`
	FromTime    *string `json:"from_time,omitempty"` // "HH:MM"
	ToTime      *string `json:"to_time,omitempty"`   // "HH:MM"
}

// CreateWorkPlaceRequest is the JSON body for POST /work-places.
type CreateWorkPlaceRequest struct {
	DoctorID  string              `json:"doctor_id"`
	PlaceType string              `json:"place_type"`
	Name      string              `json:"name"`
	Address   string              `json:"address"`
	Phone     string              `json:"phone"`
	ExamPrice *float64            `json:"exam_price"`
	Schedules []WorkScheduleInput `json:"schedules"`
}

// UpdateWorkPlaceRequest is the JSON body for PUT /work-places/:id.
type UpdateWorkPlaceRequest struct {
	PlaceType string   `json:"place_type"`
	Name      string   `json:"name"`
	Address   string   `json:"address"`
	Phone     string   `json:"phone"`
	ExamPrice *float64 `json:"exam_price"`
}

// WorkScheduleInput carries one time slot for a day.
// Send is_off = true (and omit from/to) to mark the day as unavailable.
type WorkScheduleInput struct {
	DayOfWeek int    `json:"day_of_week"`
	IsOff     bool   `json:"is_off"`
	FromTime  string `json:"from_time"`
	ToTime    string `json:"to_time"`
}

// arabicDayName maps 0-indexed day (0=Saturday) to the Arabic label.
func arabicDayName(d int) string {
	names := []string{
		"السبت", "الأحد", "الاثنين", "الثلاثاء",
		"الأربعاء", "الخميس", "الجمعة",
	}
	if d >= 0 && d < len(names) {
		return names[d]
	}
	return ""
}

// doctorOnlineStatus returns the human-readable Arabic status label.
func doctorOnlineStatus(isOnline bool) string {
	if isOnline {
		return "أنت أونلاين متصل\n🟢 متاح للاستشارة الفورية"
	}
	return "🔴 غير متاح حالياً"
}

// ============================================================
// Create Doctor
// ============================================================

func CreateDoctor(ctx *breeze.Context) {

	files, fields, err := ctx.ParseMultipart(MaxFileSize)
	if err != nil {
		fail(ctx, 400, "Invalid multipart form", err)
		return
	}

	formValue := func(name string) string {
		if values, ok := fields[name]; ok && len(values) > 0 {
			return values[0]
		}
		return ""
	}

	fullName := formValue("full_name")
	phone := formValue("phone")
	email := formValue("email")
	nationalID := formValue("national_id")
	medicalSyndicateID := formValue("medical_syndicate_id")
	birthDate := formValue("birth_date")
	specialty := formValue("specialty")
	professionalDegree := formValue("professional_degree")
	governorate := formValue("governorate")

	required := map[string]string{
		"full_name":            fullName,
		"phone":                phone,
		"national_id":          nationalID,
		"medical_syndicate_id": medicalSyndicateID,
		"birth_date":           birthDate,
		"specialty":            specialty,
		"professional_degree":  professionalDegree,
		"governorate":          governorate,
	}

	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			fail(ctx, 400, field+" is required", nil)
			return
		}
	}

	profileImage, err := readUploadedFile(files, "profile_image", MaxImageSize,
		[]string{"image/jpeg", "image/png", "image/webp"}, false)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	syndicateCard, err := readUploadedFile(files, "medical_syndicate_card", MaxFileSize,
		[]string{"image/jpeg", "image/png", "application/pdf"}, true)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	nationalIDCard, err := readUploadedFile(files, "national_id_card", MaxFileSize,
		[]string{"image/jpeg", "image/png", "application/pdf"}, true)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	specialtyCertificate, err := readUploadedFile(files, "specialty_certificate", MaxFileSize,
		[]string{"image/jpeg", "image/png", "application/pdf"}, false)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	query := `
		INSERT INTO doctors (
			profile_image,
			full_name,
			phone,
			email,
			national_id,
			medical_syndicate_id,
			birth_date,
			specialty,
			professional_degree,
			governorate,
			medical_syndicate_card,
			national_id_card,
			specialty_certificate
		)
		VALUES (
			$1, $2, $3, NULLIF($4, ''),
			$5, $6, $7, $8, $9, $10,
			$11, $12, $13
		)
		RETURNING id, created_at, updated_at
	`

	var id string
	var createdAt, updatedAt time.Time

	err = db.QueryRow(
		query,
		profileImage,
		fullName,
		phone,
		strings.TrimSpace(email),
		nationalID,
		medicalSyndicateID,
		birthDate,
		specialty,
		professionalDegree,
		governorate,
		syndicateCard,
		nationalIDCard,
		specialtyCertificate,
	).Scan(&id, &createdAt, &updatedAt)

	if err != nil {
		if isUniqueViolation(err) {
			fail(ctx, 409, "Phone, email, national_id or medical_syndicate_id already exists", nil)
			return
		}
		fail(ctx, 500, "Failed to create doctor", err)
		return
	}

	ctx.Status(201)
	ctx.JSON(map[string]interface{}{
		"message": "Doctor registered successfully",
		"doctor": map[string]interface{}{
			"id":         id,
			"full_name":  fullName,
			"created_at": createdAt,
			"updated_at": updatedAt,
		},
	})
}

// ============================================================
// Get Doctors
// ============================================================

func GetDoctors(ctx *breeze.Context) {

	rows, err := db.Query(`
		SELECT
			id,
			full_name,
			phone,
			email,
			national_id,
			medical_syndicate_id,
			birth_date,
			specialty,
			professional_degree,
			governorate,
			is_online,
			created_at,
			updated_at
		FROM doctors
		ORDER BY created_at DESC
	`)
	if err != nil {
		fail(ctx, 500, "Failed to load doctors", err)
		return
	}
	defer rows.Close()

	doctors := []Doctor{}

	for rows.Next() {

		var d Doctor
		var email sql.NullString
		var createdAt, updatedAt time.Time

		err := rows.Scan(
			&d.ID,
			&d.FullName,
			&d.Phone,
			&email,
			&d.NationalID,
			&d.MedicalSyndicateID,
			&d.BirthDate,
			&d.Specialty,
			&d.ProfessionalDegree,
			&d.Governorate,
			&d.IsOnline,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			fail(ctx, 500, "Failed to read doctors", err)
			return
		}

		if email.Valid {
			d.Email = &email.String
		}

		d.OnlineStatus = doctorOnlineStatus(d.IsOnline)
		d.CreatedAt = createdAt.Format(time.RFC3339)
		d.UpdatedAt = updatedAt.Format(time.RFC3339)

		doctors = append(doctors, d)
	}

	if err := rows.Err(); err != nil {
		fail(ctx, 500, "Failed to read doctors", err)
		return
	}

	ctx.JSON(map[string]interface{}{
		"count":   len(doctors),
		"doctors": doctors,
	})
}

// ============================================================
// Get Doctor
// ============================================================

func GetDoctor(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid doctor id", nil)
		return
	}

	var d Doctor
	var email sql.NullString
	var createdAt, updatedAt time.Time

	err := db.QueryRow(`
		SELECT
			id,
			profile_image,
			full_name,
			phone,
			email,
			national_id,
			medical_syndicate_id,
			birth_date,
			specialty,
			professional_degree,
			governorate,
			medical_syndicate_card,
			national_id_card,
			specialty_certificate,
			is_online,
			created_at,
			updated_at
		FROM doctors
		WHERE id = $1
	`, id).Scan(
		&d.ID,
		&d.ProfileImage,
		&d.FullName,
		&d.Phone,
		&email,
		&d.NationalID,
		&d.MedicalSyndicateID,
		&d.BirthDate,
		&d.Specialty,
		&d.ProfessionalDegree,
		&d.Governorate,
		&d.MedicalSyndicateCard,
		&d.NationalIDCard,
		&d.SpecialtyCertificate,
		&d.IsOnline,
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}
	if err != nil {
		fail(ctx, 500, "Failed to load doctor", err)
		return
	}

	if email.Valid {
		d.Email = &email.String
	}

	d.OnlineStatus = doctorOnlineStatus(d.IsOnline)
	d.CreatedAt = createdAt.Format(time.RFC3339)
	d.UpdatedAt = updatedAt.Format(time.RFC3339)

	ctx.JSON(map[string]interface{}{
		"doctor": d,
	})
}

// ============================================================
// Login Doctor
// ============================================================

// LoginRequest: both fields are matched case-insensitively and
// whitespace-trimmed, because the values are typed by hand.
type LoginRequest struct {
	FullName string `json:"full_name"`
	Email    string `json:"email"`
}

func LoginDoctor(ctx *breeze.Context) {

	var payload LoginRequest

	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	fullName := strings.TrimSpace(payload.FullName)
	email := strings.ToLower(strings.TrimSpace(payload.Email))

	if fullName == "" {
		fail(ctx, 400, "full_name is required", nil)
		return
	}
	if email == "" {
		fail(ctx, 400, "email is required", nil)
		return
	}

	var d Doctor
	var emailResult sql.NullString
	var createdAt, updatedAt time.Time

	err := db.QueryRow(`
		SELECT
			id,
			full_name,
			phone,
			email,
			medical_syndicate_id,
			birth_date,
			specialty,
			professional_degree,
			governorate,
			is_online,
			created_at,
			updated_at
		FROM doctors
		WHERE LOWER(full_name) = LOWER($1)
			AND LOWER(email) = LOWER($2)
		LIMIT 1
	`, fullName, email).Scan(
		&d.ID,
		&d.FullName,
		&d.Phone,
		&emailResult,
		&d.MedicalSyndicateID,
		&d.BirthDate,
		&d.Specialty,
		&d.ProfessionalDegree,
		&d.Governorate,
		&d.IsOnline,
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		fail(ctx, 401, "Invalid credentials", nil)
		return
	}
	if err != nil {
		fail(ctx, 500, "Failed to log in", err)
		return
	}

	if emailResult.Valid {
		d.Email = &emailResult.String
	}

	d.OnlineStatus = doctorOnlineStatus(d.IsOnline)
	d.CreatedAt = createdAt.Format(time.RFC3339)
	d.UpdatedAt = updatedAt.Format(time.RFC3339)

	ctx.JSON(map[string]interface{}{
		"message": "Login successful",
		"doctor":  d,
	})
}

// ============================================================
// Update Doctor
// ============================================================

func UpdateDoctor(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid doctor id", nil)
		return
	}

	_, fields, err := ctx.ParseMultipart(MaxFileSize)
	if err != nil {
		fail(ctx, 400, "Invalid multipart form", err)
		return
	}

	formValue := func(name string) string {
		if values, ok := fields[name]; ok && len(values) > 0 {
			return values[0]
		}
		return ""
	}

	fullName := formValue("full_name")
	phone := formValue("phone")
	email := strings.TrimSpace(formValue("email"))
	nationalID := formValue("national_id")
	medicalSyndicateID := formValue("medical_syndicate_id")
	birthDate := formValue("birth_date")
	specialty := formValue("specialty")
	professionalDegree := formValue("professional_degree")
	governorate := formValue("governorate")

	result, err := db.Exec(`
		UPDATE doctors
		SET
			full_name = $1,
			phone = $2,
			email = NULLIF($3, ''),
			national_id = $4,
			medical_syndicate_id = $5,
			birth_date = $6,
			specialty = $7,
			professional_degree = $8,
			governorate = $9,
			updated_at = NOW()
		WHERE id = $10
	`,
		fullName,
		phone,
		email,
		nationalID,
		medicalSyndicateID,
		birthDate,
		specialty,
		professionalDegree,
		governorate,
		id,
	)
	if err != nil {
		if isUniqueViolation(err) {
			fail(ctx, 409, "Phone, email, national_id or medical_syndicate_id already exists", nil)
			return
		}
		fail(ctx, 500, "Failed to update doctor", err)
		return
	}

	if rows, _ := result.RowsAffected(); rows == 0 {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Doctor updated successfully",
	})
}

// ============================================================
// Update Doctor Status (Online / Offline)
// ============================================================

type UpdateDoctorStatusRequest struct {
	IsOnline bool `json:"is_online"`
}

func UpdateDoctorStatus(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid doctor id", nil)
		return
	}

	var payload UpdateDoctorStatusRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	result, err := db.Exec(`
		UPDATE doctors
		SET is_online = $1, updated_at = NOW()
		WHERE id = $2
	`, payload.IsOnline, id)
	if err != nil {
		fail(ctx, 500, "Failed to update doctor status", err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		fail(ctx, 500, "Failed to update doctor status", err)
		return
	}
	if rows == 0 {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{
		"message":       "Status updated successfully",
		"is_online":     payload.IsOnline,
		"online_status": doctorOnlineStatus(payload.IsOnline),
	})
}

// ============================================================
// Delete Doctor
// ============================================================

func DeleteDoctor(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid doctor id", nil)
		return
	}

	result, err := db.Exec("DELETE FROM doctors WHERE id = $1", id)
	if err != nil {
		fail(ctx, 500, "Failed to delete doctor", err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		fail(ctx, 500, "Failed to delete doctor", err)
		return
	}
	if rows == 0 {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Doctor deleted successfully",
	})
}

// ============================================================
// Patients
// ============================================================

// Patient mirrors a row of the patients table. The id is a BIGSERIAL, so it
// travels as a JSON number and is read from the URL with strconv.ParseInt.
type Patient struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Phone     string `json:"phone"`
	CreatedAt string `json:"created_at"`
	// Photo is a base64 data URL uploaded through POST /patients/:id/chat-image.
	Photo *string `json:"photo,omitempty"`
}

type CreatePatientRequest struct {
	Username string `json:"username"`
	Phone    string `json:"phone"`
}

type UpdatePatientRequest struct {
	Username string `json:"username"`
	Phone    string `json:"phone"`
}

type LoginPatientRequest struct {
	Username string `json:"username"`
	Phone    string `json:"phone"`
}

func LoginPatient(ctx *breeze.Context) {

	var payload LoginPatientRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	username := strings.TrimSpace(payload.Username)
	phone := strings.TrimSpace(payload.Phone)

	if username == "" {
		fail(ctx, 400, "username is required", nil)
		return
	}
	if phone == "" {
		fail(ctx, 400, "phone is required", nil)
		return
	}

	var p Patient
	var photo sql.NullString
	var createdAt time.Time

	err := db.QueryRow(`
		SELECT id, username, phone, photo, created_at
		FROM patients
		WHERE username = $1 AND phone = $2
	`, username, phone).Scan(&p.ID, &p.Username, &p.Phone, &photo, &createdAt)

	if err == sql.ErrNoRows {
		fail(ctx, 401, "Invalid credentials", nil)
		return
	}
	if err != nil {
		fail(ctx, 500, "Failed to log in", err)
		return
	}

	if photo.Valid && photo.String != "" {
		p.Photo = &photo.String
	}
	p.CreatedAt = createdAt.Format(time.RFC3339)

	ctx.JSON(map[string]interface{}{
		"patient": p,
	})
}

func CreatePatient(ctx *breeze.Context) {

	var payload CreatePatientRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	username := strings.TrimSpace(payload.Username)
	phone := strings.TrimSpace(payload.Phone)

	if username == "" {
		fail(ctx, 400, "username is required", nil)
		return
	}
	if phone == "" {
		fail(ctx, 400, "phone is required", nil)
		return
	}

	var id int64
	var createdAt time.Time

	err := db.QueryRow(`
		INSERT INTO patients (username, phone)
		VALUES ($1, $2)
		RETURNING id, created_at
	`, username, phone).Scan(&id, &createdAt)

	if err != nil {
		if isUniqueViolation(err) {
			fail(ctx, 409, "Username or phone already exists", nil)
			return
		}
		fail(ctx, 500, "Failed to create patient", err)
		return
	}

	ctx.Status(201)
	ctx.JSON(map[string]interface{}{
		"message": "Patient created successfully",
		"patient": Patient{
			ID:        id,
			Username:  username,
			Phone:     phone,
			CreatedAt: createdAt.Format(time.RFC3339),
		},
	})
}

func GetPatients(ctx *breeze.Context) {

	rows, err := db.Query(`
		SELECT id, username, phone, photo, created_at
		FROM patients
		ORDER BY created_at DESC
	`)
	if err != nil {
		fail(ctx, 500, "Failed to load patients", err)
		return
	}
	defer rows.Close()

	patients := []Patient{}

	for rows.Next() {
		var p Patient
		var photo sql.NullString
		var createdAt time.Time

		if err := rows.Scan(&p.ID, &p.Username, &p.Phone, &photo, &createdAt); err != nil {
			fail(ctx, 500, "Failed to read patients", err)
			return
		}

		if photo.Valid && photo.String != "" {
			p.Photo = &photo.String
		}
		p.CreatedAt = createdAt.Format(time.RFC3339)

		patients = append(patients, p)
	}

	if err := rows.Err(); err != nil {
		fail(ctx, 500, "Failed to read patients", err)
		return
	}

	ctx.JSON(map[string]interface{}{
		"count":    len(patients),
		"patients": patients,
	})
}

func GetPatient(ctx *breeze.Context) {

	id, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		fail(ctx, 400, "Invalid patient id", nil)
		return
	}

	var p Patient
	var photo sql.NullString
	var createdAt time.Time

	err = db.QueryRow(`
		SELECT id, username, phone, photo, created_at
		FROM patients
		WHERE id = $1
	`, id).Scan(&p.ID, &p.Username, &p.Phone, &photo, &createdAt)

	if err == sql.ErrNoRows {
		fail(ctx, 404, "Patient not found", nil)
		return
	}
	if err != nil {
		fail(ctx, 500, "Failed to load patient", err)
		return
	}

	if photo.Valid && photo.String != "" {
		p.Photo = &photo.String
	}
	p.CreatedAt = createdAt.Format(time.RFC3339)

	ctx.JSON(map[string]interface{}{
		"patient": p,
	})
}

func UpdatePatient(ctx *breeze.Context) {

	id, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		fail(ctx, 400, "Invalid patient id", nil)
		return
	}

	var payload UpdatePatientRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	username := strings.TrimSpace(payload.Username)
	phone := strings.TrimSpace(payload.Phone)

	if username == "" {
		fail(ctx, 400, "username is required", nil)
		return
	}
	if phone == "" {
		fail(ctx, 400, "phone is required", nil)
		return
	}

	result, err := db.Exec(`
		UPDATE patients
		SET username = $1, phone = $2
		WHERE id = $3
	`, username, phone, id)
	if err != nil {
		if isUniqueViolation(err) {
			fail(ctx, 409, "Username or phone already exists", nil)
			return
		}
		fail(ctx, 500, "Failed to update patient", err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		fail(ctx, 500, "Failed to update patient", err)
		return
	}
	if rows == 0 {
		fail(ctx, 404, "Patient not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Patient updated successfully",
		"patient": Patient{
			ID:       id,
			Username: username,
			Phone:    phone,
		},
	})
}

func DeletePatient(ctx *breeze.Context) {

	id, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		fail(ctx, 400, "Invalid patient id", nil)
		return
	}

	result, err := db.Exec("DELETE FROM patients WHERE id = $1", id)
	if err != nil {
		fail(ctx, 500, "Failed to delete patient", err)
		return
	}

	rows, err := result.RowsAffected()
	if err != nil {
		fail(ctx, 500, "Failed to delete patient", err)
		return
	}
	if rows == 0 {
		fail(ctx, 404, "Patient not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Patient deleted successfully",
	})
}

// ============================================================
// Chat — Types
// ============================================================

type Conversation struct {
	ID        string `json:"id"`
	PatientID int64  `json:"patient_id"`
	DoctorID  string `json:"doctor_id"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	// FirstMessage is the JSON payload {"image": "<data URL>"} of the opening
	// picture. NULL until a profile picture exists.
	FirstMessage *json.RawMessage `json:"first_message,omitempty"`
}

type Message struct {
	ID             int64   `json:"id"`
	ConversationID string  `json:"conversation_id"`
	SenderType     string  `json:"sender_type"`
	SenderID       string  `json:"sender_id"`
	Message        *string `json:"message,omitempty"`
	ImageData      *string `json:"image_data,omitempty"`
	CreatedAt      string  `json:"created_at"`
}

type CreateConversationRequest struct {
	PatientID int64  `json:"patient_id"`
	DoctorID  string `json:"doctor_id"`
}

type SendMessageRequest struct {
	SenderType string `json:"sender_type"`
	SenderID   string `json:"sender_id"`
	Message    string `json:"message"`
}

type ChatWSRequest struct {
	Type           string `json:"type"` // subscribe | message
	ConversationID string `json:"conversation_id"`
	SenderType     string `json:"sender_type"`
	SenderID       string `json:"sender_id"`
	Message        string `json:"message"`
	// Optional base64 data URL (e.g. "data:image/jpeg;base64,...").
	Image string `json:"image,omitempty"`
}

type ChatWSEvent struct {
	Type    string      `json:"type"`
	Message interface{} `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// firstMessageJSON builds the JSONB payload for conversations.first_message.
// It returns a *string* on purpose: lib/pq sends []byte as bytea, which
// Postgres rejects for a JSONB column ("invalid input syntax for type json").
func firstMessageJSON(dataURL string) (string, error) {
	b, err := json.Marshal(map[string]string{"image": dataURL})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ============================================================
// Chat — Conversations
// ============================================================

func CreateConversation(ctx *breeze.Context) {

	var payload CreateConversationRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	payload.DoctorID = strings.TrimSpace(payload.DoctorID)

	if payload.PatientID <= 0 || payload.DoctorID == "" {
		fail(ctx, 400, "patient_id and doctor_id are required", nil)
		return
	}
	if !isValidUUID(payload.DoctorID) {
		fail(ctx, 400, "Invalid doctor_id", nil)
		return
	}

	var patientExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM patients WHERE id = $1)`, payload.PatientID).Scan(&patientExists); err != nil {
		fail(ctx, 500, "Failed to validate patient", err)
		return
	}
	if !patientExists {
		fail(ctx, 404, "Patient not found", nil)
		return
	}

	// Load the doctor's photo; sql.ErrNoRows means the doctor does not exist.
	var doctorPhoto sql.NullString
	err := db.QueryRow(`SELECT profile_image FROM doctors WHERE id = $1`, payload.DoctorID).Scan(&doctorPhoto)
	if err == sql.ErrNoRows {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}
	if err != nil {
		fail(ctx, 500, "Failed to load doctor", err)
		return
	}

	// nil => SQL NULL. A string => cast by Postgres to jsonb.
	var firstMessage interface{}
	if doctorPhoto.Valid && strings.TrimSpace(doctorPhoto.String) != "" {
		fm, err := firstMessageJSON(doctorPhoto.String)
		if err != nil {
			fail(ctx, 500, "Failed to encode first message", err)
			return
		}
		firstMessage = fm
	}

	var c Conversation
	var createdAt, updatedAt time.Time

	err = db.QueryRow(`
		INSERT INTO conversations (patient_id, doctor_id, first_message)
		VALUES ($1, $2, $3::jsonb)
		ON CONFLICT (patient_id, doctor_id)
		DO UPDATE SET
			updated_at = conversations.updated_at,
			first_message = COALESCE(conversations.first_message, EXCLUDED.first_message)
		RETURNING id, patient_id, doctor_id, created_at, updated_at
	`, payload.PatientID, payload.DoctorID, firstMessage).Scan(
		&c.ID, &c.PatientID, &c.DoctorID, &createdAt, &updatedAt,
	)
	if err != nil {
		fail(ctx, 500, "Failed to create conversation", err)
		return
	}

	c.CreatedAt = createdAt.Format(time.RFC3339)
	c.UpdatedAt = updatedAt.Format(time.RFC3339)

	ctx.Status(201)
	ctx.JSON(map[string]interface{}{"conversation": c})
}

func GetConversations(ctx *breeze.Context) {

	patientID := strings.TrimSpace(ctx.Query("patient_id"))
	doctorID := strings.TrimSpace(ctx.Query("doctor_id"))

	query := `
		SELECT id, patient_id, doctor_id, first_message, created_at, updated_at
		FROM conversations
	`
	args := []interface{}{}
	where := []string{}

	if patientID != "" {
		id, err := strconv.ParseInt(patientID, 10, 64)
		if err != nil {
			fail(ctx, 400, "Invalid patient_id", nil)
			return
		}
		args = append(args, id)
		where = append(where, fmt.Sprintf("patient_id = $%d", len(args)))
	}
	if doctorID != "" {
		if !isValidUUID(doctorID) {
			fail(ctx, 400, "Invalid doctor_id", nil)
			return
		}
		args = append(args, doctorID)
		where = append(where, fmt.Sprintf("doctor_id = $%d", len(args)))
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY updated_at DESC"

	rows, err := db.Query(query, args...)
	if err != nil {
		fail(ctx, 500, "Failed to load conversations", err)
		return
	}
	defer rows.Close()

	conversations := []Conversation{}
	for rows.Next() {
		var c Conversation
		var firstMessage []byte
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&c.ID, &c.PatientID, &c.DoctorID, &firstMessage, &createdAt, &updatedAt); err != nil {
			fail(ctx, 500, "Failed to read conversations", err)
			return
		}

		c.CreatedAt = createdAt.Format(time.RFC3339)
		c.UpdatedAt = updatedAt.Format(time.RFC3339)
		if len(firstMessage) > 0 {
			raw := json.RawMessage(firstMessage)
			c.FirstMessage = &raw
		}
		conversations = append(conversations, c)
	}

	if err := rows.Err(); err != nil {
		fail(ctx, 500, "Failed to read conversations", err)
		return
	}

	ctx.JSON(map[string]interface{}{"count": len(conversations), "conversations": conversations})
}

func conversationExistsForSender(conversationID, senderType, senderID string) (bool, error) {

	// A malformed UUID would make Postgres error out; treat it as "not a member".
	if !isValidUUID(conversationID) {
		return false, nil
	}

	switch senderType {
	case "patient":
		id, err := strconv.ParseInt(senderID, 10, 64)
		if err != nil {
			return false, nil
		}
		var ok bool
		err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM conversations WHERE id = $1 AND patient_id = $2)`, conversationID, id).Scan(&ok)
		return ok, err
	case "doctor":
		if !isValidUUID(senderID) {
			return false, nil
		}
		var ok bool
		err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM conversations WHERE id = $1 AND doctor_id = $2)`, conversationID, senderID).Scan(&ok)
		return ok, err
	default:
		return false, nil
	}
}

// ============================================================
// Chat — Messages
// ============================================================

func GetMessages(ctx *breeze.Context) {

	conversationID := ctx.Param("id")
	if conversationID == "" || !isValidUUID(conversationID) {
		fail(ctx, 400, "valid conversation id is required", nil)
		return
	}

	rows, err := db.Query(`
		SELECT id, conversation_id, sender_type, sender_id, message, image_data, created_at
		FROM messages
		WHERE conversation_id = $1
		ORDER BY created_at ASC, id ASC
	`, conversationID)
	if err != nil {
		fail(ctx, 500, "Failed to load messages", err)
		return
	}
	defer rows.Close()

	messages := []Message{}
	for rows.Next() {
		var m Message
		var msg, image sql.NullString
		var createdAt time.Time

		if err := rows.Scan(&m.ID, &m.ConversationID, &m.SenderType, &m.SenderID, &msg, &image, &createdAt); err != nil {
			fail(ctx, 500, "Failed to read messages", err)
			return
		}
		if msg.Valid {
			m.Message = &msg.String
		}
		if image.Valid {
			m.ImageData = &image.String
		}
		m.CreatedAt = createdAt.Format(time.RFC3339)
		messages = append(messages, m)
	}

	if err := rows.Err(); err != nil {
		fail(ctx, 500, "Failed to read messages", err)
		return
	}

	ctx.JSON(map[string]interface{}{"count": len(messages), "messages": messages})
}

func insertMessage(conversationID, senderType, senderID, message string, imageData *string) (Message, error) {

	var m Message
	var msg, image sql.NullString
	var createdAt time.Time

	err := db.QueryRow(`
		INSERT INTO messages (conversation_id, sender_type, sender_id, message, image_data)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		RETURNING id, conversation_id, sender_type, sender_id, message, image_data, created_at
	`, conversationID, senderType, senderID, strings.TrimSpace(message), imageData).Scan(
		&m.ID, &m.ConversationID, &m.SenderType, &m.SenderID, &msg, &image, &createdAt,
	)
	if err != nil {
		return m, err
	}
	if msg.Valid {
		m.Message = &msg.String
	}
	if image.Valid {
		m.ImageData = &image.String
	}
	m.CreatedAt = createdAt.Format(time.RFC3339)

	// Bump the conversation so it sorts first. A failure here must not fail
	// the send, but it should never be silent either.
	if _, err := db.Exec(`UPDATE conversations SET updated_at = NOW() WHERE id = $1`, conversationID); err != nil {
		log.Println("failed to touch conversation:", err)
	}
	return m, nil
}

func SendMessage(ctx *breeze.Context) {

	conversationID := ctx.Param("id")

	var payload SendMessageRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	payload.SenderType = strings.TrimSpace(strings.ToLower(payload.SenderType))
	payload.SenderID = strings.TrimSpace(payload.SenderID)
	payload.Message = strings.TrimSpace(payload.Message)

	if payload.SenderType == "" || payload.SenderID == "" || payload.Message == "" {
		fail(ctx, 400, "sender_type, sender_id and message are required", nil)
		return
	}

	ok, err := conversationExistsForSender(conversationID, payload.SenderType, payload.SenderID)
	if err != nil {
		fail(ctx, 500, "Failed to validate conversation", err)
		return
	}
	if !ok {
		fail(ctx, 403, "Sender is not a member of this conversation", nil)
		return
	}

	m, err := insertMessage(conversationID, payload.SenderType, payload.SenderID, payload.Message, nil)
	if err != nil {
		fail(ctx, 500, "Failed to save message", err)
		return
	}

	broadcastChatMessage(m)
	ctx.Status(201)
	ctx.JSON(map[string]interface{}{"message": m})
}

func SendImageMessage(ctx *breeze.Context) {

	conversationID := ctx.Param("id")

	files, fields, err := ctx.ParseMultipart(MaxFileSize)
	if err != nil {
		fail(ctx, 400, "Invalid multipart form", err)
		return
	}

	field := func(name string) string {
		if values, ok := fields[name]; ok && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
		return ""
	}

	senderType := strings.ToLower(field("sender_type"))
	senderID := field("sender_id")
	message := field("message")

	if senderType == "" || senderID == "" {
		fail(ctx, 400, "sender_type and sender_id are required", nil)
		return
	}

	ok, err := conversationExistsForSender(conversationID, senderType, senderID)
	if err != nil {
		fail(ctx, 500, "Failed to validate conversation", err)
		return
	}
	if !ok {
		fail(ctx, 403, "Sender is not a member of this conversation", nil)
		return
	}

	imageData, err := readUploadedFile(files, "image", MaxImageSize,
		[]string{"image/jpeg", "image/png", "image/webp"}, true)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	m, err := insertMessage(conversationID, senderType, senderID, message, imageData)
	if err != nil {
		fail(ctx, 500, "Failed to save image message", err)
		return
	}

	broadcastChatMessage(m)
	ctx.Status(201)
	ctx.JSON(map[string]interface{}{"message": m})
}

// ============================================================
// Save Doctor Chat Image
// ============================================================

// SaveDoctorChatImage handles POST /doctors/:id/chat-image.
//
// The doctor uploads a picture (multipart field "image", 5 MB max, JPEG/PNG/
// WebP). It is persisted as:
//  1. the doctor's profile_image column, and
//  2. first_message on all of the doctor's existing conversations.
//
// If the doctor has no conversation yet, first_message is written the next
// time CreateConversation runs.
func SaveDoctorChatImage(ctx *breeze.Context) {

	doctorID := ctx.Param("id")
	if !isValidUUID(doctorID) {
		fail(ctx, 400, "Invalid doctor id", nil)
		return
	}

	files, _, err := ctx.ParseMultipart(MaxImageSize)
	if err != nil {
		fail(ctx, 400, "Invalid multipart form", err)
		return
	}

	imageData, err := readUploadedFile(files, "image", MaxImageSize,
		[]string{"image/jpeg", "image/png", "image/webp"}, true)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	result, err := db.Exec(`
		UPDATE doctors
		SET profile_image = $1, updated_at = NOW()
		WHERE id = $2
	`, imageData, doctorID)
	if err != nil {
		fail(ctx, 500, "Failed to save doctor image", err)
		return
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}

	// first_message is JSONB: pass a string, never []byte (see firstMessageJSON).
	fm, err := firstMessageJSON(*imageData)
	if err != nil {
		fail(ctx, 500, "Failed to encode first message", err)
		return
	}
	if _, err := db.Exec(`
		UPDATE conversations
		SET first_message = $1::jsonb, updated_at = NOW()
		WHERE doctor_id = $2
	`, fm, doctorID); err != nil {
		log.Println("failed to update first_message for doctor:", err)
	}

	ctx.JSON(map[string]interface{}{
		"message": "Doctor image saved",
		"doctor":  map[string]interface{}{"id": doctorID, "image": *imageData},
	})
}

// ============================================================
// Save Patient Chat Image
// ============================================================

// SavePatientChatImage handles POST /patients/:id/chat-image.
// Same flow as SaveDoctorChatImage, using patients.photo.
func SavePatientChatImage(ctx *breeze.Context) {

	patientID, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		fail(ctx, 400, "Invalid patient id", nil)
		return
	}

	files, _, err := ctx.ParseMultipart(MaxImageSize)
	if err != nil {
		fail(ctx, 400, "Invalid multipart form", err)
		return
	}

	imageData, err := readUploadedFile(files, "image", MaxImageSize,
		[]string{"image/jpeg", "image/png", "image/webp"}, true)
	if err != nil {
		fail(ctx, 400, err.Error(), nil)
		return
	}

	result, err := db.Exec(`UPDATE patients SET photo = $1 WHERE id = $2`, imageData, patientID)
	if err != nil {
		fail(ctx, 500, "Failed to save patient image", err)
		return
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		fail(ctx, 404, "Patient not found", nil)
		return
	}

	fm, err := firstMessageJSON(*imageData)
	if err != nil {
		fail(ctx, 500, "Failed to encode first message", err)
		return
	}
	if _, err := db.Exec(`
		UPDATE conversations
		SET first_message = $1::jsonb, updated_at = NOW()
		WHERE patient_id = $2
	`, fm, patientID); err != nil {
		log.Println("failed to update first_message for patient:", err)
	}

	ctx.JSON(map[string]interface{}{
		"message": "Patient image saved",
		"patient": map[string]interface{}{"id": patientID, "image": *imageData},
	})
}

// ============================================================
// Chat — WebSocket
// ============================================================

func broadcastChatMessage(m Message) {

	payload, err := json.Marshal(ChatWSEvent{Type: "message.created", Message: m})
	if err != nil {
		return
	}

	chatWS.RLock()
	connections := make([]*breeze.WSConn, 0, len(chatWS.clients))
	for conn, conversationID := range chatWS.clients {
		if conversationID == m.ConversationID {
			connections = append(connections, conn)
		}
	}
	chatWS.RUnlock()

	for _, conn := range connections {
		_ = conn.SendText(string(payload))
	}
}

func chatWSOnConnect(conn *breeze.WSConn) {
	chatWS.Lock()
	chatWS.clients[conn] = ""
	chatWS.Unlock()
	_ = conn.SendText(`{"type":"connected","message":"Send a subscribe event to join a conversation"}`)
}

func chatWSOnClose(conn *breeze.WSConn, code uint16, reason string) {
	chatWS.Lock()
	delete(chatWS.clients, conn)
	chatWS.Unlock()
}

func chatWSOnMessage(conn *breeze.WSConn, opcode byte, payload []byte) {

	if opcode != 1 { // text frames only for the JSON chat protocol
		return
	}

	var req ChatWSRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = conn.SendText(`{"type":"error","error":"Invalid JSON"}`)
		return
	}

	req.Type = strings.TrimSpace(strings.ToLower(req.Type))
	req.ConversationID = strings.TrimSpace(req.ConversationID)
	req.SenderType = strings.TrimSpace(strings.ToLower(req.SenderType))
	req.SenderID = strings.TrimSpace(req.SenderID)

	switch req.Type {

	case "subscribe":
		if req.ConversationID == "" || req.SenderType == "" || req.SenderID == "" {
			_ = conn.SendText(`{"type":"error","error":"conversation_id, sender_type and sender_id are required"}`)
			return
		}
		ok, err := conversationExistsForSender(req.ConversationID, req.SenderType, req.SenderID)
		if err != nil {
			_ = conn.SendText(`{"type":"error","error":"Failed to validate conversation"}`)
			return
		}
		if !ok {
			_ = conn.SendText(`{"type":"error","error":"Sender is not a member of this conversation"}`)
			return
		}
		chatWS.Lock()
		chatWS.clients[conn] = req.ConversationID
		chatWS.Unlock()
		_ = conn.SendText(`{"type":"subscribed"}`)

	case "message":
		chatWS.RLock()
		subscribedConversation := chatWS.clients[conn]
		chatWS.RUnlock()

		if subscribedConversation == "" || subscribedConversation != req.ConversationID {
			_ = conn.SendText(`{"type":"error","error":"Subscribe to the conversation first"}`)
			return
		}
		if req.SenderType == "" || req.SenderID == "" {
			_ = conn.SendText(`{"type":"error","error":"sender_type and sender_id are required"}`)
			return
		}
		if strings.TrimSpace(req.Message) == "" && strings.TrimSpace(req.Image) == "" {
			_ = conn.SendText(`{"type":"error","error":"message or image is required"}`)
			return
		}
		ok, err := conversationExistsForSender(req.ConversationID, req.SenderType, req.SenderID)
		if err != nil || !ok {
			_ = conn.SendText(`{"type":"error","error":"Sender is not a member of this conversation"}`)
			return
		}

		var imgPtr *string
		if strings.TrimSpace(req.Image) != "" {
			v := req.Image
			imgPtr = &v
		}

		m, err := insertMessage(req.ConversationID, req.SenderType, req.SenderID, req.Message, imgPtr)
		if err != nil {
			log.Println("ws insertMessage error:", err)
			_ = conn.SendText(`{"type":"error","error":"Failed to save message"}`)
			return
		}
		broadcastChatMessage(m)

	default:
		_ = conn.SendText(`{"type":"error","error":"Unknown event type"}`)
	}
}

// ============================================================
// Work Places — CRUD
// ============================================================

// CreateWorkPlace handles POST /work-places. Schedules are optional.
func CreateWorkPlace(ctx *breeze.Context) {

	var payload CreateWorkPlaceRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	payload.DoctorID = strings.TrimSpace(payload.DoctorID)
	payload.PlaceType = strings.TrimSpace(payload.PlaceType)
	payload.Name = strings.TrimSpace(payload.Name)
	payload.Address = strings.TrimSpace(payload.Address)
	payload.Phone = strings.TrimSpace(payload.Phone)

	required := map[string]string{
		"doctor_id":  payload.DoctorID,
		"place_type": payload.PlaceType,
		"name":       payload.Name,
		"address":    payload.Address,
	}
	for field, val := range required {
		if val == "" {
			fail(ctx, 400, field+" is required", nil)
			return
		}
	}
	if !isValidUUID(payload.DoctorID) {
		fail(ctx, 400, "Invalid doctor_id", nil)
		return
	}

	var doctorExists bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM doctors WHERE id = $1)`, payload.DoctorID,
	).Scan(&doctorExists); err != nil {
		fail(ctx, 500, "Failed to validate doctor", err)
		return
	}
	if !doctorExists {
		fail(ctx, 404, "Doctor not found", nil)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		fail(ctx, 500, "Failed to start transaction", err)
		return
	}
	defer tx.Rollback()

	var wp WorkPlace
	var nsPhone sql.NullString
	var nfPrice sql.NullFloat64
	var createdAt, updatedAt time.Time

	err = tx.QueryRow(`
		INSERT INTO work_places (doctor_id, place_type, name, address, phone, exam_price)
		VALUES ($1, $2, $3, $4, NULLIF($5,''), $6)
		RETURNING id, doctor_id, place_type, name, address, phone, exam_price, created_at, updated_at
	`, payload.DoctorID, payload.PlaceType, payload.Name, payload.Address,
		payload.Phone, payload.ExamPrice,
	).Scan(
		&wp.ID, &wp.DoctorID, &wp.PlaceType, &wp.Name, &wp.Address,
		&nsPhone, &nfPrice,
		&createdAt, &updatedAt,
	)
	if err != nil {
		fail(ctx, 500, "Failed to create work place", err)
		return
	}
	scanNullableString(nsPhone, &wp.Phone)
	scanNullableFloat(nfPrice, &wp.ExamPrice)
	wp.CreatedAt = createdAt.Format(time.RFC3339)
	wp.UpdatedAt = updatedAt.Format(time.RFC3339)

	if len(payload.Schedules) > 0 {
		if err := insertSchedules(tx, wp.ID, payload.Schedules); err != nil {
			fail(ctx, 400, err.Error(), nil)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		fail(ctx, 500, "Failed to commit transaction", err)
		return
	}

	wp.Schedules = loadSchedules(wp.ID)

	ctx.Status(201)
	ctx.JSON(map[string]interface{}{"work_place": wp})
}

// GetWorkPlaces handles GET /work-places?doctor_id=...
func GetWorkPlaces(ctx *breeze.Context) {

	doctorID := strings.TrimSpace(ctx.Query("doctor_id"))

	query := `
		SELECT id, doctor_id, place_type, name, address, phone, exam_price, created_at, updated_at
		FROM work_places
	`
	args := []interface{}{}
	if doctorID != "" {
		if !isValidUUID(doctorID) {
			fail(ctx, 400, "Invalid doctor_id", nil)
			return
		}
		query += " WHERE doctor_id = $1"
		args = append(args, doctorID)
	}
	query += " ORDER BY created_at ASC"

	rows, err := db.Query(query, args...)
	if err != nil {
		fail(ctx, 500, "Failed to load work places", err)
		return
	}
	defer rows.Close()

	places := []WorkPlace{}
	for rows.Next() {
		var wp WorkPlace
		var nsP sql.NullString
		var nfP sql.NullFloat64
		var createdAt, updatedAt time.Time

		if err := rows.Scan(
			&wp.ID, &wp.DoctorID, &wp.PlaceType, &wp.Name, &wp.Address,
			&nsP, &nfP,
			&createdAt, &updatedAt,
		); err != nil {
			fail(ctx, 500, "Failed to read work places", err)
			return
		}
		scanNullableString(nsP, &wp.Phone)
		scanNullableFloat(nfP, &wp.ExamPrice)
		wp.CreatedAt = createdAt.Format(time.RFC3339)
		wp.UpdatedAt = updatedAt.Format(time.RFC3339)
		wp.Schedules = loadSchedules(wp.ID)
		places = append(places, wp)
	}

	if err := rows.Err(); err != nil {
		fail(ctx, 500, "Failed to read work places", err)
		return
	}

	ctx.JSON(map[string]interface{}{"count": len(places), "work_places": places})
}

// GetWorkPlace handles GET /work-places/:id
func GetWorkPlace(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid work place id", nil)
		return
	}

	var wp WorkPlace
	var nsP sql.NullString
	var nfP sql.NullFloat64
	var createdAt, updatedAt time.Time

	err := db.QueryRow(`
		SELECT id, doctor_id, place_type, name, address, phone, exam_price, created_at, updated_at
		FROM work_places
		WHERE id = $1
	`, id).Scan(
		&wp.ID, &wp.DoctorID, &wp.PlaceType, &wp.Name, &wp.Address,
		&nsP, &nfP,
		&createdAt, &updatedAt,
	)
	if err == sql.ErrNoRows {
		fail(ctx, 404, "Work place not found", nil)
		return
	}
	if err != nil {
		fail(ctx, 500, "Failed to load work place", err)
		return
	}

	scanNullableString(nsP, &wp.Phone)
	scanNullableFloat(nfP, &wp.ExamPrice)
	wp.CreatedAt = createdAt.Format(time.RFC3339)
	wp.UpdatedAt = updatedAt.Format(time.RFC3339)
	wp.Schedules = loadSchedules(wp.ID)

	ctx.JSON(map[string]interface{}{"work_place": wp})
}

// UpdateWorkPlace handles PUT /work-places/:id
func UpdateWorkPlace(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid work place id", nil)
		return
	}

	var payload UpdateWorkPlaceRequest
	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	payload.PlaceType = strings.TrimSpace(payload.PlaceType)
	payload.Name = strings.TrimSpace(payload.Name)
	payload.Address = strings.TrimSpace(payload.Address)
	payload.Phone = strings.TrimSpace(payload.Phone)

	required := map[string]string{
		"place_type": payload.PlaceType,
		"name":       payload.Name,
		"address":    payload.Address,
	}
	for field, val := range required {
		if val == "" {
			fail(ctx, 400, field+" is required", nil)
			return
		}
	}

	result, err := db.Exec(`
		UPDATE work_places
		SET
			place_type = $1,
			name       = $2,
			address    = $3,
			phone      = NULLIF($4, ''),
			exam_price = $5,
			updated_at = NOW()
		WHERE id = $6
	`, payload.PlaceType, payload.Name, payload.Address, payload.Phone, payload.ExamPrice, id)
	if err != nil {
		fail(ctx, 500, "Failed to update work place", err)
		return
	}

	if rows, _ := result.RowsAffected(); rows == 0 {
		fail(ctx, 404, "Work place not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{"message": "Work place updated successfully"})
}

// DeleteWorkPlace handles DELETE /work-places/:id
func DeleteWorkPlace(ctx *breeze.Context) {

	id := ctx.Param("id")
	if !isValidUUID(id) {
		fail(ctx, 400, "Invalid work place id", nil)
		return
	}

	result, err := db.Exec(`DELETE FROM work_places WHERE id = $1`, id)
	if err != nil {
		fail(ctx, 500, "Failed to delete work place", err)
		return
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		fail(ctx, 404, "Work place not found", nil)
		return
	}

	ctx.JSON(map[string]interface{}{"message": "Work place deleted successfully"})
}

// ============================================================
// Work Schedules
// ============================================================

// SetWorkSchedules handles POST /work-places/:id/schedules.
// It replaces ALL schedules for the work place.
// Body: { "schedules": [ WorkScheduleInput, ... ] }
func SetWorkSchedules(ctx *breeze.Context) {

	workPlaceID := ctx.Param("id")
	if !isValidUUID(workPlaceID) {
		fail(ctx, 400, "Invalid work place id", nil)
		return
	}

	var body struct {
		Schedules []WorkScheduleInput `json:"schedules"`
	}
	if err := json.Unmarshal(ctx.Req.Body, &body); err != nil {
		fail(ctx, 400, "Invalid JSON body", nil)
		return
	}

	var exists bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM work_places WHERE id = $1)`, workPlaceID,
	).Scan(&exists); err != nil {
		fail(ctx, 500, "Failed to validate work place", err)
		return
	}
	if !exists {
		fail(ctx, 404, "Work place not found", nil)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		fail(ctx, 500, "Failed to start transaction", err)
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM work_schedules WHERE work_place_id = $1`, workPlaceID); err != nil {
		fail(ctx, 500, "Failed to clear schedules", err)
		return
	}

	if len(body.Schedules) > 0 {
		if err := insertSchedules(tx, workPlaceID, body.Schedules); err != nil {
			fail(ctx, 400, err.Error(), nil)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		fail(ctx, 500, "Failed to commit transaction", err)
		return
	}

	ctx.JSON(map[string]interface{}{
		"message":   "Schedules updated successfully",
		"schedules": loadSchedules(workPlaceID),
	})
}

// ============================================================
// Work Schedules — Internal Helpers
// ============================================================

// insertSchedules inserts schedule rows inside an active transaction.
// Each input with is_off = false must have valid from_time / to_time ("HH:MM").
func insertSchedules(tx *sql.Tx, workPlaceID string, schedules []WorkScheduleInput) error {
	for i, s := range schedules {
		if s.DayOfWeek < 0 || s.DayOfWeek > 6 {
			return fmt.Errorf("schedules[%d]: day_of_week must be 0–6", i)
		}
		if s.IsOff {
			if _, err := tx.Exec(`
				INSERT INTO work_schedules (work_place_id, day_of_week, is_off, from_time, to_time)
				VALUES ($1, $2, TRUE, NULL, NULL)
			`, workPlaceID, s.DayOfWeek); err != nil {
				return fmt.Errorf("failed to insert schedule for day %d: %w", s.DayOfWeek, err)
			}
			continue
		}
		if !isValidTime(s.FromTime) || !isValidTime(s.ToTime) {
			return fmt.Errorf("schedules[%d]: from_time and to_time must be HH:MM", i)
		}
		if s.FromTime >= s.ToTime {
			return fmt.Errorf("schedules[%d] (day %d): from_time (%s) must be before to_time (%s)",
				i, s.DayOfWeek, s.FromTime, s.ToTime)
		}
		if _, err := tx.Exec(`
			INSERT INTO work_schedules (work_place_id, day_of_week, is_off, from_time, to_time)
			VALUES ($1, $2, FALSE, $3, $4)
		`, workPlaceID, s.DayOfWeek, s.FromTime, s.ToTime); err != nil {
			return fmt.Errorf("failed to insert schedule for day %d: %w", s.DayOfWeek, err)
		}
	}
	return nil
}

// loadSchedules fetches all schedule rows for a work place, preserving
// multiple periods per day.
func loadSchedules(workPlaceID string) []WorkSchedule {
	rows, err := db.Query(`
		SELECT id, work_place_id, day_of_week, is_off,
		       to_char(from_time, 'HH24:MI'),
		       to_char(to_time,   'HH24:MI')
		FROM work_schedules
		WHERE work_place_id = $1
		ORDER BY day_of_week ASC, from_time ASC NULLS LAST, id ASC
	`, workPlaceID)
	if err != nil {
		log.Println("loadSchedules query error:", err)
		return nil
	}
	defer rows.Close()

	var result []WorkSchedule
	for rows.Next() {
		var s WorkSchedule
		var from, to sql.NullString
		if err := rows.Scan(&s.ID, &s.WorkPlaceID, &s.DayOfWeek, &s.IsOff, &from, &to); err != nil {
			log.Println("loadSchedules scan error:", err)
			continue
		}
		s.DayName = arabicDayName(s.DayOfWeek)
		if from.Valid {
			t := from.String
			s.FromTime = &t
		}
		if to.Valid {
			t := to.String
			s.ToTime = &t
		}
		result = append(result, s)
	}
	return result
}

// isValidTime checks that s matches "HH:MM".
func isValidTime(s string) bool {
	if len(s) != 5 || s[2] != ':' {
		return false
	}
	_, err := time.Parse("15:04", s)
	return err == nil
}

// scanNullableString copies a scanned NullString into an optional pointer.
func scanNullableString(ns sql.NullString, p **string) {
	if ns.Valid {
		s := ns.String
		*p = &s
	}
}

// scanNullableFloat copies a scanned NullFloat64 into an optional pointer.
func scanNullableFloat(nf sql.NullFloat64, p **float64) {
	if nf.Valid {
		f := nf.Float64
		*p = &f
	}
}

// ============================================================
// File Upload Helper
// ============================================================

// readUploadedFile validates an uploaded file and returns it as a base64 data URL.
// The type is detected from the file's actual bytes, NOT from the client's
// Content-Type header: proxies (such as a SvelteKit route) often send
// "application/octet-stream", which would otherwise be rejected.
func readUploadedFile(
	files map[string][]*breeze.UploadedFile,
	fieldName string,
	maxSize int64,
	allowedTypes []string,
	required bool,
) (*string, error) {

	items := files[fieldName]
	if len(items) == 0 {
		if !required {
			return nil, nil
		}
		return nil, fmt.Errorf("%s is required", fieldName)
	}

	file := items[0]
	if file.Size > maxSize {
		return nil, fmt.Errorf("%s exceeds maximum file size", fieldName)
	}

	if len(file.Content) == 0 {
		return nil, fmt.Errorf("%s is empty or could not be read", fieldName)
	}

	contentType := httpDetectContentType(file.Content)

	valid := false
	for _, allowed := range allowedTypes {
		if contentType == allowed {
			valid = true
			break
		}
	}
	if !valid {
		return nil, fmt.Errorf("invalid file type for %s: %s", fieldName, contentType)
	}

	encoded := base64.StdEncoding.EncodeToString(file.Content)
	result := "data:" + contentType + ";base64," + encoded
	return &result, nil
}

func httpDetectContentType(data []byte) string {
	return http.DetectContentType(data)
}

// ============================================================
// Database Error Helper
// ============================================================

// isUniqueViolation reports whether err is a Postgres 23505 (unique_violation)
// so create/update paths can return 409 instead of 500.
func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}