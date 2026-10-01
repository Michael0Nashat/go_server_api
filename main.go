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
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/nelthaarion/breeze"
	middleware "github.com/nelthaarion/breeze/middlewares"

	"github.com/lib/pq"
)

var db *sql.DB

// ============================================================
// Configuration
// ============================================================

const (
	MaxImageSize = 5 * 1024 * 1024  // 5 MB
	MaxFileSize  = 10 * 1024 * 1024 // 10 MB
)

// ============================================================
// Main
// ============================================================

func main() {

	// =========================
	// Environment
	// =========================

	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found")
	}

	// =========================
	// Database
	// =========================

	if err := InitDB(); err != nil {
		log.Fatal("Database connection failed:", err)
	}

	defer db.Close()

	if err := CreateTables(); err != nil {
		log.Fatal("Database migration failed:", err)
	}

	log.Println("Database connected successfully")

	// =========================
	// Router
	// =========================

	router := breeze.NewRouter()

	router.Use(middleware.RecoveryMiddleware())
	router.Use(middleware.LoggingMiddleware())

	// =========================
	// Home
	// =========================

	router.Handle(breeze.GET, "/", func(ctx *breeze.Context) {
		ctx.JSON(map[string]interface{}{
			"message": "Doctor API",
			"status":  "ok",
		})
	})

	// =========================
	// Health
	// =========================

	router.Handle(breeze.GET, "/health", func(ctx *breeze.Context) {
		ctx.JSON(map[string]interface{}{
			"status": "healthy",
		})
	})

	// =========================
	// Doctors API
	// =========================

	router.HandleBlocking(
		breeze.POST,
		"/doctors",
		CreateDoctor,
	)

	router.HandleBlocking(
		breeze.GET,
		"/doctors",
		GetDoctors,
	)

	router.HandleBlocking(
		breeze.GET,
		"/doctors/:id",
		GetDoctor,
	)

	router.HandleBlocking(
		breeze.PUT,
		"/doctors/:id",
		UpdateDoctor,
	)

	router.HandleBlocking(
		breeze.DELETE,
		"/doctors/:id",
		DeleteDoctor,
	)

	// Static segment, so it wins over "/doctors/:id" for POST lookups.
	router.HandleBlocking(
		breeze.POST,
		"/doctors/login",
		LoginDoctor,
	)

	// =========================
	// Patients API
	// =========================

	router.HandleBlocking(
		breeze.POST,
		"/patients",
		CreatePatient,
	)

	router.HandleBlocking(
		breeze.GET,
		"/patients",
		GetPatients,
	)

	router.HandleBlocking(
		breeze.GET,
		"/patients/:id",
		GetPatient,
	)

	router.HandleBlocking(
		breeze.PUT,
		"/patients/:id",
		UpdatePatient,
	)

	router.HandleBlocking(
		breeze.DELETE,
		"/patients/:id",
		DeletePatient,
	)

	// =========================
	// Worker Pool
	// =========================

	pool := breeze.NewWorkerPool(runtime.NumCPU())
	app := breeze.New(router, pool)

	// =========================
	// Port
	// =========================

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

	if err := db.Ping(); err != nil {
		return err
	}

	return nil
}

// ============================================================
// Create Tables
// ============================================================

// doctorsDDL and patientsDDL are the schemas this service owns. Both are
// idempotent, so CreateTables can run on every boot without touching rows that
// already exist.
var (
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

		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`

	patientsDDL = `
	CREATE TABLE IF NOT EXISTS patients (
		id BIGSERIAL PRIMARY KEY,
		username VARCHAR(100) NOT NULL UNIQUE,
		phone VARCHAR(20) NOT NULL UNIQUE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`
)

func CreateTables() error {

	queries := []string{doctorsDDL, patientsDDL}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

// ============================================================
// Doctor Request
// ============================================================

type Doctor struct {
	ID                     string  `json:"id"`
	ProfileImage           *string `json:"profile_image,omitempty"`
	FullName               string  `json:"full_name"`
	Phone                  string  `json:"phone"`
	Email                  *string `json:"email,omitempty"`
	NationalID             string  `json:"national_id"`
	MedicalSyndicateID     string  `json:"medical_syndicate_id"`
	BirthDate              string  `json:"birth_date"`
	Specialty              string  `json:"specialty"`
	ProfessionalDegree     string  `json:"professional_degree"`
	Governorate            string  `json:"governorate"`
	MedicalSyndicateCard   string  `json:"medical_syndicate_card"`
	NationalIDCard         string  `json:"national_id_card"`
	SpecialtyCertificate   *string `json:"specialty_certificate,omitempty"`
	CreatedAt              string  `json:"created_at"`
	UpdatedAt              string  `json:"updated_at"`
}

// ============================================================
// Create Doctor
// ============================================================

func CreateDoctor(ctx *breeze.Context) {

	files, fields, err := ctx.ParseMultipart(MaxFileSize)
	if err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid multipart form",
			"details": err.Error(),
		})
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

	// =========================
	// Required fields
	// =========================

	required := map[string]string{
		"full_name":              fullName,
		"phone":                  phone,
		"national_id":            nationalID,
		"medical_syndicate_id":   medicalSyndicateID,
		"birth_date":             birthDate,
		"specialty":              specialty,
		"professional_degree":    professionalDegree,
		"governorate":            governorate,
	}

	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			ctx.JSON(map[string]interface{}{
				"error": field + " is required",
			})
			return
		}
	}

	// =========================
	// Profile Image
	// =========================

	profileImage, err := readUploadedFile(
		files,
		"profile_image",
		MaxImageSize,
		[]string{
			"image/jpeg",
			"image/png",
		},
		false,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// =========================
	// Syndicate Card
	// =========================

	syndicateCard, err := readUploadedFile(
		files,
		"medical_syndicate_card",
		MaxFileSize,
		[]string{
			"image/jpeg",
			"image/png",
			"application/pdf",
		},
		true,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// =========================
	// National ID
	// =========================

	nationalIDCard, err := readUploadedFile(
		files,
		"national_id_card",
		MaxFileSize,
		[]string{
			"image/jpeg",
			"image/png",
			"application/pdf",
		},
		true,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// =========================
	// Specialty Certificate
	// Optional
	// =========================

	specialtyCertificate, err := readUploadedFile(
		files,
		"specialty_certificate",
		MaxFileSize,
		[]string{
			"image/jpeg",
			"image/png",
			"application/pdf",
		},
		false,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// =========================
	// Insert
	// =========================

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
	var createdAt time.Time
	var updatedAt time.Time

	err = db.QueryRow(
		query,
		profileImage,
		fullName,
		phone,
		email,
		nationalID,
		medicalSyndicateID,
		birthDate,
		specialty,
		professionalDegree,
		governorate,
		syndicateCard,
		nationalIDCard,
		specialtyCertificate,
	).Scan(
		&id,
		&createdAt,
		&updatedAt,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": "Failed to create doctor",
			"details": err.Error(),
		})
		return
	}

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
			created_at,
			updated_at
		FROM doctors
		ORDER BY created_at DESC
	`)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	defer rows.Close()

	var doctors []Doctor

	for rows.Next() {

		var d Doctor
		var email sql.NullString

		var createdAt time.Time
		var updatedAt time.Time

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
			&createdAt,
			&updatedAt,
		)

		if err != nil {
			ctx.JSON(map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		if email.Valid {
			d.Email = &email.String
		}

		d.CreatedAt = createdAt.Format(time.RFC3339)
		d.UpdatedAt = updatedAt.Format(time.RFC3339)

		doctors = append(doctors, d)
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

	var d Doctor

	var email sql.NullString
	var createdAt time.Time
	var updatedAt time.Time

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
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		ctx.JSON(map[string]interface{}{
			"error": "Doctor not found",
		})
		return
	}

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if email.Valid {
		d.Email = &email.String
	}

	d.CreatedAt = createdAt.Format(time.RFC3339)
	d.UpdatedAt = updatedAt.Format(time.RFC3339)

	ctx.JSON(map[string]interface{}{
		"doctor": d,
	})
}

// ============================================================
// Login Request
// ============================================================

// LoginDoctor authenticates a doctor from the two fields the login form sends.
// Both are matched case-insensitively and whitespace-trimmed, because the
// values are typed by hand and Arabic names are often entered inconsistently.
type LoginRequest struct {
	FullName string `json:"full_name"`
	Email    string `json:"email"`
}

// ============================================================
// Login Doctor
// ============================================================

func LoginDoctor(ctx *breeze.Context) {

	var payload LoginRequest

	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid JSON body",
		})
		return
	}

	fullName := strings.TrimSpace(payload.FullName)
	email := strings.ToLower(strings.TrimSpace(payload.Email))

	// =========================
	// Required fields
	// =========================

	if fullName == "" {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "full_name is required",
		})
		return
	}

	if email == "" {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "email is required",
		})
		return
	}

	// =========================
	// Lookup
	// =========================

	var d Doctor

	var emailResult sql.NullString

	var createdAt time.Time
	var updatedAt time.Time

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
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		ctx.Status(401)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid credentials",
		})
		return
	}

	if err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error":   "Failed to log in",
			"details": err.Error(),
		})
		return
	}

	if emailResult.Valid {
		d.Email = &emailResult.String
	}

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

	_, fields, err := ctx.ParseMultipart(MaxFileSize)
	if err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid multipart form",
			"details": err.Error(),
		})
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

	_, err = db.Exec(`
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
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Doctor updated successfully",
	})
}

// ============================================================
// Delete Doctor
// ============================================================

func DeleteDoctor(ctx *breeze.Context) {

	id := ctx.Param("id")

	result, err := db.Exec(
		"DELETE FROM doctors WHERE id = $1",
		id,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	rows, err := result.RowsAffected()

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if rows == 0 {
		ctx.JSON(map[string]interface{}{
			"error": "Doctor not found",
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Doctor deleted successfully",
	})
}

// ============================================================
// Patient Request
// ============================================================

// Patient mirrors a row of the patients table. The id is a BIGSERIAL, so it
// travels as a JSON number and is read from the URL with strconv.ParseInt.
type Patient struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Phone     string `json:"phone"`
	CreatedAt string `json:"created_at"`
}

// CreatePatientRequest is the JSON body accepted by POST /patients.
type CreatePatientRequest struct {
	Username string `json:"username"`
	Phone    string `json:"phone"`
}

// UpdatePatientRequest is the JSON body accepted by PUT /patients/:id. Both
// fields are required, mirroring how UpdateDoctor replaces the whole record.
type UpdatePatientRequest struct {
	Username string `json:"username"`
	Phone    string `json:"phone"`
}

// ============================================================
// Create Patient
// ============================================================

func CreatePatient(ctx *breeze.Context) {

	var payload CreatePatientRequest

	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid JSON body",
		})
		return
	}

	username := strings.TrimSpace(payload.Username)
	phone := strings.TrimSpace(payload.Phone)

	// =========================
	// Required fields
	// =========================

	required := map[string]string{
		"username": username,
		"phone":    phone,
	}

	for field, value := range required {
		if value == "" {
			ctx.Status(400)
			ctx.JSON(map[string]interface{}{
				"error": field + " is required",
			})
			return
		}
	}

	// =========================
	// Insert
	// =========================

	query := `
		INSERT INTO patients (
			username,
			phone
		)
		VALUES ($1, $2)
		RETURNING id, created_at
	`

	var id int64
	var createdAt time.Time

	err := db.QueryRow(
		query,
		username,
		phone,
	).Scan(
		&id,
		&createdAt,
	)

	if err != nil {
		if isUniqueViolation(err) {
			ctx.Status(409)
			ctx.JSON(map[string]interface{}{
				"error": "Username or phone already exists",
			})
			return
		}

		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error":   "Failed to create patient",
			"details": err.Error(),
		})
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

// ============================================================
// Get Patients
// ============================================================

func GetPatients(ctx *breeze.Context) {

	rows, err := db.Query(`
		SELECT
			id,
			username,
			phone,
			created_at
		FROM patients
		ORDER BY created_at DESC
	`)

	if err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	defer rows.Close()

	patients := []Patient{}

	for rows.Next() {

		var p Patient
		var createdAt time.Time

		err := rows.Scan(
			&p.ID,
			&p.Username,
			&p.Phone,
			&createdAt,
		)

		if err != nil {
			ctx.Status(500)
			ctx.JSON(map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		p.CreatedAt = createdAt.Format(time.RFC3339)

		patients = append(patients, p)
	}

	if err := rows.Err(); err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"count":    len(patients),
		"patients": patients,
	})
}

// ============================================================
// Get Patient
// ============================================================

func GetPatient(ctx *breeze.Context) {

	id, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid patient id",
		})
		return
	}

	var p Patient
	var createdAt time.Time

	err = db.QueryRow(`
		SELECT
			id,
			username,
			phone,
			created_at
		FROM patients
		WHERE id = $1
	`, id).Scan(
		&p.ID,
		&p.Username,
		&p.Phone,
		&createdAt,
	)

	if err == sql.ErrNoRows {
		ctx.Status(404)
		ctx.JSON(map[string]interface{}{
			"error": "Patient not found",
		})
		return
	}

	if err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	p.CreatedAt = createdAt.Format(time.RFC3339)

	ctx.JSON(map[string]interface{}{
		"patient": p,
	})
}

// ============================================================
// Update Patient
// ============================================================

func UpdatePatient(ctx *breeze.Context) {

	id, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid patient id",
		})
		return
	}

	var payload UpdatePatientRequest

	if err := json.Unmarshal(ctx.Req.Body, &payload); err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid JSON body",
		})
		return
	}

	username := strings.TrimSpace(payload.Username)
	phone := strings.TrimSpace(payload.Phone)

	// =========================
	// Required fields
	// =========================

	required := map[string]string{
		"username": username,
		"phone":    phone,
	}

	for field, value := range required {
		if value == "" {
			ctx.Status(400)
			ctx.JSON(map[string]interface{}{
				"error": field + " is required",
			})
			return
		}
	}

	// =========================
	// Update
	// =========================

	result, err := db.Exec(`
		UPDATE patients
		SET
			username = $1,
			phone = $2
		WHERE id = $3
	`,
		username,
		phone,
		id,
	)

	if err != nil {
		if isUniqueViolation(err) {
			ctx.Status(409)
			ctx.JSON(map[string]interface{}{
				"error": "Username or phone already exists",
			})
			return
		}

		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error":   "Failed to update patient",
			"details": err.Error(),
		})
		return
	}

	rows, err := result.RowsAffected()

	if err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if rows == 0 {
		ctx.Status(404)
		ctx.JSON(map[string]interface{}{
			"error": "Patient not found",
		})
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

// ============================================================
// Delete Patient
// ============================================================

func DeletePatient(ctx *breeze.Context) {

	id, err := strconv.ParseInt(ctx.Param("id"), 10, 64)
	if err != nil {
		ctx.Status(400)
		ctx.JSON(map[string]interface{}{
			"error": "Invalid patient id",
		})
		return
	}

	result, err := db.Exec(
		"DELETE FROM patients WHERE id = $1",
		id,
	)

	if err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	rows, err := result.RowsAffected()

	if err != nil {
		ctx.Status(500)
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if rows == 0 {
		ctx.Status(404)
		ctx.JSON(map[string]interface{}{
			"error": "Patient not found",
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "Patient deleted successfully",
	})
}

// ============================================================
// File Upload Helper
// ============================================================

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

	contentType := file.ContentType
	if contentType == "" {
		contentType = httpDetectContentType(file.Content)
	}

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

// isUniqueViolation reports whether err is a Postgres 23505 (unique_violation),
// which the patients table raises when a username or phone is already taken.
// Both columns are UNIQUE, so the create and update paths need to tell that
// case apart from a genuine failure to return 409 instead of 500.
func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

// ============================================================
// JSON Helper
// ============================================================

func jsonResponse(v interface{}) []byte {

	data, _ := json.Marshal(v)

	return data
}
