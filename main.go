package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
// Every 5xx is also reported to the circuit breaker (see protect()).
func fail(ctx *breeze.Context, status int, msg string, err error) {
	if status >= 500 {
		markFailed(ctx)
	}
	ctx.Status(status)
	body := map[string]interface{}{"error": msg}
	if err != nil {
		body["details"] = err.Error()
	}
	ctx.JSON(body)
}

// ============================================================
// Resilience: globals
// ============================================================

var (
	// bgCtx stops background goroutines (limiter cleanup) on shutdown.
	bgCtx, bgCancel = context.WithCancel(context.Background())

	shuttingDown atomic.Bool
	inflight     atomic.Int64

	inflightSem chan struct{} // backpressure: max concurrent HTTP handlers
	semWait     time.Duration // how long a request may wait for a slot

	dbBreaker  *CircuitBreaker
	jobQueue   *JobQueue
	wsLimiter  *RateLimiter // per-WebSocket-connection message limiter
	maxWSConns int

	// Per-request "this request hit a 5xx" marker, keyed by *breeze.Context.
	// Written by fail(), read by protect() to feed the circuit breaker.
	reqFailed sync.Map
)

// ============================================================
// Resilience: env helpers
// ============================================================

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func initResilience() {
	inflightSem = make(chan struct{}, envInt("MAX_INFLIGHT_REQUESTS", 100))
	semWait = envDuration("BACKPRESSURE_WAIT", 250*time.Millisecond)

	dbBreaker = NewCircuitBreaker(
		envInt("BREAKER_THRESHOLD", 5),
		envDuration("BREAKER_COOLDOWN", 15*time.Second),
		1,
	)

	jobQueue = NewJobQueue(
		envInt("QUEUE_WORKERS", 8),
		envInt("QUEUE_SIZE", 1024),
		envDuration("QUEUE_JOB_TIMEOUT", 15*time.Second),
	)

	wsLimiter = NewRateLimiter(5, 10) // 5 msgs/sec sustained, bursts of 10
	maxWSConns = envInt("MAX_WS_CONNECTIONS", 5000)
}

// ============================================================
// Resilience: Rate Limiter (token bucket, per key)
// ============================================================

type bucket struct {
	tokens float64
	last   time.Time
}

type RateLimiter struct {
	mu      sync.Mutex
	rate    float64 // tokens added per second
	burst   float64
	buckets map[string]*bucket
}

func NewRateLimiter(ratePerSec float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		rate:    ratePerSec,
		burst:   float64(burst),
		buckets: make(map[string]*bucket),
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *RateLimiter) Allow(key string) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		rl.buckets[key] = &bucket{tokens: rl.burst - 1, last: now}
		return true
	}

	b.tokens += now.Sub(b.last).Seconds() * rl.rate
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (rl *RateLimiter) Remove(key string) {
	rl.mu.Lock()
	delete(rl.buckets, key)
	rl.mu.Unlock()
}

// cleanupLoop evicts idle buckets so the map cannot grow without bound.
func (rl *RateLimiter) cleanupLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-bgCtx.Done():
			return
		case <-t.C:
			cutoff := time.Now().Add(-5 * time.Minute)
			rl.mu.Lock()
			for k, b := range rl.buckets {
				if b.last.Before(cutoff) {
					delete(rl.buckets, k)
				}
			}
			rl.mu.Unlock()
		}
	}
}

// globalKey: one shared bucket per limiter. Works as a coarse load-shedding
// valve. Replace with a per-IP key once you know how Breeze exposes the
// remote address / X-Forwarded-For header.
func globalKey(*breeze.Context) string { return "global" }

// jsonFieldKey limits per account (e.g. the login "email"), which is what
// stops credential-guessing without needing the client IP.
func jsonFieldKey(field string) func(*breeze.Context) string {
	return func(ctx *breeze.Context) string {
		var m map[string]interface{}
		if json.Unmarshal(ctx.Req.Body, &m) == nil {
			if s, ok := m[field].(string); ok {
				if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
					return s
				}
			}
		}
		return "unknown"
	}
}

// ============================================================
// Resilience: Circuit Breaker
// ============================================================

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type CircuitBreaker struct {
	mu        sync.Mutex
	state     breakerState
	failures  int
	threshold int
	cooldown  time.Duration
	openedAt  time.Time
	probes    int // in-flight half-open probes
	maxProbes int
}

func NewCircuitBreaker(threshold int, cooldown time.Duration, maxProbes int) *CircuitBreaker {
	return &CircuitBreaker{threshold: threshold, cooldown: cooldown, maxProbes: maxProbes}
}

// Allow reports whether a call may proceed. Every true MUST be followed by
// exactly one Success() or Failure().
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if time.Since(cb.openedAt) < cb.cooldown {
			return false
		}
		cb.state = breakerHalfOpen
		cb.probes = 1
		log.Println("circuit breaker: half-open, probing database")
		return true
	default: // half-open
		if cb.probes < cb.maxProbes {
			cb.probes++
			return true
		}
		return false
	}
}

func (cb *CircuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == breakerHalfOpen {
		log.Println("circuit breaker: closed (database recovered)")
	}
	cb.state = breakerClosed
	cb.failures = 0
	cb.probes = 0
}

func (cb *CircuitBreaker) Failure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case breakerHalfOpen:
		cb.trip()
	case breakerClosed:
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.trip()
		}
	}
}

func (cb *CircuitBreaker) trip() { // caller holds the lock
	cb.state = breakerOpen
	cb.openedAt = time.Now()
	cb.probes = 0
	cb.failures = 0
	log.Printf("circuit breaker: OPEN for %s", cb.cooldown)
}

func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	}
	return "closed"
}

// ============================================================
// Resilience: Job Queue (bounded, worker pool)
// ============================================================

var (
	ErrQueueFull   = errors.New("job queue is full")
	ErrQueueClosed = errors.New("job queue is closed")
)

type Job func(ctx context.Context)

type JobQueue struct {
	jobs    chan Job
	wg      sync.WaitGroup
	mu      sync.RWMutex // guards closed + the close(jobs) call
	closed  bool
	timeout time.Duration
}

func NewJobQueue(workers, size int, jobTimeout time.Duration) *JobQueue {
	q := &JobQueue{jobs: make(chan Job, size), timeout: jobTimeout}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

func (q *JobQueue) worker() {
	defer q.wg.Done()
	for job := range q.jobs {
		q.run(job)
	}
}

func (q *JobQueue) run(job Job) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("job panic recovered: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()
	job(ctx)
}

// Submit never blocks: a full queue is reported immediately so the caller can
// push back on its client (that is the backpressure).
func (q *JobQueue) Submit(j Job) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrQueueClosed
	}
	select {
	case q.jobs <- j:
		return nil
	default:
		return ErrQueueFull
	}
}

// Shutdown stops intake, then waits for queued jobs to finish or ctx to expire.
func (q *JobQueue) Shutdown(ctx context.Context) {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
	q.mu.Unlock()

	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("shutdown: job queue drained")
	case <-ctx.Done():
		log.Println("shutdown: job queue drain timed out")
	}
}

// runOrQueue defers non-critical work to the queue; if the queue is full or
// closed it runs inline so the work is never silently dropped.
func runOrQueue(fn func()) {
	if err := jobQueue.Submit(func(context.Context) { fn() }); err != nil {
		fn()
	}
}

// ============================================================
// Resilience: protect() = rate limit + backpressure + circuit breaker
// ============================================================

func reject(ctx *breeze.Context, status int, msg string, retryAfterSec int) {
	ctx.Status(status)
	ctx.JSON(map[string]interface{}{
		"error":       msg,
		"retry_after": retryAfterSec,
	})
}

// markFailed is called by fail() for every 5xx.
func markFailed(ctx *breeze.Context) { reqFailed.Store(ctx, true) }

func protect(h func(*breeze.Context), rl *RateLimiter, key func(*breeze.Context) string) func(*breeze.Context) {
	return func(ctx *breeze.Context) {

		if shuttingDown.Load() {
			reject(ctx, 503, "Server is shutting down", 5)
			return
		}

		if rl != nil && !rl.Allow(key(ctx)) {
			reject(ctx, 429, "Too many requests", 1)
			return
		}

		// Backpressure: wait briefly for a slot, then shed load.
		select {
		case inflightSem <- struct{}{}:
		case <-time.After(semWait):
			reject(ctx, 503, "Server is busy, try again shortly", 1)
			return
		}
		inflight.Add(1)
		defer func() {
			inflight.Add(-1)
			<-inflightSem
		}()

		if !dbBreaker.Allow() {
			reject(ctx, 503, "Service temporarily unavailable", 15)
			return
		}

		completed := false
		defer func() {
			_, failed := reqFailed.LoadAndDelete(ctx)
			if failed || !completed { // a panic counts as a failure
				dbBreaker.Failure()
			} else {
				dbBreaker.Success()
			}
		}()

		h(ctx)
		completed = true
	}
}

// ============================================================
// Resilience: Postgres connection parameters (server-side timeouts)
// ============================================================

// withPGParams adds runtime parameters that lib/pq forwards to Postgres at
// connect time, without overriding anything already in the URL/DSN.
func withPGParams(dsn string, params map[string]string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return dsn
		}
		q := u.Query()
		for k, v := range params {
			if q.Get(k) == "" {
				q.Set(k, v)
			}
		}
		u.RawQuery = q.Encode()
		return u.String()
	}
	for k, v := range params {
		if !strings.Contains(dsn, k+"=") {
			dsn += fmt.Sprintf(" %s=%s", k, v)
		}
	}
	return dsn
}

// ============================================================
// Resilience: Graceful shutdown
// ============================================================

func gracefulShutdown(timeout time.Duration) {
	log.Println("shutdown: draining…")

	// 1. Stop taking new work (protect() now answers 503, /health goes 503).
	shuttingDown.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 2. Wait for in-flight HTTP handlers.
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
wait:
	for inflight.Load() > 0 {
		select {
		case <-ctx.Done():
			log.Printf("shutdown: %d requests still running, giving up", inflight.Load())
			break wait
		case <-t.C:
		}
	}

	// 3. Drain queued jobs (WebSocket messages, async updates).
	jobQueue.Shutdown(ctx)

	// 4. Stop background goroutines. The DB is closed by main's defer.
	bgCancel()
	log.Println("shutdown: complete")
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

	initResilience()

	// Rate limiters (rate/sec, burst)
	apiLimiter := NewRateLimiter(100, 200) // coarse global valve
	loginLimiter := NewRateLimiter(0.2, 5) // per account: 5 tries, then 1 per 5s

	api := func(h func(*breeze.Context)) func(*breeze.Context) {
		return protect(h, apiLimiter, globalKey)
	}

	router := breeze.NewRouter()

	router.Use(middleware.RecoveryMiddleware())
	router.Use(middleware.LoggingMiddleware())

	router.Handle(breeze.GET, "/", func(ctx *breeze.Context) {
		ctx.JSON(map[string]interface{}{
			"message": "Doctor API",
			"status":  "ok",
		})
	})

	// Readiness: 503 while draining so a load balancer stops sending traffic.
	router.Handle(breeze.GET, "/health", func(ctx *breeze.Context) {
		st := db.Stats()
		status := "healthy"
		if shuttingDown.Load() {
			ctx.Status(503)
			status = "shutting_down"
		}
		ctx.JSON(map[string]interface{}{
			"status":        status,
			"db_breaker":    dbBreaker.State(),
			"db_open_conns": st.OpenConnections,
			"db_in_use":     st.InUse,
			"db_wait_count": st.WaitCount,
			"inflight":      inflight.Load(),
		})
	})

	// ---------- Doctors ----------
	router.HandleBlocking(breeze.POST, "/doctors", api(CreateDoctor))
	router.HandleBlocking(breeze.GET, "/doctors", api(GetDoctors))
	router.HandleBlocking(breeze.GET, "/doctors/:id", api(GetDoctor))
	router.HandleBlocking(breeze.PUT, "/doctors/:id", api(UpdateDoctor))
	router.HandleBlocking(breeze.DELETE, "/doctors/:id", api(DeleteDoctor))

	// Static segment, so it wins over "/doctors/:id" for POST lookups.
	router.HandleBlocking(breeze.POST, "/doctors/login", protect(LoginDoctor, loginLimiter, jsonFieldKey("email")))

	router.HandleBlocking(breeze.PATCH, "/doctors/:id/status", api(UpdateDoctorStatus))

	// The SvelteKit dashboard proxies this as POST /api/doctors/:id/chat-image.
	router.HandleBlocking(breeze.POST, "/doctors/:id/chat-image", api(SaveDoctorChatImage))

	// ---------- Patients ----------
	router.HandleBlocking(breeze.POST, "/patients", api(CreatePatient))

	// Static segment wins over "/patients/:id" for POST lookups.
	router.HandleBlocking(breeze.POST, "/patients/login", protect(LoginPatient, loginLimiter, jsonFieldKey("username")))

	router.HandleBlocking(breeze.GET, "/patients", api(GetPatients))
	router.HandleBlocking(breeze.GET, "/patients/:id", api(GetPatient))
	router.HandleBlocking(breeze.PUT, "/patients/:id", api(UpdatePatient))
	router.HandleBlocking(breeze.DELETE, "/patients/:id", api(DeletePatient))

	// The SvelteKit dashboard proxies this as POST /api/patients/:id/chat-image.
	router.HandleBlocking(breeze.POST, "/patients/:id/chat-image", api(SavePatientChatImage))

	// ---------- Work Places ----------
	router.HandleBlocking(breeze.POST, "/work-places", api(CreateWorkPlace))
	router.HandleBlocking(breeze.GET, "/work-places", api(GetWorkPlaces))
	router.HandleBlocking(breeze.GET, "/work-places/:id", api(GetWorkPlace))
	router.HandleBlocking(breeze.PUT, "/work-places/:id", api(UpdateWorkPlace))
	router.HandleBlocking(breeze.DELETE, "/work-places/:id", api(DeleteWorkPlace))
	router.HandleBlocking(breeze.POST, "/work-places/:id/schedules", api(SetWorkSchedules))

	// ---------- Chat ----------
	router.HandleBlocking(breeze.POST, "/conversations", api(CreateConversation))
	router.HandleBlocking(breeze.GET, "/conversations", api(GetConversations))
	router.HandleBlocking(breeze.GET, "/conversations/:id/messages", api(GetMessages))
	router.HandleBlocking(breeze.POST, "/conversations/:id/messages", api(SendMessage))
	router.HandleBlocking(breeze.POST, "/conversations/:id/messages/image", api(SendImageMessage))

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

	// Run the server in the background; wait for a signal or a fatal error.
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("Server running on port %d", port)
		serverErr <- app.Run(port, true)
	}()

	select {
	case err := <-serverErr:
		log.Fatal(err)
	case <-sigCtx.Done():
		stop() // a second Ctrl+C now kills the process immediately
		gracefulShutdown(envDuration("SHUTDOWN_TIMEOUT", 25*time.Second))
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

	// Server-side timeouts apply to EVERY query and transaction, so no handler
	// can hang forever on a slow query, a lock, or an abandoned transaction.
	databaseURL = withPGParams(databaseURL, map[string]string{
		"connect_timeout":                     "10",    // seconds
		"statement_timeout":                   "10000", // ms
		"lock_timeout":                        "5000",  // ms
		"idle_in_transaction_session_timeout": "15000", // ms
	})

	var err error
	db, err = sql.Open("postgres", databaseURL)
	if err != nil {
		return err
	}

	// Connection pool
	db.SetMaxOpenConns(envInt("DB_MAX_OPEN_CONNS", 25))
	db.SetMaxIdleConns(envInt("DB_MAX_IDLE_CONNS", 10))
	db.SetConnMaxLifetime(envDuration("DB_CONN_MAX_LIFETIME", 30*time.Minute))
	db.SetConnMaxIdleTime(envDuration("DB_CONN_MAX_IDLE_TIME", 5*time.Minute))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return db.PingContext(ctx)
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
		if isBadInputError(err) {
			fail(ctx, 400, "Invalid doctor data (check birth_date, professional_degree, governorate and field lengths)", nil)
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

	// Same required-field validation as CreateDoctor, so bad client input
	// returns 400 instead of a 500 that would count against the breaker.
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
		if isBadInputError(err) {
			fail(ctx, 400, "Invalid doctor data (check birth_date, professional_degree, governorate and field lengths)", nil)
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
		if isBadInputError(err) {
			fail(ctx, 400, "Invalid patient data (username or phone is too long)", nil)
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
		if isBadInputError(err) {
			fail(ctx, 400, "Invalid patient data (username or phone is too long)", nil)
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

	// Use NULLIF only if message is not empty after trim; otherwise pass the trimmed message
	// so the CHECK constraint (NULLIF(TRIM(message), '') IS NOT NULL OR image_data IS NOT NULL)
	// is satisfied when image_data is provided.
	var messageForDB string
	if strings.TrimSpace(message) == "" {
		messageForDB = message // keep as empty string; constraint will check image_data IS NOT NULL
	} else {
		messageForDB = strings.TrimSpace(message)
	}

	err := db.QueryRow(`
		INSERT INTO messages (conversation_id, sender_type, sender_id, message, image_data)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, conversation_id, sender_type, sender_id, message, image_data, created_at
	`, conversationID, senderType, senderID, messageForDB, imageData).Scan(
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

	// Ensure the constraint (NULLIF(TRIM(message), '') IS NOT NULL OR image_data IS NOT NULL) is satisfied.
	// If message is empty/whitespace, image_data must be provided; if message has content, constraint is satisfied.
	// Default message to a space only when there's no image data to avoid constraint violation.
	if strings.TrimSpace(message) == "" && imageData == nil {
		message = " "
	}

	m, err := insertMessage(conversationID, senderType, senderID, message, imageData)
	if err != nil {
		// Log the full error for debugging, but show a user-friendly message
		log.Printf("Failed to save image message: %v", err)
		fail(ctx, 500, "تعذّر حفظ رسالة الصورة", err)
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
//  2. first_message on all of the doctor's existing conversations
//     (done in the background job queue).
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

	// The image is already saved, so this fan-out doesn't block the response.
	runOrQueue(func() {
		if _, err := db.Exec(`
			UPDATE conversations
			SET first_message = $1::jsonb, updated_at = NOW()
			WHERE doctor_id = $2
		`, fm, doctorID); err != nil {
			log.Println("failed to update first_message for doctor:", err)
		}
	})

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

	runOrQueue(func() {
		if _, err := db.Exec(`
			UPDATE conversations
			SET first_message = $1::jsonb, updated_at = NOW()
			WHERE patient_id = $2
		`, fm, patientID); err != nil {
			log.Println("failed to update first_message for patient:", err)
		}
	})

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

func connKey(conn *breeze.WSConn) string { return fmt.Sprintf("%p", conn) }

func chatWSOnConnect(conn *breeze.WSConn) {
	chatWS.Lock()
	if len(chatWS.clients) >= maxWSConns {
		chatWS.Unlock()
		_ = conn.SendText(`{"type":"error","error":"Server is at capacity, try again later"}`)
		return // not registered, so every message from it is ignored
	}
	chatWS.clients[conn] = ""
	chatWS.Unlock()
	_ = conn.SendText(`{"type":"connected","message":"Send a subscribe event to join a conversation"}`)
}

func chatWSOnClose(conn *breeze.WSConn, code uint16, reason string) {
	chatWS.Lock()
	delete(chatWS.clients, conn)
	chatWS.Unlock()
	wsLimiter.Remove(connKey(conn))
}

// normalizeImageDataURL validates a base64 image data URL received over the
// WebSocket. Like the REST upload path, it decodes the bytes and detects the
// real type instead of trusting the client's declared MIME type.
// It returns the data URL rebuilt with the detected type.
func normalizeImageDataURL(s string) (string, error) {

	s = strings.TrimSpace(s)
	const marker = ";base64,"

	if !strings.HasPrefix(s, "data:") {
		return "", errors.New("image must be a base64 data URL")
	}
	idx := strings.Index(s, marker)
	if idx < 0 {
		return "", errors.New("image must be a base64 data URL")
	}

	b64 := s[idx+len(marker):]
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", errors.New("image is not valid base64")
	}
	if len(raw) == 0 {
		return "", errors.New("image is empty")
	}
	if len(raw) > MaxImageSize {
		return "", errors.New("image too large")
	}

	contentType := httpDetectContentType(raw)
	switch contentType {
	case "image/jpeg", "image/png", "image/webp":
	default:
		return "", fmt.Errorf("invalid image type: %s", contentType)
	}

	return "data:" + contentType + ";base64," + b64, nil
}

func chatWSOnMessage(conn *breeze.WSConn, opcode byte, payload []byte) {

	if opcode != 1 { // text frames only for the JSON chat protocol
		return
	}

	chatWS.RLock()
	_, registered := chatWS.clients[conn]
	chatWS.RUnlock()
	if !registered {
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
		if !dbBreaker.Allow() {
			_ = conn.SendText(`{"type":"error","error":"Service temporarily unavailable"}`)
			return
		}
		ok, err := conversationExistsForSender(req.ConversationID, req.SenderType, req.SenderID)
		if err != nil {
			dbBreaker.Failure()
			_ = conn.SendText(`{"type":"error","error":"Failed to validate conversation"}`)
			return
		}
		dbBreaker.Success()
		if !ok {
			_ = conn.SendText(`{"type":"error","error":"Sender is not a member of this conversation"}`)
			return
		}
		chatWS.Lock()
		chatWS.clients[conn] = req.ConversationID
		chatWS.Unlock()
		_ = conn.SendText(`{"type":"subscribed"}`)

	case "message":
		if shuttingDown.Load() {
			_ = conn.SendText(`{"type":"error","error":"Server is shutting down"}`)
			return
		}
		if !wsLimiter.Allow(connKey(conn)) {
			_ = conn.SendText(`{"type":"error","error":"Rate limit exceeded, slow down"}`)
			return
		}

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

		rawImage := strings.TrimSpace(req.Image)
		if strings.TrimSpace(req.Message) == "" && rawImage == "" {
			_ = conn.SendText(`{"type":"error","error":"message or image is required"}`)
			return
		}
		// base64 inflates by ~4/3; reject oversized images before doing any work.
		if len(rawImage) > MaxImageSize*4/3+1024 {
			_ = conn.SendText(`{"type":"error","error":"Image too large"}`)
			return
		}

		// All DB work (and image decoding) moves off the socket-reading
		// goroutine into the queue.
		err := jobQueue.Submit(func(context.Context) {

			var imgPtr *string
			if rawImage != "" {
				normalized, err := normalizeImageDataURL(rawImage)
				if err != nil {
					b, _ := json.Marshal(ChatWSEvent{Type: "error", Error: err.Error()})
					_ = conn.SendText(string(b))
					return
				}
				imgPtr = &normalized
			}

			if !dbBreaker.Allow() {
				_ = conn.SendText(`{"type":"error","error":"Service temporarily unavailable"}`)
				return
			}
			ok, err := conversationExistsForSender(req.ConversationID, req.SenderType, req.SenderID)
			if err != nil {
				dbBreaker.Failure()
				_ = conn.SendText(`{"type":"error","error":"Failed to validate conversation"}`)
				return
			}
			if !ok {
				dbBreaker.Success()
				_ = conn.SendText(`{"type":"error","error":"Sender is not a member of this conversation"}`)
				return
			}
			m, err := insertMessage(req.ConversationID, req.SenderType, req.SenderID, req.Message, imgPtr)
			if err != nil {
				dbBreaker.Failure()
				log.Println("ws insertMessage error:", err)
				_ = conn.SendText(`{"type":"error","error":"Failed to save message"}`)
				return
			}
			dbBreaker.Success()
			broadcastChatMessage(m)
		})
		if err != nil { // queue full or closed: tell the client to back off
			_ = conn.SendText(`{"type":"error","error":"Server is busy, retry shortly"}`)
		}

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
		if isBadInputError(err) {
			fail(ctx, 400, "Invalid work place data (check place_type, exam_price and field lengths)", nil)
			return
		}
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
		places = append(places, wp)
	}

	if err := rows.Err(); err != nil {
		fail(ctx, 500, "Failed to read work places", err)
		return
	}
	rows.Close() // release the connection BEFORE issuing more queries (avoids pool deadlock)

	for i := range places {
		places[i].Schedules = loadSchedules(places[i].ID)
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
		if isBadInputError(err) {
			fail(ctx, 400, "Invalid work place data (check place_type, exam_price and field lengths)", nil)
			return
		}
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

	// Try to detect content type from file extension as fallback
	// since http.DetectContentType may not work for all image formats
	if !isValidContentType(contentType, allowedTypes) && file.Filename != "" {
		ext := strings.ToLower(filepath.Ext(file.Filename))
		contentType = guessContentTypeFromExt(ext)
		if contentType != "" && isValidContentType(contentType, allowedTypes) {
			// Use the extension-detected content type
		} else if contentType != "" {
			contentType = httpDetectContentType(file.Content) // fall back to original
		}
	}

	valid := false
	for _, allowed := range allowedTypes {
		if contentType == allowed {
			valid = true
			break
		}
	}
	if !valid {
		return nil, fmt.Errorf("invalid file type for %s: detected %s", fieldName, contentType)
	}

	encoded := base64.StdEncoding.EncodeToString(file.Content)
	result := "data:" + contentType + ";base64," + encoded
	return &result, nil
}

func isValidContentType(contentType string, allowedTypes []string) bool {
	for _, allowed := range allowedTypes {
		if contentType == allowed {
			return true
		}
	}
	return false
}

func guessContentTypeFromExt(ext string) string {
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".webp":
		return "image/webp"
	case ".gif":
		return "image/gif"
	default:
		return ""
	}
}

func httpDetectContentType(data []byte) string {
	return http.DetectContentType(data)
}

// ============================================================
// Database Error Helpers
// ============================================================

// pgCode returns the Postgres SQLSTATE of err, or "" if it is not a pq error.
func pgCode(err error) string {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}
	return ""
}

// isUniqueViolation reports whether err is a Postgres 23505 (unique_violation)
// so create/update paths can return 409 instead of 500.
func isUniqueViolation(err error) bool {
	return pgCode(err) == "23505"
}

// isBadInputError reports whether err was caused by client-supplied data
// rather than a server fault, so the handler can answer 400 instead of 500:
//   - 23514 check_violation (e.g. invalid governorate / professional_degree)
//   - 23502 not_null_violation
//   - class 22 data exceptions (bad date format, value too long, ...)
func isBadInputError(err error) bool {
	code := pgCode(err)
	if code == "" {
		return false
	}
	return code == "23514" || code == "23502" || strings.HasPrefix(code, "22")
}