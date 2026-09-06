package main

import (
    "log"
    "os"
    "runtime"
    "strconv"

    "github.com/joho/godotenv"
    "github.com/nelthaarion/breeze"
    middleware "github.com/nelthaarion/breeze/middlewares"
)

func main() {

	// =========================
	// Database
	// =========================
	 if err := godotenv.Load(); err != nil {
        log.Println(".env file not found")
    }

    if err := InitDB(); err != nil {
        log.Fatal("Database connection failed:", err)
    }

	if db != nil {
		defer db.Close()

		if err := CreateTables(); err != nil {
			log.Fatal("Database migration failed:", err)
		}

		log.Println("Database connected successfully")
	} else {
		log.Println("DATABASE_URL is not set")
	}

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
			"message": "Hello from Breeze!",
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
	// Users
	// =========================

	router.HandleBlocking(breeze.GET, "/users", GetUsers) 
	router.HandleBlocking(breeze.GET, "/users/:id", GetUser) 
	router.HandleBlocking(breeze.POST, "/users", CreateUser) 
	router.HandleBlocking(breeze.PUT, "/users/:id", UpdateUser) 
	router.HandleBlocking(breeze.DELETE, "/users/:id", DeleteUser)

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