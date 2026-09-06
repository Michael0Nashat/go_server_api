package main

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool

func InitDB() error {
	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		return nil
	}

	var err error

	db, err = pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return err
	}

	return db.Ping(context.Background())
}

func CreateTables() error {
	if db == nil {
		return nil
	}

	_, err := db.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS users (
			id BIGSERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)

	return err
}