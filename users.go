
package main

import (
	"context"
	"strconv"
	"time"

	"github.com/nelthaarion/breeze"
)

type User struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type UpdateUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// =========================
// GET /users
// =========================

func GetUsers(ctx *breeze.Context) {
	if db == nil {
		ctx.JSON(map[string]interface{}{
			"error": "database is not connected",
		})
		return
	}

	rows, err := db.Query(
		context.Background(),
		`SELECT id, name, email, created_at
		 FROM users
		 ORDER BY id`,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	defer rows.Close()

	users := make([]User, 0)

	for rows.Next() {
		var user User

		if err := rows.Scan(
			&user.ID,
			&user.Name,
			&user.Email,
			&user.CreatedAt,
		); err != nil {
			ctx.JSON(map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"users": users,
	})
}

// =========================
// GET /users/:id
// =========================

func GetUser(ctx *breeze.Context) {
	if db == nil {
		ctx.JSON(map[string]interface{}{
			"error": "database is not connected",
		})
		return
	}

	id, err := getUserID(ctx)
	if err != nil {
		return
	}

	var user User

	err = db.QueryRow(
		context.Background(),
		`SELECT id, name, email, created_at
		 FROM users
		 WHERE id = $1`,
		id,
	).Scan(
		&user.ID,
		&user.Name,
		&user.Email,
		&user.CreatedAt,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": "user not found",
		})
		return
	}

	ctx.JSON(user)
}

// =========================
// POST /users
// =========================

func CreateUser(ctx *breeze.Context) {
	if db == nil {
		ctx.JSON(map[string]interface{}{
			"error": "database is not connected",
		})
		return
	}

	var req CreateUserRequest

	if err := ctx.Bind(&req); err != nil {
    ctx.JSON(map[string]interface{}{
        "error": "invalid JSON body",
    })
    return
}

	if req.Name == "" || req.Email == "" {
		ctx.JSON(map[string]interface{}{
			"error": "name and email are required",
		})
		return
	}

	var user User

	err := db.QueryRow(
		context.Background(),
		`INSERT INTO users (name, email)
		 VALUES ($1, $2)
		 RETURNING id, name, email, created_at`,
		req.Name,
		req.Email,
	).Scan(
		&user.ID,
		&user.Name,
		&user.Email,
		&user.CreatedAt,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "user created successfully",
		"user":    user,
	})
}

// =========================
// PUT /users/:id
// =========================

func UpdateUser(ctx *breeze.Context) {
	if db == nil {
		ctx.JSON(map[string]interface{}{
			"error": "database is not connected",
		})
		return
	}

	id, err := getUserID(ctx)
	if err != nil {
		return
	}

	var req UpdateUserRequest

	if err := ctx.Bind(&req); err != nil {
    ctx.JSON(map[string]interface{}{
        "error": "invalid JSON body",
    })
    return
}

	if req.Name == "" || req.Email == "" {
		ctx.JSON(map[string]interface{}{
			"error": "name and email are required",
		})
		return
	}

	var user User

	err = db.QueryRow(
		context.Background(),
		`UPDATE users
		 SET name = $1,
		     email = $2
		 WHERE id = $3
		 RETURNING id, name, email, created_at`,
		req.Name,
		req.Email,
		id,
	).Scan(
		&user.ID,
		&user.Name,
		&user.Email,
		&user.CreatedAt,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": "user not found",
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "user updated successfully",
		"user":    user,
	})
}

// =========================
// DELETE /users/:id
// =========================

func DeleteUser(ctx *breeze.Context) {
	if db == nil {
		ctx.JSON(map[string]interface{}{
			"error": "database is not connected",
		})
		return
	}

	id, err := getUserID(ctx)
	if err != nil {
		return
	}

	result, err := db.Exec(
		context.Background(),
		`DELETE FROM users WHERE id = $1`,
		id,
	)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if result.RowsAffected() == 0 {
		ctx.JSON(map[string]interface{}{
			"error": "user not found",
		})
		return
	}

	ctx.JSON(map[string]interface{}{
		"message": "user deleted successfully",
	})
}

// =========================
// Helper
// =========================

func getUserID(ctx *breeze.Context) (int64, error) {
	idString := ctx.Param("id")

	id, err := strconv.ParseInt(idString, 10, 64)

	if err != nil {
		ctx.JSON(map[string]interface{}{
			"error": "invalid user id",
		})
	}

	return id, err
}