// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// schema_generator_test.go
// Unit tests for schema_generator.go
package workload_generator

import (
	"bufio"
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	//testZipDir = "pkg/workload/workload_generator/test_data/debug"
	testDBName = "tpcc"
)

func TestSplitColumnDefsAndTableConstraints(t *testing.T) {
	body := `
		id INT PRIMARY KEY,
		name TEXT NOT NULL,
		age INT DEFAULT 30,
		CONSTRAINT user_pk PRIMARY KEY (id),
		UNIQUE (name),
		FOREIGN KEY (age) REFERENCES other(age),
		CHECK (age > 0)
	`
	cols, constraints := splitColumnDefsAndTableConstraints(body)
	wantCols := []string{
		"id INT PRIMARY KEY",
		"name TEXT NOT NULL",
		"age INT DEFAULT 30",
	}
	wantConstraints := []string{
		"CONSTRAINT user_pk PRIMARY KEY (id)",
		"UNIQUE (name)",
		"FOREIGN KEY (age) REFERENCES other(age)",
		"CHECK (age > 0)",
	}
	assert.Equal(t, wantCols, cols)
	assert.Equal(t, wantConstraints, constraints)
}

func TestParseDDL(t *testing.T) {
	t.Run("Basic DDL", func(t *testing.T) {
		d := `CREATE TABLE IF NOT EXISTS schema.users (
		id INT PRIMARY KEY,
		name TEXT,
		age INT DEFAULT 18,
		email TEXT UNIQUE,
		country TEXT CHECK (country IN ('US','CA'))
    CHECK (age>0)
	)`
		schema, err := ParseDDL(d)
		assert.NoError(t, err)
		// Table name
		assert.Equal(t, "schema.users", schema.TableName)
		// Column order
		wantCols := []string{"id", "name", "age", "email", "country"}
		assert.Equal(t, wantCols, schema.ColumnOrder)
		// Primary key
		assert.Equal(t, []string{"id"}, schema.PrimaryKeys)
		// Default value
		ageCol := schema.Columns["age"]
		assert.Equal(t, "18", ageCol.Default)
		// Unique
		emailCol := schema.Columns["email"]
		assert.True(t, emailCol.IsUnique)
		// Check constraint
		assert.Equal(t, []string{"country IN ('US','CA')"}, schema.CheckConstraints)
	})
	t.Run("Table Constraints", func(t *testing.T) {
		d := `CREATE TABLE orders (
		order_id INT,
		user_id INT,
		amount DECIMAL REFERENCES users(amt),
		PRIMARY KEY (order_id, user_id),
		UNIQUE (amount),
		FOREIGN KEY (user_id) REFERENCES users(id),
		CHECK (amount > 0)
	)`
		schema, err := ParseDDL(d)
		assert.NoError(t, err)
		// Composite primary key
		assert.Equal(t, []string{"order_id", "user_id"}, schema.PrimaryKeys)
		// Unique constraints
		assert.Len(t, schema.UniqueConstraints, 1)
		assert.Equal(t, []string{"amount"}, schema.UniqueConstraints[0])

		// Foreign keys
		assert.Len(t, schema.ForeignKeys, 1)
		fk := schema.ForeignKeys[0]
		local := fk[0].([]string)
		refTable := fk[1].(string)
		refCols := fk[2].([]string)
		assert.Equal(t, []string{"user_id"}, local)
		assert.Equal(t, "users", refTable)
		assert.Equal(t, []string{"id"}, refCols)
		// Check constraints
		assert.Equal(t, []string{"amount > 0"}, schema.CheckConstraints)
	})
	t.Run("Invalid DDL", func(t *testing.T) {
		_, err := ParseDDL("INVALID DDL")
		assert.Error(t, err)
	})
}

//go:embed test_data/debug/crdb_internal.create_statements.txt
var data string

func TestGenerateDDLsIntegration(t *testing.T) {
	t.Run("expect success", func(t *testing.T) {
		schemas, stmts, err := generateDDLFromCSV(strings.NewReader(data), testDBName, false)
		assert.NoError(t, err)
		assert.NotEmpty(t, schemas)
		assert.NotEmpty(t, stmts)
	})
	t.Run("expect failure due to invalid file location", func(t *testing.T) {
		_, _, err := generateDDLs("wrong_file_location", testDBName, "", false)
		assert.NotNil(t, err)
	})
}

func TestGenerateDDLFromDDLFile(t *testing.T) {
	// Sample DDL statements for testing
	sampleDDL := `"CREATE TABLE public.users (
		id INT8 NOT NULL,
		name VARCHAR(50) NOT NULL,
		email VARCHAR(100) UNIQUE,
		CONSTRAINT users_pkey PRIMARY KEY (id)
	);"

	CREATE TABLE public.orders (
		order_id INT8 NOT NULL,
		user_id INT8 NOT NULL,
		amount DECIMAL(10,2) NOT NULL,
		CONSTRAINT orders_pkey PRIMARY KEY (order_id)
	);

	ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);
	`

	t.Run("expect success with DDL file", func(t *testing.T) {
		reader := strings.NewReader(sampleDDL)
		schemas, stmts, err := generateDDLFromDDLFile(bufio.NewReader(reader), testDBName, false)
		assert.NoError(t, err)
		assert.NotEmpty(t, schemas)
		assert.NotEmpty(t, stmts)

		// Verify that both tables were parsed correctly
		assert.Contains(t, schemas, "users")
		assert.Contains(t, schemas, "orders")

		// Verify that the users table has the correct columns
		usersSchema := schemas["users"]
		assert.Contains(t, usersSchema.Columns, "id")
		assert.Contains(t, usersSchema.Columns, "name")
		assert.Contains(t, usersSchema.Columns, "email")

		// Verify that the orders table has the correct columns
		ordersSchema := schemas["orders"]
		assert.Contains(t, ordersSchema.Columns, "order_id")
		assert.Contains(t, ordersSchema.Columns, "user_id")
		assert.Contains(t, ordersSchema.Columns, "amount")
	})
}
