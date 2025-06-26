// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file

// ddl_generator_test.go
// Unit tests for ddl_generator.go
package workload_generator

import (
	"os"
	"reflect"
	"testing"
)

const (
	testZipDir = "/Users/pradyumagarwal/workloads/git-dbworkload/dbworkload_intProj/debug-zips/debug_2"
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
		`id INT PRIMARY KEY`,
		`name TEXT NOT NULL`,
		`age INT DEFAULT 30`,
	}
	wantConstraints := []string{
		`CONSTRAINT user_pk PRIMARY KEY (id)`,
		`UNIQUE (name)`,
		`FOREIGN KEY (age) REFERENCES other(age)`,
		`CHECK (age > 0)`,
	}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Errorf("splitColumnDefsAndTableConstraints cols = %v; want %v", cols, wantCols)
	}
	if !reflect.DeepEqual(constraints, wantConstraints) {
		t.Errorf("splitColumnDefsAndTableConstraints constraints = %v; want %v", constraints, wantConstraints)
	}
}

func TestParseDDLBasic(t *testing.T) {
	d := `CREATE TABLE IF NOT EXISTS schema.users (
		id INT PRIMARY KEY,
		name TEXT,
		age INT DEFAULT 18,
		email TEXT UNIQUE,
		country TEXT CHECK (country IN ('US','CA'))
    CHECK (age>0)
	)`
	schema, err := ParseDDL(d)
	if err != nil {
		t.Fatalf("ParseDDL() error = %v; want nil", err)
	}
	// Table name
	if schema.TableName != "schema.users" {
		t.Errorf("schema.TableName = %s; want %s", schema.TableName, "schema.users")
	}
	// Column order
	wantCols := []string{"id", "name", "age", "email", "country"}
	if !reflect.DeepEqual(schema.ColumnOrder, wantCols) {
		t.Errorf("schema.ColumnOrder = %v; want %v", schema.ColumnOrder, wantCols)
	}
	// Primary key
	if !reflect.DeepEqual(schema.PrimaryKeys, []string{"id"}) {
		t.Errorf("schema.PrimaryKeys = %v; want [id]", schema.PrimaryKeys)
	}
	// Default value
	ageCol, ok := schema.Columns["age"]
	if !ok {
		t.Fatalf("age column not found in schema.Columns")
	}
	if ageCol.Default != "18" {
		t.Errorf("ageCol.Default = %s; want %s", ageCol.Default, "18")
	}
	// Unique
	emailCol, ok := schema.Columns["email"]
	if !ok {
		t.Fatalf("email column not found in schema.Columns")
	}
	if !emailCol.IsUnique {
		t.Errorf("email column should be unique")
	}
	// Check constraint
	wantChecks := []string{"country IN ('US','CA')"}
	if !reflect.DeepEqual(schema.CheckConstraints, wantChecks) {
		t.Errorf("schema.CheckConstraints = %v; want %v", schema.CheckConstraints, wantChecks)
	}
}

func TestParseDDLTableConstraints(t *testing.T) {
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
	if err != nil {
		t.Fatalf("ParseDDL() error = %v; want nil", err)
	}
	// Composite primary key
	wantPK := []string{"order_id", "user_id"}
	if !reflect.DeepEqual(schema.PrimaryKeys, wantPK) {
		t.Errorf("schema.PrimaryKeys = %v; want %v", schema.PrimaryKeys, wantPK)
	}
	// Unique constraints
	if len(schema.UniqueConstraints) != 1 || !reflect.DeepEqual(schema.UniqueConstraints[0], []string{"amount"}) {
		t.Errorf("schema.UniqueConstraints = %v; want [[amount]]", schema.UniqueConstraints)
	}
	// Foreign keys
	if len(schema.ForeignKeys) != 1 {
		t.Fatalf("got %d foreign keys; want 1", len(schema.ForeignKeys))
	}
	fk := schema.ForeignKeys[0]
	// fk is [localCols, refTable, refCols]
	local, _ := fk[0].([]string)
	refTable, _ := fk[1].(string)
	refCols, _ := fk[2].([]string)
	if !reflect.DeepEqual(local, []string{"user_id"}) || refTable != "users" || !reflect.DeepEqual(refCols, []string{"id"}) {
		t.Errorf("got FK = %v; want [[user_id], users, [id]]", fk)
	}
	// Check constraints
	wantChecks := []string{"amount > 0"}
	if !reflect.DeepEqual(schema.CheckConstraints, wantChecks) {
		t.Errorf("schema.CheckConstraints = %v; want %v", schema.CheckConstraints, wantChecks)
	}
}

func TestParseDDLError(t *testing.T) {
	_, err := ParseDDL("INVALID DDL")
	if err == nil {
		t.Error("ParseDDL(\"INVALID DDL\") expected error; got nil")
	}
}

func TestGenerateDDLsIntegration(t *testing.T) {
	if _, err := os.Stat(testZipDir); os.IsNotExist(err) {
		t.Skipf("testZipDir %s not found; skipping integration test", testZipDir)
	}
	schemas, stmts, err := GenerateDDLs(testZipDir, testDBName, false)
	if err != nil {
		t.Fatalf("GenerateDDLs() error = %v; want nil", err)
	}
	if len(schemas) == 0 {
		t.Error("GenerateDDLs() returned empty schemas; want non-empty")
	}
	if len(stmts) == 0 {
		t.Error("GenerateDDLs() returned empty createStmts; want non-empty")
	}
}
