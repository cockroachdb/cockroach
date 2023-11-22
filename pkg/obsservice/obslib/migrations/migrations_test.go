// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package migrations

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

// TestMigrationHaveColumns test that all migrations created for
// events have the required columns.
func TestMigrationHaveRequiredColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Required columns with any amount of space required for alignment (\s+).
	requiredColumns := []string{
		`timestamp\s+TIMESTAMPTZ`,
		`event_id\s+STRING`,
		`org_id\s+STRING`,
		`cluster_id\s+STRING`,
		`tenant_id\s+STRING`,
	}

	// Add migrations that don't need to contain the required columns and
	// can be skipped from this check.
	var ignoreMigrations []string
	migrationsDir := "sqlmigrations"

	migrations, err := GetDBMigrations().ReadDir(migrationsDir)
	require.NoError(t, err, "error while reading migrations directory")

	for _, migration := range migrations {
		if slices.Contains(ignoreMigrations, migration.Name()) {
			return
		}
		b, err := GetDBMigrations().ReadFile(fmt.Sprintf("%s/%s", migrationsDir, migration.Name()))
		require.NoError(t, err, fmt.Sprintf("error while reading migration %v", migration.Name()))

		migrationContent := string(b)
		for _, column := range requiredColumns {
			r := regexp.MustCompile(column)
			require.Equal(
				t,
				1,
				len(r.FindAllString(migrationContent, 1)),
				fmt.Sprintf("migration %v missing column: %v", migration.Name(), column),
			)
		}
	}
}
