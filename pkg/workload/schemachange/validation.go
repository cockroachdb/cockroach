// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/errors"
)

// InvalidObject represents an invalid database object found during validation.
type InvalidObject struct {
	ID           int
	DatabaseName string
	SchemaName   string
	ObjName      string
	Error        string
}

// ValidateInvalidObjects checks for invalid objects in the database by querying
// crdb_internal.invalid_objects. It returns a slice of InvalidObject structs
// representing any invalid objects found, or an error if the query fails.
//
// This function is useful for validating that schema change operations haven't
// left the database in an inconsistent state with orphaned or invalid objects.
func ValidateInvalidObjects(ctx context.Context, db *gosql.DB) ([]InvalidObject, error) {
	query := `SELECT id, database_name, schema_name, obj_name, error FROM crdb_internal.invalid_objects`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to query invalid objects")
	}

	var invalidObjects []InvalidObject
	for rows.Next() {
		var obj InvalidObject
		if err := rows.Scan(&obj.ID, &obj.DatabaseName, &obj.SchemaName, &obj.ObjName, &obj.Error); err != nil {
			return nil, errors.Wrapf(err, "failed to scan invalid object row")
		}
		invalidObjects = append(invalidObjects, obj)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "error iterating invalid objects")
	}

	return invalidObjects, nil
}
