// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

// InspectResult is a struct for the row results for a SHOW INSPECT ERRORS query.
type InspectResult struct {
	ErrorType  string
	Database   string
	Schema     string
	Table      string
	PrimaryKey string
	JobID      int64
	Aost       string
	Details    string
}

// getInspectResultRows will scan and unmarshal InspectResults from a Rows
// iterator. The Rows iterate must from a SHOW INSPECT ERRORS query.
func getInspectResultRows(rows *gosql.Rows) (results []InspectResult, err error) {
	defer rows.Close()

	for rows.Next() {
		result := InspectResult{}
		if err := rows.Scan(
			&result.ErrorType,
			&result.Database,
			&result.Schema,
			&result.Table,
			&result.PrimaryKey,
			&result.JobID,
			&result.Aost,
			&result.Details,
		); err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// RunInspect will run execute an exhaustive inspect check for a table.
func RunInspect(sqlDB *gosql.DB, database string, table string) error {
	return runInspectWithOptions(sqlDB, database, table, "")
}

// runInspectWithOptions will run an inspect check for a table with the specified options string.
func runInspectWithOptions(sqlDB *gosql.DB, database string, table string, options string) error {
	if _, err := sqlDB.Exec(fmt.Sprintf(`INSPECT TABLE %s.%s %s`, database, table, options)); err == nil {
		return nil
	} else if !(strings.Contains(err.Error(), "INSPECT found inconsistencies") || strings.Contains(err.Error(), "INSPECT encountered internal errors")) {
		return err
	}

	rows, err := sqlDB.Query(fmt.Sprintf(`SHOW INSPECT ERRORS FOR TABLE %s.%s WITH DETAILS`, database, table))
	if err != nil {
		return err
	}
	defer rows.Close()

	results, err := getInspectResultRows(rows)
	if err != nil {
		return err
	}

	if len(results) > 0 {
		return errors.Errorf("expected no inspect results instead got: %#v", results)
	}

	return nil
}
