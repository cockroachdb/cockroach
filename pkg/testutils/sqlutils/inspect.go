// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
)

// InspectResult is the go struct for the row results for an
// INSPECT query.
type InspectResult struct {
	ErrorType  string
	Database   string
	Table      string
	PrimaryKey string
	Timestamp  time.Time
	Repaired   bool
	Details    string
}

// GetInspectResultRows will scan and unmarshal InspectResults from a Rows
// iterator. The Rows iterate must from an INSPECT query.
func GetInspectResultRows(rows *gosql.Rows) (results []InspectResult, err error) {
	defer rows.Close()

	var unused *string
	for rows.Next() {
		result := InspectResult{}
		if err := rows.Scan(
			&unused, /* job_uuid */
			&result.ErrorType,
			&result.Database,
			&result.Table,
			&result.PrimaryKey,
			&result.Timestamp,
			&result.Repaired,
			&result.Details,
		); err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if rows.Err() != nil {
		return nil, err
	}

	return results, nil
}

// RunInspect will run execute an exhaustive inspect check for a table.
func RunInspect(sqlDB *gosql.DB, database string, table string) error {
	return RunInspectWithOptions(sqlDB, database, table, "")
}

// RunInspectWithOptions will run an inspect check for a table with the specified options string.
func RunInspectWithOptions(sqlDB *gosql.DB, database string, table string, options string) error {
	if _, err := sqlDB.Exec(`SET enable_inspect_command = true;`); err != nil {
		return err
	}
	rows, err := sqlDB.Query(fmt.Sprintf(`INSPECT TABLE %s.%s %s`,
		database, table, options))
	if err != nil {
		return err
	}

	results, err := GetInspectResultRows(rows)
	if err != nil {
		return err
	}

	if len(results) > 0 {
		return errors.Errorf("expected no inspect results instead got: %#v", results)
	}
	return nil
}
