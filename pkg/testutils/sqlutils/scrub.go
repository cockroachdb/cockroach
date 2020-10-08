// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
)

// ScrubResult is the go struct for the row results for an
// EXPERIMENTAL SCRUB query.
type ScrubResult struct {
	ErrorType  string
	Database   string
	Table      string
	PrimaryKey string
	Timestamp  time.Time
	Repaired   bool
	Details    string
}

// GetScrubResultRows will scan and unmarshal ScrubResults from a Rows
// iterator. The Rows iterate must from an EXPERIMENTAL SCRUB query.
func GetScrubResultRows(rows *gosql.Rows) (results []ScrubResult, err error) {
	defer rows.Close()

	var unused *string
	for rows.Next() {
		result := ScrubResult{}
		if err := rows.Scan(
			// TODO(joey): In the future, SCRUB will run as a job during execution.
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

// RunScrub will run execute an exhaustive scrub check for a table.
func RunScrub(sqlDB *gosql.DB, database string, table string) error {
	return RunScrubWithOptions(sqlDB, database, table, "")
}

// RunScrubWithOptions will run a SCRUB check for a table with the specified options string.
func RunScrubWithOptions(sqlDB *gosql.DB, database string, table string, options string) error {
	rows, err := sqlDB.Query(fmt.Sprintf(`EXPERIMENTAL SCRUB TABLE %s.%s %s`,
		database, table, options))
	if err != nil {
		return err
	}

	results, err := GetScrubResultRows(rows)
	if err != nil {
		return err
	}

	if len(results) > 0 {
		return errors.Errorf("expected no scrub results instead got: %#v", results)
	}
	return nil
}
