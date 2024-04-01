// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

// WaitForSpanConfigReconciliation waits for span config reconciliation to ensure
// that MR changes are propagated.
func WaitForSpanConfigReconciliation(t Fataler, tdb *SQLRunner) {
	tdb.Exec(t, "CREATE TABLE after AS SELECT now() AS after")
	tdb.CheckQueryResultsRetry(t, `
  WITH progress AS (
                    SELECT crdb_internal.pb_to_json(
                            'progress',
                            progress
                           )->'AutoSpanConfigReconciliation' AS p
                      FROM crdb_internal.system_jobs
                     WHERE status = 'running'
                ),
       checkpoint AS (
                    SELECT (p->'checkpoint'->>'wallTime')::FLOAT8 / 1e9 AS checkpoint
                      FROM progress
                     WHERE p IS NOT NULL
                  )
SELECT checkpoint > extract(epoch from after)
  FROM checkpoint, after`,
		[][]string{{"true"}})
}
