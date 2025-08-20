// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestTableSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	runner := sqlutils.MakeSQLRunner(db)

	const jobID jobspb.JobID = 50

	tableLogger := getInspectTableLogger(s.InternalDB().(descs.DB), jobID)
	issue := inspectIssue{
		ErrorType:  MissingSecondaryIndexEntry,
		DatabaseID: 1,
		SchemaID:   2,
		ObjectID:   3,
		PrimaryKey: "key",
		Details: map[redact.RedactableString]interface{}{
			"foo": "bar",
			"biz": 15,
		},
	}

	require.NoError(t, tableLogger.logIssue(context.Background(), &issue))

	// Query the system.inspect_errors table and expect one entry
	var count int
	runner.QueryRow(t, `SELECT count(*) FROM system.inspect_errors WHERE job_id = $1`, jobID).Scan(&count)
	require.Equal(t, 1, count, "Expected exactly one entry in system.inspect_errors")

	// Compare the entry against the test instance
	var actualJobID int64
	var actualErrorType, actualPrimaryKey, actualDetails string
	var actualDatabaseID, actualSchemaID, actualObjectID int64

	runner.QueryRow(t, `SELECT job_id, error_type, database_id, schema_id, id, primary_key, details 
		FROM system.inspect_errors WHERE job_id = $1`, jobID).Scan(
		&actualJobID, &actualErrorType, &actualDatabaseID, &actualSchemaID,
		&actualObjectID, &actualPrimaryKey, &actualDetails)

	require.Equal(t, int64(jobID), actualJobID, "job_id should match")
	require.Equal(t, string(issue.ErrorType), actualErrorType, "error_type should match")
	require.Equal(t, int64(issue.DatabaseID), actualDatabaseID, "database_id should match")
	require.Equal(t, int64(issue.SchemaID), actualSchemaID, "schema_id should match")
	require.Equal(t, int64(issue.ObjectID), actualObjectID, "id should match")
	require.Equal(t, issue.PrimaryKey, actualPrimaryKey, "primary_key should match")

	// Verify the details JSON contains the expected values
	var details map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(actualDetails), &details))
	require.Equal(t, issue.Details["foo"], details["foo"])
	require.Equal(t, float64(15), details["biz"])
}
