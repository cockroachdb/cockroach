// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schematelemetry_test

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func makeTestServerArgs() (args base.TestServerArgs) {
	args.Knobs.JobsTestingKnobs = &jobs.TestingKnobs{
		JobSchedulerEnv: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables,
			timeutil.Now(),
			tree.ScheduledSchemaTelemetryExecutor,
		),
	}
	aostDuration := time.Nanosecond
	args.Knobs.SchemaTelemetry = &sql.SchemaTelemetryTestingKnobs{
		AOSTDuration: &aostDuration,
	}
	return args
}

var (
	qExists = fmt.Sprintf(`
    SELECT recurrence, count(*)
      FROM [SHOW SCHEDULES]
      WHERE label = '%s'
        AND schedule_status = 'ACTIVE'
      GROUP BY recurrence`,
		schematelemetrycontroller.SchemaTelemetryScheduleName)

	qID = fmt.Sprintf(`
    SELECT id
      FROM [SHOW SCHEDULES]
      WHERE label = '%s'
        AND schedule_status = 'ACTIVE'`,
		schematelemetrycontroller.SchemaTelemetryScheduleName)

	qSet = fmt.Sprintf(`SET CLUSTER SETTING %s = '* * * * *'`,
		schematelemetrycontroller.SchemaTelemetryRecurrence.Name())

	qJob = fmt.Sprintf(`SELECT %s()`,
		builtinconstants.CreateSchemaTelemetryJobBuiltinName)
)

const qHasJob = `SELECT count(*) FROM crdb_internal.jobs WHERE job_type = 'AUTO SCHEMA TELEMETRY' AND status = 'succeeded'`

func TestSchemaTelemetrySchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := makeTestServerArgs()
	// 'sql.schema.telemetry.recurrence' setting is settable only by the
	// operator.
	args.DefaultTestTenant = base.TestIsSpecificToStorageLayerAndNeedsASystemTenant
	srv, db, _ := serverutils.StartServer(t, args)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)

	clusterID := s.ExecutorConfig().(sql.ExecutorConfig).NodeInfo.LogicalClusterID()
	exp := scheduledjobs.MaybeRewriteCronExpr(clusterID, "@weekly")
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{exp, "1"}})
	tdb.ExecSucceedsSoon(t, qSet)
	tdb.CheckQueryResultsRetry(t, qExists, [][]string{{"* * * * *", "1"}})
}

func TestSchemaTelemetryJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, makeTestServerArgs())
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, `SET CLUSTER SETTING server.eventlog.enabled = true`)

	// Pause the existing schema telemetry schedule so that it doesn't interfere.
	res := tdb.QueryStr(t, qID)
	require.NotEmpty(t, res)
	require.NotEmpty(t, res[0])
	id := res[0][0]
	tdb.ExecSucceedsSoon(t, fmt.Sprintf("PAUSE SCHEDULE %s", id))
	tdb.CheckQueryResults(t, qHasJob, [][]string{{"0"}})

	// NB: The following block is copied directly from
	// pkg/sql/crdb_internal_test.go. It may be worthwhile to add utilities for
	// generating descriptor corruption in the future rather than copying the same codeblock around.

	// Create some tables that we can corrupt the descriptors of.
	tdb.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT8);
CREATE TABLE fktbl (id INT8 PRIMARY KEY);
CREATE TABLE tbl (
	customer INT8 NOT NULL REFERENCES fktbl (id)
);
CREATE TABLE nojob (k INT8);
	`)

	// Retrieve their IDs.
	databaseID := int(sqlutils.QueryDatabaseID(t, db, "t"))
	tableTID := int(sqlutils.QueryTableID(t, db, "t", "public", "test"))
	tableFkTblID := int(sqlutils.QueryTableID(t, db, "defaultdb", "public", "fktbl"))
	tableNoJobID := int(sqlutils.QueryTableID(t, db, "defaultdb", "public", "nojob"))
	const fakeID = 12345

	// Now introduce some inconsistencies.
	tdb.Exec(t, fmt.Sprintf(`
INSERT INTO system.users VALUES ('node', NULL, true, 3);
GRANT node TO root;
DELETE FROM system.descriptor WHERE id = %d;
DELETE FROM system.descriptor WHERE id = %d;
SELECT
	crdb_internal.unsafe_upsert_descriptor(
		id,
		crdb_internal.json_to_pb(
			'cockroach.sql.sqlbase.Descriptor',
			json_set(
				json_set(
					crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor, false),
					ARRAY['table', 'mutationJobs'],
					jsonb_build_array(jsonb_build_object('job_id', 123456, 'mutation_id', 1))
				),
				ARRAY['table', 'mutations'],
				jsonb_build_array(jsonb_build_object('mutation_id', 1))
			)
		),
		true
	)
FROM
	system.descriptor
WHERE
	id = %d;
UPDATE system.namespace SET id = %d WHERE id = %d;
	`, databaseID, tableFkTblID, tableNoJobID, fakeID, tableTID))

	// Grab a handle to the job's metrics struct.
	metrics := s.JobRegistry().(*jobs.Registry).MetricsStruct().JobSpecificMetrics[jobspb.TypeAutoSchemaTelemetry].(schematelemetry.Metrics)

	// Run a schema telemetry job and wait for it to succeed.
	tdb.Exec(t, qJob)
	tdb.CheckQueryResultsRetry(t, qHasJob, [][]string{{"1"}})

	// Assert that the InvalidObjects gauge is set to the number of expected
	// invalid object. Our above query should have generated 9 corruptions. See
	// the pkg/sql/crdb_internal_test.go:TestInvalidObjects for the breakdown of
	// what exactly was done.
	require.Equal(t, int64(9), metrics.InvalidObjects.Value())

	// Ensure that our logs are flushed to disk before asserting about log
	// entries.
	log.FlushFiles()

	// Ensure that a log line is emitted for each invalid object, with a loose
	// enforcement of the log structure.
	errorRE := regexp.MustCompile(`found invalid object with ID \d+: .+`)
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1000, errorRE, log.SelectEditMode(false /* redact */, false /* keepRedactable */))
	require.NoError(t, err)
	require.Len(t, entries, 9)

	// Verify that the log entries have redaction markers applied by checking one
	// of the specific error messages.
	errorRE = regexp.MustCompile(`found invalid object with ID \d+: relation ‹"nojob"›`)
	entries, err = log.FetchEntriesFromFiles(0, math.MaxInt64, 1000, errorRE, log.SelectEditMode(false /* redact */, true /* keepRedactable */))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
