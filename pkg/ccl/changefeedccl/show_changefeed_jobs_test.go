// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type fakeResumer struct {
	done chan struct{}
}

var _ jobs.Resumer = (*fakeResumer)(nil)

func (d *fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return nil
	}
}

func (d *fakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	return nil
}

func waitForJobStatus(
	runner *sqlutils.SQLRunner, t *testing.T, id jobspb.JobID, targetStatus string,
) {
	testutils.SucceedsSoon(t, func() error {
		var jobStatus string
		query := `SELECT status FROM [SHOW CHANGEFEED JOB $1]`
		runner.QueryRow(t, query, id).Scan(&jobStatus)
		if targetStatus != jobStatus {
			return errors.Errorf("Expected status:%s but found status:%s", targetStatus, jobStatus)
		}
		return nil
	})
}

func TestShowChangefeedJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	registry := s.JobRegistry().(*jobs.Registry)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id             jobspb.JobID
		SinkURI        string
		FulLTableNames []uint8
		format         string
		DescriptorIDs  []descpb.ID
	}

	query := `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	query = `CREATE TABLE bar (b string)`
	sqlDB.Exec(t, query)

	var fooDescriptorID, barDescriptorID int

	query = `SELECT table_id from crdb_internal.tables WHERE name = 'foo'`
	sqlDB.QueryRow(t, query).Scan(&fooDescriptorID)

	query = `SELECT table_id from crdb_internal.tables WHERE name = 'bar'`
	sqlDB.QueryRow(t, query).Scan(&barDescriptorID)

	doneCh := make(chan struct{})
	defer close(doneCh)

	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		jobspb.TypeChangefeed: func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{
				done: doneCh,
			}
			return &r
		},
	}

	query = `SET CLUSTER SETTING kv.rangefeed.enabled = true`
	sqlDB.Exec(t, query)

	var changefeedID jobspb.JobID

	// Cannot use kafka for tests right now because of leaked goroutine issue
	query = `CREATE CHANGEFEED FOR TABLE foo, bar INTO 
		'experimental-s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456'`
	sqlDB.QueryRow(t, query).Scan(&changefeedID)

	var out row

	query = `SELECT job_id, sink_uri, full_table_names, format FROM [SHOW CHANGEFEED JOB $1]`
	sqlDB.QueryRow(t, query, changefeedID).Scan(&out.id, &out.SinkURI, &out.FulLTableNames, &out.format)

	require.Equal(t, changefeedID, out.id, "Expected id:%d but found id:%d", changefeedID, out.id)
	require.Equal(t, "experimental-s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=redacted", out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", "experimental-s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=redacted", out.SinkURI)
	require.Equal(t, "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FulLTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FulLTableNames))
	require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)

	query = `SELECT job_id, sink_uri, full_table_names, format FROM [SHOW CHANGEFEED JOBS] ORDER BY job_id`
	rowResults := sqlDB.Query(t, query)

	if !rowResults.Next() {
		err := rowResults.Err()
		if err != nil {
			t.Fatalf("Error encountered while querying the next row: %v", err)
		} else {
			t.Fatalf("Expected more rows when querying and none found for query: %s", query)
		}
	}
	err := rowResults.Scan(&out.id, &out.SinkURI, &out.FulLTableNames, &out.format)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, changefeedID, out.id, "Expected id:%d but found id:%d", changefeedID, out.id)
	require.Equal(t, "experimental-s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=redacted", out.SinkURI, "Expected sinkUri:%s but found sinkUri:%s", "experimental-s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=redacted", out.SinkURI)
	require.Equal(t, "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FulLTableNames), "Expected fullTableNames:%s but found fullTableNames:%s", "{defaultdb.public.foo,defaultdb.public.bar}", string(out.FulLTableNames))
	require.Equal(t, "json", out.format, "Expected format:%s but found format:%s", "json", out.format)

	hasNext := rowResults.Next()
	require.Equal(t, false, hasNext, "Expected no more rows for query:%s", query)

	err = rowResults.Err()
	require.Equal(t, nil, err, "Expected no error for query:%s but got error %v", query, err)
}

func TestShowChangefeedJobsStatusChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.TestingSetAdoptAndCancelIntervals(10*time.Millisecond, 10*time.Millisecond)()

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	registry := s.JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	query := `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	doneCh := make(chan struct{})
	defer close(doneCh)

	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		jobspb.TypeChangefeed: func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{
				done: doneCh,
			}
			return &r
		},
	}

	query = `SET CLUSTER SETTING kv.rangefeed.enabled = true`
	sqlDB.Exec(t, query)

	var changefeedID jobspb.JobID

	query = `CREATE CHANGEFEED FOR TABLE foo INTO 
		'experimental-s3://fake-bucket-name/fake/path?AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456'`
	sqlDB.QueryRow(t, query).Scan(&changefeedID)

	waitForJobStatus(sqlDB, t, changefeedID, "running")

	query = `PAUSE JOB $1`
	sqlDB.Exec(t, query, changefeedID)

	waitForJobStatus(sqlDB, t, changefeedID, "paused")

	query = `RESUME JOB $1`
	sqlDB.Exec(t, query, changefeedID)

	waitForJobStatus(sqlDB, t, changefeedID, "running")
}

func TestShowChangefeedJobsNoResults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.Background())

	query := `SELECT job_id, sink_uri, full_table_names, format FROM [SHOW CHANGEFEED JOB 999999999999]`
	rowResults := sqlDB.Query(t, query)

	if rowResults.Next() || rowResults.Err() != nil {
		t.Fatalf("Expected no results for query:%s", query)
	}

	query = `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	query = `ALTER TABLE foo ADD COLUMN b BOOL DEFAULT true`
	sqlDB.Exec(t, query)

	var jobID int

	query = `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%ALTER TABLE defaultdb.public.foo%'`
	rowResults = sqlDB.Query(t, query)

	if !rowResults.Next() {
		err := rowResults.Err()
		if err != nil {
			t.Fatalf("Error encountered while querying the next row: %v", err)
		} else {
			t.Fatalf("Expected more rows when querying and none found for query: %s", query)
		}
	}

	err := rowResults.Scan(&jobID)
	if err != nil {
		t.Fatal(err)
	}

	query = `SELECT job_id, sink_uri, full_table_names, format FROM [SHOW CHANGEFEED JOB $1]`
	rowResults = sqlDB.Query(t, query, jobID)

	if rowResults.Next() || rowResults.Err() != nil {
		t.Fatalf("Expected no results for query:%s", query)
	}
}
