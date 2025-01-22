// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// getSystemJobIDsForNonAutoJobs queries the jobs table for all job IDs that have
// the given status. Sorted by decreasing creation time.
func getSystemJobIDsForNonAutoJobs(
	t testing.TB, db *sqlutils.SQLRunner, status jobs.State,
) []int64 {
	q := safesql.NewQuery()
	q.Append(`SELECT job_id FROM crdb_internal.jobs WHERE status=$`, status)
	q.Append(` AND (`)
	for i, jobType := range jobspb.AutomaticJobTypes {
		q.Append(`job_type != $`, jobType.String())
		if i < len(jobspb.AutomaticJobTypes)-1 {
			q.Append(" AND ")
		}
	}
	q.Append(` OR job_type IS NULL)`)
	q.Append(` ORDER BY created DESC`)
	rows := db.Query(
		t,
		q.String(),
		q.QueryArguments()...,
	)
	defer rows.Close()

	res := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		res = append(res, id)
	}
	return res
}

func TestAdminAPIJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	now := timeutil.Now()
	retentionTime := 336 * time.Hour
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: &jobs.TestingKnobs{
				IntervalOverrides: jobs.TestingIntervalOverrides{
					RetentionTime: &retentionTime,
				},
			},
			Server: &server.TestingKnobs{
				StubTimeNow: func() time.Time { return now },
			},
		},
	})

	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		// Creating this client causes a user to be created, which causes jobs
		// to be created, so we do it up-front rather than inside the test.
		_, err := s.GetAuthenticatedHTTPClient(isAdmin, serverutils.SingleTenantSession)
		if err != nil {
			t.Fatal(err)
		}
	})

	existingSucceededIDs := getSystemJobIDsForNonAutoJobs(t, sqlDB, jobs.StateSucceeded)
	existingRunningIDs := getSystemJobIDsForNonAutoJobs(t, sqlDB, jobs.StateRunning)
	existingIDs := append(existingSucceededIDs, existingRunningIDs...)

	runningOnlyIds := []int64{1, 2, 4, 11, 12}
	revertingOnlyIds := []int64{7, 8, 9}
	retryRunningIds := []int64{6}
	retryRevertingIds := []int64{10}
	ef := &jobspb.RetriableExecutionFailure{
		TruncatedError: "foo",
	}
	// Add a regression test for #84139 where a string with a quote in it
	// caused a failure in the admin API.
	efQuote := &jobspb.RetriableExecutionFailure{
		TruncatedError: "foo\"abc\"",
	}

	testJobs := []struct {
		id                int64
		status            jobs.State
		details           jobspb.Details
		progress          jobspb.ProgressDetails
		username          username.SQLUsername
		numRuns           int64
		lastRun           time.Time
		executionFailures []*jobspb.RetriableExecutionFailure
	}{
		{1, jobs.StateRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{2, jobs.StateRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, timeutil.Now().Add(10 * time.Minute), nil},
		{3, jobs.StateSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{4, jobs.StateRunning, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{5, jobs.StateSucceeded, jobspb.BackupDetails{}, jobspb.BackupProgress{}, apiconstants.TestingUserNameNoAdmin(), 1, time.Time{}, nil},
		{6, jobs.StateRunning, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 2, timeutil.Now().Add(10 * time.Minute), nil},
		{7, jobs.StateReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{8, jobs.StateReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 1, timeutil.Now().Add(10 * time.Minute), nil},
		{9, jobs.StateReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{10, jobs.StateReverting, jobspb.ImportDetails{}, jobspb.ImportProgress{}, username.RootUserName(), 2, timeutil.Now().Add(10 * time.Minute), nil},
		{11, jobs.StateRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, []*jobspb.RetriableExecutionFailure{ef}},
		{12, jobs.StateRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, []*jobspb.RetriableExecutionFailure{efQuote}},
	}
	for _, job := range testJobs {
		payload := jobspb.Payload{
			UsernameProto: job.username.EncodeProto(),
			Details:       jobspb.WrapPayloadDetails(job.details),
		}
		payloadBytes, err := protoutil.Marshal(&payload)
		if err != nil {
			t.Fatal(err)
		}

		progress := jobspb.Progress{Details: jobspb.WrapProgressDetails(job.progress)}
		// Populate progress.Progress field with a specific progress type based on
		// the job type.
		if _, ok := job.progress.(jobspb.ChangefeedProgress); ok {
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &hlc.Timestamp{},
			}
		} else {
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 1.0,
			}
		}

		progressBytes, err := protoutil.Marshal(&progress)
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.Exec(t,
			`INSERT INTO system.jobs (id, status, num_runs, last_run, job_type, owner) VALUES ($1, $2, $3, $4, $5, $6)`,
			job.id, job.status, job.numRuns, job.lastRun, payload.Type().String(), payload.UsernameProto.Decode().Normalized(),
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyPayloadKey(), payloadBytes,
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyProgressKey(), progressBytes,
		)
	}

	const invalidJobType = math.MaxInt32

	testCases := []struct {
		uri                    string
		expectedIDsViaAdmin    []int64
		expectedIDsViaNonAdmin []int64
	}{
		{
			"jobs",
			append([]int64{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, existingIDs...),
			[]int64{5},
		},
		{
			"jobs?limit=1",
			[]int64{12},
			[]int64{5},
		},
		{
			"jobs?status=succeeded",
			append([]int64{5, 3}, existingSucceededIDs...),
			[]int64{5},
		},
		{
			"jobs?status=running",
			append(append(append([]int64{}, runningOnlyIds...), retryRunningIds...), existingRunningIDs...),
			[]int64{},
		},
		{
			"jobs?status=reverting",
			append(append([]int64{}, revertingOnlyIds...), retryRevertingIds...),
			[]int64{},
		},
		{
			"jobs?status=pending",
			[]int64{},
			[]int64{},
		},
		{
			"jobs?status=garbage",
			[]int64{},
			[]int64{},
		},
		{
			fmt.Sprintf("jobs?type=%d", jobspb.TypeBackup),
			[]int64{5, 3, 2},
			[]int64{5},
		},
		{
			fmt.Sprintf("jobs?type=%d", jobspb.TypeRestore),
			[]int64{1, 11, 12},
			[]int64{},
		},
		{
			fmt.Sprintf("jobs?type=%d", invalidJobType),
			[]int64{},
			[]int64{},
		},
		{
			fmt.Sprintf("jobs?status=running&type=%d", jobspb.TypeBackup),
			[]int64{2},
			[]int64{},
		},
	}

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		for i, testCase := range testCases {
			var res serverpb.JobsResponse
			if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, testCase.uri, &res, isAdmin); err != nil {
				t.Fatal(err)
			}
			resIDs := []int64{}
			for _, job := range res.Jobs {
				resIDs = append(resIDs, job.ID)
			}

			expected := testCase.expectedIDsViaAdmin
			if !isAdmin {
				expected = testCase.expectedIDsViaNonAdmin
			}

			sort.Slice(expected, func(i, j int) bool {
				return expected[i] < expected[j]
			})

			sort.Slice(resIDs, func(i, j int) bool {
				return resIDs[i] < resIDs[j]
			})
			if e, a := expected, resIDs; !reflect.DeepEqual(e, a) {
				t.Errorf("%d - %v: expected job IDs %v, but got %v", i, testCase.uri, e, a)
			}
			// We don't use require.Equal() because timestamps don't necessarily
			// compare == due to only one of them having a monotonic clock reading.
			require.True(t, now.Add(-retentionTime).Equal(res.EarliestRetainedTime))
		}
	})
}

func TestAdminAPIJobsDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	now := timeutil.Now()

	encodedError := func(err error) *errors.EncodedError {
		ee := errors.EncodeError(context.Background(), err)
		return &ee
	}
	testJobs := []struct {
		id           int64
		status       jobs.State
		details      jobspb.Details
		progress     jobspb.ProgressDetails
		username     username.SQLUsername
		numRuns      int64
		lastRun      time.Time
		executionLog []*jobspb.RetriableExecutionFailure
	}{
		{1, jobs.StateRunning, jobspb.RestoreDetails{}, jobspb.RestoreProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{2, jobs.StateReverting, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, time.Time{}, nil},
		{3, jobs.StateRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 1, now.Add(10 * time.Minute), nil},
		{4, jobs.StateReverting, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 1, now.Add(10 * time.Minute), nil},
		{5, jobs.StateRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{6, jobs.StateReverting, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 2, time.Time{}, nil},
		{7, jobs.StateRunning, jobspb.BackupDetails{}, jobspb.BackupProgress{}, username.RootUserName(), 2, now.Add(10 * time.Minute), nil},
		{8, jobs.StateReverting, jobspb.ChangefeedDetails{}, jobspb.ChangefeedProgress{}, username.RootUserName(), 2, now.Add(10 * time.Minute), []*jobspb.RetriableExecutionFailure{
			{
				Status:               string(jobs.StateRunning),
				ExecutionStartMicros: now.Add(-time.Minute).UnixMicro(),
				ExecutionEndMicros:   now.Add(-30 * time.Second).UnixMicro(),
				InstanceID:           1,
				Error:                encodedError(errors.New("foo")),
			},
			{
				Status:               string(jobs.StateReverting),
				ExecutionStartMicros: now.Add(-29 * time.Minute).UnixMicro(),
				ExecutionEndMicros:   now.Add(-time.Second).UnixMicro(),
				InstanceID:           1,
				TruncatedError:       "bar",
			},
		}},
	}
	for _, job := range testJobs {
		payload := jobspb.Payload{
			UsernameProto: job.username.EncodeProto(),
			Details:       jobspb.WrapPayloadDetails(job.details),
		}
		payloadBytes, err := protoutil.Marshal(&payload)
		if err != nil {
			t.Fatal(err)
		}

		progress := jobspb.Progress{Details: jobspb.WrapProgressDetails(job.progress)}
		// Populate progress.Progress field with a specific progress type based on
		// the job type.
		if _, ok := job.progress.(jobspb.ChangefeedProgress); ok {
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &hlc.Timestamp{},
			}
		} else {
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 1.0,
			}
		}

		progressBytes, err := protoutil.Marshal(&progress)
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.Exec(t,
			`INSERT INTO system.jobs (id, status, num_runs, last_run, job_type) VALUES ($1, $2, $3, $4, $5)`,
			job.id, job.status, job.numRuns, job.lastRun, payload.Type().String(),
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyPayloadKey(), payloadBytes,
		)
		sqlDB.Exec(t,
			`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			job.id, jobs.GetLegacyProgressKey(), progressBytes,
		)
	}

	var res serverpb.JobsResponse
	if err := srvtestutils.GetAdminJSONProto(s, "jobs", &res); err != nil {
		t.Fatal(err)
	}

	// Trim down our result set to the jobs we injected.
	resJobs := append([]serverpb.JobResponse(nil), res.Jobs...)
	sort.Slice(resJobs, func(i, j int) bool {
		return resJobs[i].ID < resJobs[j].ID
	})
	resJobs = resJobs[:len(testJobs)]

	for i, job := range resJobs {
		require.Equal(t, testJobs[i].id, job.ID)
	}
}

func TestJobStatusResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(context.Background())

	client := ts.GetStatusClient(t)

	request := &serverpb.JobStatusRequest{JobId: -1}
	response, err := client.JobStatus(context.Background(), request)
	require.Regexp(t, `job with ID -1 does not exist`, err)
	require.Nil(t, response)

	ctx := context.Background()
	jr := ts.JobRegistry().(*jobs.Registry)
	job, err := jr.CreateJobWithTxn(
		ctx,
		jobs.Record{
			Description: "testing",
			Statements:  []string{"SELECT 1"},
			Username:    username.RootUserName(),
			Details: jobspb.ImportDetails{
				Tables: []jobspb.ImportDetails_Table{
					{
						Desc: &descpb.TableDescriptor{
							ID: 1,
						},
					},
					{
						Desc: &descpb.TableDescriptor{
							ID: 2,
						},
					},
				},
				URIs: []string{"a", "b"},
			},
			Progress:      jobspb.ImportProgress{},
			DescriptorIDs: []descpb.ID{1, 2, 3},
		},
		jr.MakeJobID(),
		nil)
	if err != nil {
		t.Fatal(err)
	}
	request.JobId = int64(job.ID())
	response, err = client.JobStatus(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, job.ID(), response.Job.Id)
	require.Equal(t, job.Payload(), *response.Job.Payload)
	require.Equal(t, job.Progress(), *response.Job.Progress)
}
