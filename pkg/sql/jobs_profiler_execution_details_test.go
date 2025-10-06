// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/klauspost/compress/zip"
	"github.com/stretchr/testify/require"
)

// fakeExecResumer calls optional callbacks during the job lifecycle.
type fakeExecResumer struct {
	OnResume     func(context.Context) error
	FailOrCancel func(context.Context) error
}

func (d fakeExecResumer) ForceRealSpan() bool {
	return true
}

func (d fakeExecResumer) DumpTraceAfterRun() bool {
	return true
}

var _ jobs.Resumer = fakeExecResumer{}
var _ jobs.TraceableJob = fakeExecResumer{}

func (d fakeExecResumer) Resume(ctx context.Context, execCtx interface{}) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d fakeExecResumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

func (d fakeExecResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// checkForPlanDiagram is a method used in tests to wait for the existence of a
// DSP diagram for the provided jobID.
func checkForPlanDiagrams(
	ctx context.Context, t *testing.T, db isql.DB, jobID jobspb.JobID, expectedNumDiagrams int,
) {
	testutils.SucceedsSoon(t, func() error {
		return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			var found int
			err := infoStorage.Iterate(ctx, profilerconstants.DSPDiagramInfoKeyPrefix,
				func(infoKey string, value []byte) error {
					found++
					return nil
				})
			if err != nil {
				return err
			}
			if found != expectedNumDiagrams {
				return errors.Newf("found %d diagrams, expected %d", found, expectedNumDiagrams)
			}
			return nil
		})
	})
}

func TestShowJobsWithExecutionDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timeout the test in a few minutes if it hasn't succeeded.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	params, _ := createTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	defer jobs.ResetConstructors()()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeExecResumer{
			OnResume: func(ctx context.Context) error {
				p := sql.PhysicalPlan{}
				infra := physicalplan.NewPhysicalInfrastructure(uuid.MakeV4(), base.SQLInstanceID(1))
				p.PhysicalInfrastructure = infra
				jobsprofiler.StorePlanDiagram(ctx, s.Stopper(), &p, s.InternalDB().(isql.DB), j.ID())
				checkForPlanDiagrams(ctx, t, s.InternalDB().(isql.DB), j.ID(), 1)
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)()

	runner.Exec(t, `CREATE TABLE t (id INT)`)
	runner.Exec(t, `INSERT INTO t SELECT generate_series(1, 100)`)
	var importJobID int
	runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
	jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))

	var count int
	runner.QueryRow(t, `SELECT count(*) FROM [SHOW JOB $1 WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, importJobID).Scan(&count)
	require.NotZero(t, count)
	runner.CheckQueryResults(t, `SELECT count(*) FROM [SHOW JOBS WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, [][]string{{"1"}})
}

// TestReadWriteProfilerExecutionDetails is an end-to-end test of requesting and collecting
// execution details for a job.
func TestReadWriteProfilerExecutionDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timeout the test in a few minutes if it hasn't succeeded.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	params, _ := createTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	defer jobs.ResetConstructors()()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, `CREATE TABLE t (id INT)`)
	runner.Exec(t, `INSERT INTO t SELECT generate_series(1, 100)`)

	isRunning := make(chan struct{})
	defer close(isRunning)
	continueRunning := make(chan struct{})
	t.Run("read/write DistSQL diagram", func(t *testing.T) {
		defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return fakeExecResumer{
				OnResume: func(ctx context.Context) error {
					p := sql.PhysicalPlan{}
					infra := physicalplan.NewPhysicalInfrastructure(uuid.MakeV4(), base.SQLInstanceID(1))
					p.PhysicalInfrastructure = infra
					jobsprofiler.StorePlanDiagram(ctx, s.Stopper(), &p, s.InternalDB().(isql.DB), j.ID())
					checkForPlanDiagrams(ctx, t, s.InternalDB().(isql.DB), j.ID(), 1)
					isRunning <- struct{}{}
					<-continueRunning
					return nil
				},
			}
		}, jobs.UsesTenantCostControl)()

		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		<-isRunning
		runner.Exec(t, `SELECT crdb_internal.request_job_execution_details($1)`, importJobID)
		distSQLDiagram, err := checkExecutionDetails(t, s, jobspb.JobID(importJobID), "distsql")
		require.NoError(t, err)
		require.Regexp(t, "<meta http-equiv=\"Refresh\" content=\"0\\; url=https://cockroachdb\\.github\\.io/distsqlplan/decode.html.*>", string(distSQLDiagram))
		close(continueRunning)
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))
	})

	t.Run("read/write goroutines", func(t *testing.T) {
		blockCh := make(chan struct{})
		continueCh := make(chan struct{})
		defer close(blockCh)
		defer close(continueCh)
		defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return fakeExecResumer{
				OnResume: func(ctx context.Context) error {
					pprof.Do(ctx, pprof.Labels("foo", "bar"), func(ctx2 context.Context) {
						blockCh <- struct{}{}
						<-continueCh
					})
					return nil
				},
			}
		}, jobs.UsesTenantCostControl)()
		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		<-blockCh
		runner.Exec(t, `SELECT crdb_internal.request_job_execution_details($1)`, importJobID)
		goroutines, err := checkExecutionDetails(t, s, jobspb.JobID(importJobID), "goroutines")
		require.NoError(t, err)
		continueCh <- struct{}{}
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))
		require.True(t, strings.Contains(string(goroutines), fmt.Sprintf("labels: {\"foo\":\"bar\", \"job\":\"IMPORT id=%d\", \"n\":\"1\"}", importJobID)))
		require.True(t, strings.Contains(string(goroutines), "github.com/cockroachdb/cockroach/pkg/sql_test.fakeExecResumer.Resume"))
	})

	t.Run("execution details for invalid job ID", func(t *testing.T) {
		runner.ExpectErr(t, `coordinator not found for job -123`, `SELECT crdb_internal.request_job_execution_details(-123)`)
	})

	t.Run("read/write terminal trace", func(t *testing.T) {
		defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return fakeExecResumer{
				OnResume: func(ctx context.Context) error {
					sp := tracing.SpanFromContext(ctx)
					require.NotNil(t, sp)
					sp.RecordStructured(&types.StringValue{Value: "should see this"})
					return nil
				},
			}
		}, jobs.UsesTenantCostControl)()
		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))
		var trace []byte
		// There may be some delay between the job finishing and the trace being
		// persisted.
		testutils.SucceedsSoon(t, func() error {
			var err error
			trace, err = checkExecutionDetails(t, s, jobspb.JobID(importJobID), "resumer-trace")
			return err
		})
		require.Contains(t, string(trace), "should see this")
	})

	t.Run("read/write active trace", func(t *testing.T) {
		blockCh := make(chan struct{})
		continueCh := make(chan struct{})
		defer close(blockCh)
		defer close(continueCh)
		defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return fakeExecResumer{
				OnResume: func(ctx context.Context) error {
					_, childSp := tracing.ChildSpan(ctx, "child")
					defer childSp.Finish()
					blockCh <- struct{}{}
					<-continueCh
					return nil
				},
			}
		}, jobs.UsesTenantCostControl)()
		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		<-blockCh
		runner.Exec(t, `SELECT crdb_internal.request_job_execution_details($1)`, importJobID)
		activeTraces, err := checkExecutionDetails(t, s, jobspb.JobID(importJobID), "trace")
		require.NoError(t, err)
		continueCh <- struct{}{}
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))
		unzip, err := zip.NewReader(bytes.NewReader(activeTraces), int64(len(activeTraces)))
		require.NoError(t, err)

		// Make sure the bundle contains the expected list of files.
		var files []string
		for _, f := range unzip.File {
			if f.UncompressedSize64 == 0 {
				t.Fatalf("file %s is empty", f.Name)
			}
			files = append(files, f.Name)

			r, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			contents := string(bytes)

			// Verify some contents in the active traces.
			if strings.Contains(f.Name, ".txt") {
				require.Regexp(t, "[child: {count: 1, duration.*, unfinished}]", contents)
			} else if strings.Contains(f.Name, ".json") {
				require.True(t, strings.Contains(contents, "\"operationName\": \"child\""))
			}
		}
		require.Equal(t, []string{"node1-trace.txt", "node1-jaeger.json"}, files)
	})
}

func TestListProfilerExecutionDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timeout the test in a few minutes if it hasn't succeeded.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	params, _ := createTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	defer jobs.ResetConstructors()()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	expectedDiagrams := 1
	writtenDiagram := make(chan struct{})
	defer close(writtenDiagram)
	continueCh := make(chan struct{})
	defer close(continueCh)
	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeExecResumer{
			OnResume: func(ctx context.Context) error {
				p := sql.PhysicalPlan{}
				infra := physicalplan.NewPhysicalInfrastructure(uuid.MakeV4(), base.SQLInstanceID(1))
				p.PhysicalInfrastructure = infra
				jobsprofiler.StorePlanDiagram(ctx, s.Stopper(), &p, s.InternalDB().(isql.DB), j.ID())
				checkForPlanDiagrams(ctx, t, s.InternalDB().(isql.DB), j.ID(), expectedDiagrams)
				writtenDiagram <- struct{}{}
				<-continueCh
				if err := execCfg.JobRegistry.CheckPausepoint("fakeresumer.pause"); err != nil {
					return err
				}
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)()

	runner.Exec(t, `CREATE TABLE t (id INT)`)
	runner.Exec(t, `INSERT INTO t SELECT generate_series(1, 100)`)

	t.Run("list execution detail files", func(t *testing.T) {
		runner.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = 'fakeresumer.pause'`)
		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		<-writtenDiagram

		runner.Exec(t, `SELECT crdb_internal.request_job_execution_details($1)`, importJobID)
		files := listExecutionDetails(t, s, jobspb.JobID(importJobID))
		require.Len(t, files, 3)
		require.Regexp(t, "distsql\\..*\\.html", files[0])
		require.Regexp(t, "goroutines\\..*\\.txt", files[1])
		require.Regexp(t, "trace\\..*\\.zip", files[2])

		continueCh <- struct{}{}
		jobutils.WaitForJobToPause(t, runner, jobspb.JobID(importJobID))

		testutils.SucceedsSoon(t, func() error {
			files = listExecutionDetails(t, s, jobspb.JobID(importJobID))
			if len(files) != 5 {
				return errors.Newf("expected 5 files, got %d: %v", len(files), files)
			}
			return nil
		})

		// Resume the job, so it can write another DistSQL diagram and goroutine
		// snapshot.
		runner.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = ''`)
		expectedDiagrams = 2
		runner.Exec(t, `RESUME JOB $1`, importJobID)
		<-writtenDiagram
		runner.Exec(t, `SELECT crdb_internal.request_job_execution_details($1)`, importJobID)
		continueCh <- struct{}{}
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))
		testutils.SucceedsSoon(t, func() error {
			files = listExecutionDetails(t, s, jobspb.JobID(importJobID))
			if len(files) != 10 {
				return errors.Newf("expected 10 files, got %d: %v", len(files), files)
			}
			return nil
		})
		require.Regexp(t, "[0-9]/resumer-trace/.*~cockroach\\.sql\\.jobs\\.jobspb\\.TraceData\\.binpb", files[0])
		require.Regexp(t, "[0-9]/resumer-trace/.*~cockroach\\.sql\\.jobs\\.jobspb\\.TraceData\\.binpb.txt", files[1])
		require.Regexp(t, "[0-9]/resumer-trace/.*~cockroach\\.sql\\.jobs\\.jobspb\\.TraceData\\.binpb", files[2])
		require.Regexp(t, "[0-9]/resumer-trace/.*~cockroach\\.sql\\.jobs\\.jobspb\\.TraceData\\.binpb.txt", files[3])
		require.Regexp(t, "distsql\\..*\\.html", files[4])
		require.Regexp(t, "distsql\\..*\\.html", files[5])
		require.Regexp(t, "goroutines\\..*\\.txt", files[6])
		require.Regexp(t, "goroutines\\..*\\.txt", files[7])
		require.Regexp(t, "trace\\..*\\.zip", files[8])
		require.Regexp(t, "trace\\..*\\.zip", files[9])
	})
}

func listExecutionDetails(
	t *testing.T, s serverutils.TestServerInterface, jobID jobspb.JobID,
) []string {
	t.Helper()

	client, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	url := s.AdminURL().WithPath(fmt.Sprintf("/_status/list_job_profiler_execution_details/%d", jobID)).String()
	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)

	req.Header.Set("Content-Type", httputil.ProtoContentType)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	edResp := serverpb.ListJobProfilerExecutionDetailsResponse{}
	require.NoError(t, protoutil.Unmarshal(body, &edResp))
	sort.Slice(edResp.Files, func(i, j int) bool {
		return edResp.Files[i] < edResp.Files[j]
	})
	return edResp.Files
}

func checkExecutionDetails(
	t *testing.T, s serverutils.TestServerInterface, jobID jobspb.JobID, filename string,
) ([]byte, error) {
	t.Helper()

	client, err := s.GetAdminHTTPClient()
	if err != nil {
		return nil, err
	}

	url := s.AdminURL().WithPath(fmt.Sprintf("/_status/job_profiler_execution_details/%d?%s", jobID, filename)).String()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", httputil.ProtoContentType)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)

	edResp := serverpb.GetJobProfilerExecutionDetailResponse{}
	if err := protoutil.Unmarshal(body, &edResp); err != nil {
		return nil, err
	}

	r := bytes.NewReader(edResp.Data)
	data, err := io.ReadAll(r)
	if err != nil {
		return data, err
	}
	if len(data) == 0 {
		return data, errors.New("no data returned")
	}
	return data, nil
}
