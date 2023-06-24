// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestReadWriteProfilerBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timeout the test in a few minutes if it hasn't succeeded.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	defer jobs.ResetConstructors()()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	runner := sqlutils.MakeSQLRunner(sqlDB)

	jobs.RegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeExecResumer{
			OnResume: func(ctx context.Context) error {
				p := sql.PhysicalPlan{}
				infra := physicalplan.NewPhysicalInfrastructure(uuid.FastMakeV4(), base.SQLInstanceID(1))
				p.PhysicalInfrastructure = infra
				jobsprofiler.StorePlanDiagram(ctx, s.Stopper(), &p, s.InternalDB().(isql.DB), j.ID())
				checkForPlanDiagram(ctx, t, s.InternalDB().(isql.DB), j.ID())
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)

	runner.Exec(t, `CREATE TABLE t (id INT)`)
	runner.Exec(t, `INSERT INTO t SELECT generate_series(1, 100)`)

	t.Run("one chunk", func(t *testing.T) {
		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))

		runner.Exec(t, `SELECT crdb_internal.request_job_profiler_bundle($1)`, importJobID)
		md := profilerpb.ProfilerBundleMetadata{}
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			jobInfo := jobs.InfoStorageForJob(txn, jobspb.JobID(importJobID))
			var res []byte
			err := jobInfo.GetLast(ctx, profilerconstants.ProfilerBundleMetadataKeyPrefix, func(infoKey string, value []byte) error {
				res = value
				return nil
			})
			if err != nil {
				return err
			}
			require.NotEmpty(t, res)
			if err := protoutil.Unmarshal(res, &md); err != nil {
				return err
			}
			require.Equal(t, 1, int(md.NumChunks))
			return nil
		})
		fmt.Printf("bundle ID one %s\n", md.BundleID.String())
		require.NoError(t, err)
		checkBundle(t, s, jobspb.JobID(importJobID), md.BundleID.String(), "distsql.html")
	})

	t.Run("multiple chunks", func(t *testing.T) {
		var importJobID int
		runner.QueryRow(t, `IMPORT INTO t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
		jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(importJobID))

		runner.Exec(t, `SET CLUSTER SETTING jobs.profiler.bundle_chunk_size = '17b'`)
		defer func() {
			runner.Exec(t, `RESET CLUSTER SETTING jobs.profiler.bundle_chunk_size`)
		}()
		runner.Exec(t, `SELECT crdb_internal.request_job_profiler_bundle($1)`, importJobID)
		md := profilerpb.ProfilerBundleMetadata{}
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			jobInfo := jobs.InfoStorageForJob(txn, jobspb.JobID(importJobID))
			var res []byte
			err := jobInfo.GetLast(ctx, profilerconstants.ProfilerBundleMetadataKeyPrefix, func(infoKey string, value []byte) error {
				res = value
				return nil
			})
			if err != nil {
				return err
			}
			require.NotEmpty(t, res)
			if err := protoutil.Unmarshal(res, &md); err != nil {
				return err
			}
			require.Greater(t, md.NumChunks, int32(1))
			return nil
		})
		require.NoError(t, err)
		checkBundle(t, s, jobspb.JobID(importJobID), md.BundleID.String(), "distsql.html")
	})

	t.Run("bundle for non-existent job", func(t *testing.T) {
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			jobInfo := jobs.InfoStorageForJob(txn, jobspb.JobID(1))
			var res []byte
			err := jobInfo.GetLast(ctx, profilerconstants.ProfilerBundleMetadataKeyPrefix, func(infoKey string, value []byte) error {
				res = value
				return nil
			})
			if err != nil {
				return err
			}
			require.Empty(t, res)
			return nil
		})
		require.NoError(t, err)
	})
}

func checkBundle(
	t *testing.T,
	s serverutils.TestServerInterface,
	jobID jobspb.JobID,
	bundleID string,
	expectedFiles ...string,
) {
	t.Helper()

	client, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	url := s.AdminURL() + fmt.Sprintf("/_status/job_profiler_bundle/%d/%s", jobID, bundleID)
	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)

	// Retrieve the session list for the system tenant.
	req.Header.Set("Content-Type", httputil.ProtoContentType)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	bundleResp := serverpb.GetJobProfilerBundleResponse{}
	require.NoError(t, protoutil.Unmarshal(body, &bundleResp))

	unzip, err := zip.NewReader(bytes.NewReader(bundleResp.Bundle), int64(len(bundleResp.Bundle)))
	require.NoError(t, err)

	// Make sure the bundle contains the expected list of files.
	var files []string
	for _, f := range unzip.File {
		t.Logf("found file: %s", f.Name)
		if f.UncompressedSize64 == 0 {
			t.Fatalf("file %s is empty", f.Name)
		}
		files = append(files, f.Name)

		r, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
	}

	var expList []string
	for _, s := range expectedFiles {
		expList = append(expList, strings.Split(s, " ")...)
	}
	sort.Strings(files)
	sort.Strings(expList)
	if fmt.Sprint(files) != fmt.Sprint(expList) {
		t.Errorf("unexpected list of files:\n  %v\nexpected:\n  %v", files, expList)
	}
}
