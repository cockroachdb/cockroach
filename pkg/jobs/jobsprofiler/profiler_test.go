// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobsprofiler_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestProfilerStorePlanDiagram(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// First verify that directly calling StorePlanDiagram writes n times causes
	// the expected number of persisted rows in job_info, respecting the limit.
	db := s.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	const fakeJobID = 4567
	plan := &sql.PhysicalPlan{PhysicalPlan: physicalplan.MakePhysicalPlan(&physicalplan.PhysicalInfrastructure{})}
	for i := 1; i < 10; i++ {
		jobsprofiler.StorePlanDiagram(ctx, s.ApplicationLayer().AppStopper(), plan, db, fakeJobID)
		testutils.SucceedsSoon(t, func() error {
			var count int
			if err := sqlDB.QueryRow(
				`SELECT count(*) FROM system.job_info WHERE job_id = $1`, fakeJobID,
			).Scan(&count); err != nil {
				return err
			}
			if expected := min(i, jobsprofiler.MaxRetainedDSPDiagramsPerJob); count != expected {
				return errors.Errorf("expected %d rows, got %d", expected, count)
			}
			return nil
		})
	}

	// Now run various jobs that have been extended to persist diagrams and make
	// sure that they also create persisted diagram rows.
	_, err := sqlDB.Exec(`CREATE DATABASE test`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE TABLE foo (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO foo VALUES (1), (2)`)
	require.NoError(t, err)

	for _, l := range []serverutils.ApplicationLayerInterface{s.ApplicationLayer(), s.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	for _, tc := range []struct {
		name string
		sql  string
		typ  jobspb.Type
	}{
		{
			name: "backup",
			sql:  "BACKUP TABLE foo INTO 'userfile:///foo'",
			typ:  jobspb.TypeBackup,
		},
		{
			name: "restore",
			sql:  "RESTORE TABLE foo FROM LATEST IN 'userfile:///foo' WITH into_db='test'",
			typ:  jobspb.TypeRestore,
		},
		{
			name: "changefeed",
			sql:  "CREATE CHANGEFEED FOR foo INTO 'null://sink'",
			typ:  jobspb.TypeChangefeed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlDB.Exec(tc.sql)
			require.NoError(t, err)

			var jobID jobspb.JobID
			err = sqlDB.QueryRow(
				`SELECT id FROM crdb_internal.system_jobs WHERE job_type = $1`, tc.typ.String()).Scan(&jobID)
			require.NoError(t, err)

			execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
			testutils.SucceedsSoon(t, func() error {
				var count int
				err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					infoStorage := jobs.InfoStorageForJob(txn, jobID)
					return infoStorage.Iterate(ctx, profilerconstants.DSPDiagramInfoKeyPrefix,
						func(infoKey string, value []byte) error {
							count++
							return nil
						})
				})
				require.NoError(t, err)
				if count != 1 {
					return errors.Newf("expected a row for the DistSQL diagram to be written but found %d", count)
				}
				return nil
			})
		})
	}
}

func TestStorePerNodeProcessorProgressFraction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	randID := uuid.MakeV4()
	componentID := execinfrapb.ComponentID{
		FlowID: execinfrapb.FlowID{UUID: randID},
		Type:   execinfrapb.ComponentID_PROCESSOR,
	}

	jobID := jobspb.JobID(42)

	// Record progress for two components.
	n1proc1 := componentID
	n1proc1.SQLInstanceID = 1
	n1proc1.ID = 1

	n2proc1 := componentID
	n2proc1.SQLInstanceID = 2
	n2proc1.ID = 1

	jobsprofiler.StorePerNodeProcessorProgressFraction(ctx, s.InternalDB().(isql.DB),
		jobID, map[execinfrapb.ComponentID]float32{n1proc1: 0.95, n2proc1: 0.50})

	// Update n2proc1.
	jobsprofiler.StorePerNodeProcessorProgressFraction(ctx, s.InternalDB().(isql.DB),
		jobID, map[execinfrapb.ComponentID]float32{n2proc1: 0.70})
	// Update n1proc1.
	jobsprofiler.StorePerNodeProcessorProgressFraction(ctx, s.InternalDB().(isql.DB),
		jobID, map[execinfrapb.ComponentID]float32{n1proc1: 1.00})

	var persistedProgress map[string]string
	err := s.ExecutorConfig().(sql.ExecutorConfig).InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		persistedProgress = make(map[string]string)
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		return infoStorage.Iterate(ctx, profilerconstants.NodeProcessorProgressInfoKeyPrefix,
			func(infoKey string, value []byte) error {
				f, err := strconv.ParseFloat(string(value), 32)
				require.NoError(t, err)
				f = math.Ceil(f*100) / 100
				persistedProgress[infoKey] = fmt.Sprintf("%.2f", f)
				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Len(t, persistedProgress, 2)
	for k, v := range persistedProgress {
		if strings.Contains(k, fmt.Sprintf("%s,1,1", randID.String())) {
			require.Equal(t, "1.00", v)
		} else if strings.Contains(k, fmt.Sprintf("%s,2,1", randID.String())) {
			require.Equal(t, "0.70", v)
		} else {
			t.Fatalf("unexpected info key %s:%s", k, v)
		}
	}
}

func TestTraceRecordingOnResumerCompletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE DATABASE test`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE TABLE foo (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO foo VALUES (1), (2)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`SET CLUSTER SETTING jobs.debug.pausepoints = 'backup.before.flow'`)
	require.NoError(t, err)

	var jobID int
	err = sqlDB.QueryRow(`BACKUP INTO 'userfile:///foo' WITH detached`).Scan(&jobID)
	require.NoError(t, err)
	runner := sqlutils.MakeSQLRunner(sqlDB)
	jobutils.WaitForJobToPause(t, runner, jobspb.JobID(jobID))

	_, err = sqlDB.Exec(`SET CLUSTER SETTING jobs.debug.pausepoints = ''`)
	require.NoError(t, err)

	runner.Exec(t, `RESUME JOB $1`, jobID)
	jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(jobID))

	// At this point there should have been two resumers, and so we expect two
	// trace recordings.
	testutils.SucceedsSoon(t, func() error {
		recordings := make([][]byte, 0)
		execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
		edFiles, err := jobs.ListExecutionDetailFiles(ctx, execCfg.InternalDB, jobspb.JobID(jobID))
		if err != nil {
			return err
		}
		var traceFiles []string
		for _, f := range edFiles {
			if strings.Contains(f, "resumer-trace") {
				traceFiles = append(traceFiles, f)
			}
		}

		return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			for _, f := range traceFiles {
				data, err := jobs.ReadExecutionDetailFile(ctx, f, txn, jobspb.JobID(jobID))
				if err != nil {
					return err
				}
				recordings = append(recordings, data)
				if strings.HasSuffix(f, "binpb") {
					td := jobspb.TraceData{}
					if err := protoutil.Unmarshal(data, &td); err != nil {
						return err
					}
					require.NotEmpty(t, td.CollectedSpans)
				}
			}
			if len(recordings) != 4 {
				return errors.Newf("expected 2 entries but found %d", len(recordings))
			}
			return nil
		})
	})
}
