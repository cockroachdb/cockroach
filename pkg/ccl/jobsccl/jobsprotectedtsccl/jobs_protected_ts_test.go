// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobsprotectedtsccl

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Imported to allow multi-tenant tests
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

type fakeResumer struct{}

func (f fakeResumer) Resume(ctx context.Context, _ interface{}) error {
	<-ctx.Done()
	return ctx.Err()
}

func (f fakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	<-ctx.Done()
	return ctx.Err()
}

func (f fakeResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func testJobsProtectedTimestamp(
	ctx context.Context,
	t *testing.T,
	runner *sqlutils.SQLRunner,
	jr *jobs.Registry,
	execCfg *sql.ExecutorConfig,
	ptp protectedts.Provider,
	clock *hlc.Clock,
) {
	t.Helper()

	insqlDB := execCfg.InternalDB
	mkJobAndRecord := func(f func(context.Context, isql.Txn, *jobs.Job) error) (rec *ptpb.Record) {
		ts := clock.Now()
		jobID := jr.MakeJobID()
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			jobRec := jobs.Record{
				Description: "testing",
				Statements:  []string{"SELECT 1"},
				Username:    username.RootUserName(),
				Details: jobspb.SchemaChangeGCDetails{
					Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
						{
							ID:       42,
							DropTime: clock.PhysicalNow(),
						},
					},
				},
				Progress:      jobspb.SchemaChangeGCProgress{},
				DescriptorIDs: []descpb.ID{42},
			}

			j, err := jr.CreateJobWithTxn(ctx, jobRec, jobID, txn)
			if err != nil {
				return err
			}
			deprecatedSpansToProtect := roachpb.Spans{{Key: keys.MinKey, EndKey: keys.MaxKey}}
			targetToProtect := ptpb.MakeClusterTarget()
			rec = jobsprotectedts.MakeRecord(uuid.MakeV4(), int64(jobID), ts,
				deprecatedSpansToProtect, jobsprotectedts.Jobs, targetToProtect)
			if err := ptp.WithTxn(txn).Protect(ctx, rec); err != nil {
				return err
			}
			return f(ctx, txn, j)
		}))
		return rec
	}
	recMovedToFailed := mkJobAndRecord(func(ctx context.Context, txn isql.Txn, j *jobs.Job) error {
		return jr.UnsafeFailed(ctx, txn, j.ID(), io.ErrUnexpectedEOF)
	})
	recFinished := mkJobAndRecord(func(ctx context.Context, txn isql.Txn, j *jobs.Job) error {
		return jr.Succeeded(ctx, txn, j.ID())
	})
	recRemains := mkJobAndRecord(func(context.Context, isql.Txn, *jobs.Job) error { return nil })
	ensureNotExists := func(ctx context.Context, txn isql.Txn, ptsID uuid.UUID) (err error) {
		_, err = ptp.WithTxn(txn).GetRecord(ctx, ptsID)
		if err == nil {
			return errors.New("found pts record, waiting for ErrNotExists")
		}
		if errors.Is(err, protectedts.ErrNotExists) {
			return nil
		}
		return errors.Wrap(err, "waiting for ErrNotExists")
	}
	testutils.SucceedsSoon(t, func() (err error) {
		return insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			if err := ensureNotExists(ctx, txn, recMovedToFailed.ID.GetUUID()); err != nil {
				return err
			}
			if err := ensureNotExists(ctx, txn, recFinished.ID.GetUUID()); err != nil {
				return err
			}
			_, err := ptp.WithTxn(txn).GetRecord(ctx, recRemains.ID.GetUUID())
			require.NoError(t, err)
			return err
		})
	})

	// Verify that the two jobs we just observed as removed were recorded in the
	// metrics.
	runner.CheckQueryResultsRetry(t, `
SELECT
    value >= 2 -- we expect 2, but with retries it can be higher
FROM
    crdb_internal.node_metrics
WHERE
    name = 'kv.protectedts.reconciliation.records_removed';
`, [][]string{{"true"}})
}

// TestJobsProtectedTimestamp is an end-to-end test of protected timestamp
// reconciliation for jobs.
func TestJobsProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer jobs.TestingRegisterConstructor(
		jobspb.TypeSchemaChangeGC,
		func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return fakeResumer{}
		},
		jobs.UsesTenantCostControl)()

	ctx := context.Background()
	s0, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s0.Stopper().Stop(ctx)

	// Now I want to create some artifacts that should get reconciled away and
	// then make sure that they do and others which should not do not.
	hostRunner := sqlutils.MakeSQLRunner(db)

	hostRunner.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	hostRunner.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")
	// Also set what tenants see for these settings.
	// TODO(radu): use ALTER TENANT statement when that is available.
	hostRunner.Exec(t, `INSERT INTO system.tenant_settings (tenant_id, name, value, value_type)
		SELECT 0, name, value, "valueType" FROM system.settings
		WHERE name IN ('kv.closed_timestamp.target_duration', 'kv.protectedts.reconciliation.interval')`)

	t.Run("secondary-tenant", func(t *testing.T) {
		ten10, conn10 := serverutils.StartTenant(t, s0, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
		defer conn10.Close()
		ptp := ten10.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		execCfg := ten10.ExecutorConfig().(sql.ExecutorConfig)
		runner := sqlutils.MakeSQLRunner(conn10)
		jr := ten10.JobRegistry().(*jobs.Registry)
		testJobsProtectedTimestamp(ctx, t, runner, jr, &execCfg, ptp, ten10.Clock())
	})

	t.Run("system-tenant", func(t *testing.T) {
		ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
		jr := s0.JobRegistry().(*jobs.Registry)
		testJobsProtectedTimestamp(ctx, t, hostRunner, jr, &execCfg, ptp, s0.Clock())
	})
}

func testSchedulesProtectedTimestamp(
	ctx context.Context,
	t *testing.T,
	runner *sqlutils.SQLRunner,
	execCfg *sql.ExecutorConfig,
	ptp protectedts.Provider,
	clock *hlc.Clock,
) {
	t.Helper()

	insqlDB := execCfg.InternalDB
	mkScheduledJobRec := func(scheduleLabel string) *jobs.ScheduledJob {
		j := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
		j.SetScheduleLabel(scheduleLabel)
		j.SetOwner(username.TestUserName())
		any, err := types.MarshalAny(&jobspb.SqlStatementExecutionArg{Statement: ""})
		require.NoError(t, err)
		j.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
		j.SetExecutionDetails(jobs.InlineExecutorName, jobspb.ExecutionArguments{Args: any})
		return j
	}
	mkScheduleAndRecord := func(scheduleLabel string) (*jobs.ScheduledJob, *ptpb.Record) {
		ts := clock.Now()
		var rec *ptpb.Record
		var sj *jobs.ScheduledJob
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
			schedules := jobs.ScheduledJobTxn(txn)
			sj = mkScheduledJobRec(scheduleLabel)
			require.NoError(t, schedules.Create(ctx, sj))
			deprecatedSpansToProtect := roachpb.Spans{{Key: keys.MinKey, EndKey: keys.MaxKey}}
			targetToProtect := ptpb.MakeClusterTarget()
			rec = jobsprotectedts.MakeRecord(uuid.MakeV4(), int64(sj.ScheduleID()), ts,
				deprecatedSpansToProtect, jobsprotectedts.Schedules, targetToProtect)
			return ptp.WithTxn(txn).Protect(ctx, rec)
		}))
		return sj, rec
	}
	sjDropped, recScheduleDropped := mkScheduleAndRecord("drop")
	_, err := insqlDB.Executor().Exec(ctx, "drop-schedule", nil,
		`DROP SCHEDULE $1`, sjDropped.ScheduleID())
	require.NoError(t, err)
	_, recSchedule := mkScheduleAndRecord("do-not-drop")
	ensureNotExists := func(ctx context.Context, txn isql.Txn, ptsID uuid.UUID) (err error) {
		_, err = ptp.WithTxn(txn).GetRecord(ctx, ptsID)
		if err == nil {
			return errors.New("found pts record, waiting for ErrNotExists")
		}
		if errors.Is(err, protectedts.ErrNotExists) {
			return nil
		}
		return errors.Wrap(err, "waiting for ErrNotExists")
	}
	testutils.SucceedsSoon(t, func() (err error) {
		return insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			if err := ensureNotExists(ctx, txn, recScheduleDropped.ID.GetUUID()); err != nil {
				return err
			}
			_, err := ptp.WithTxn(txn).GetRecord(ctx, recSchedule.ID.GetUUID())
			require.NoError(t, err)
			return err
		})
	})

	// Verify that the two jobs we just observed as removed were recorded in the
	// metrics.
	runner.CheckQueryResultsRetry(t, `
SELECT
    value
FROM
    crdb_internal.node_metrics
WHERE
    name = 'kv.protectedts.reconciliation.records_removed';
`, [][]string{{"1"}})
}

// TestSchedulesProtectedTimestamp is an end-to-end test of protected timestamp
// reconciliation for schedules.
func TestSchedulesProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer jobs.TestingRegisterConstructor(
		jobspb.TypeSchemaChangeGC,
		func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return fakeResumer{}
		},
		jobs.UsesTenantCostControl)()

	ctx := context.Background()
	s0, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s0.Stopper().Stop(ctx)

	// Now I want to create some artifacts that should get reconciled away and
	// then make sure that they do and others which should not do not.
	hostRunner := sqlutils.MakeSQLRunner(db)

	hostRunner.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	hostRunner.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")
	// Also set what tenants see for these settings.
	// TODO(radu): use ALTER TENANT statement when that is available.
	hostRunner.Exec(t, `INSERT INTO system.tenant_settings (tenant_id, name, value, value_type)
		SELECT 0, name, value, "valueType" FROM system.settings
		WHERE name IN ('kv.closed_timestamp.target_duration', 'kv.protectedts.reconciliation.interval')`)

	t.Run("secondary-tenant", func(t *testing.T) {
		ten10, conn10 := serverutils.StartTenant(t, s0, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
		defer conn10.Close()
		ptp := ten10.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		execCfg := ten10.ExecutorConfig().(sql.ExecutorConfig)
		runner := sqlutils.MakeSQLRunner(conn10)
		testSchedulesProtectedTimestamp(ctx, t, runner, &execCfg, ptp, ten10.Clock())
	})

	t.Run("system-tenant", func(t *testing.T) {
		ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
		testSchedulesProtectedTimestamp(ctx, t, hostRunner, &execCfg, ptp, s0.Clock())
	})
}
