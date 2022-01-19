// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func writeColumnMutation(
	t *testing.T,
	kvDB *kv.DB,
	tableDesc *tabledesc.Mutable,
	column string,
	m descpb.DescriptorMutation,
) {
	col, err := tableDesc.FindColumnWithName(tree.Name(column))
	if err != nil {
		t.Fatal(err)
	}
	for i := range tableDesc.Columns {
		if col.GetID() == tableDesc.Columns[i].ID {
			// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
			// to ColumnDescriptors may unexpectedly change.
			tableDesc.Columns = append(tableDesc.Columns[:i:i], tableDesc.Columns[i+1:]...)
			break
		}
	}
	m.Descriptor_ = &descpb.DescriptorMutation_Column{Column: col.ColumnDesc()}
	writeMutation(t, kvDB, tableDesc, m)
}

// writeMutation writes the mutation to the table descriptor.
func writeMutation(
	t *testing.T, kvDB *kv.DB, tableDesc *tabledesc.Mutable, m descpb.DescriptorMutation,
) {
	tableDesc.Mutations = append(tableDesc.Mutations, m)
	tableDesc.Version++
	if err := descbuilder.ValidateSelf(tableDesc, clusterversion.TestingClusterVersion); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

type mutationOptions struct {
	// Set if the desc should have any mutations of any sort.
	hasMutation bool
	// Set if the desc should have a job that is dropping it.
	hasDropJob bool
}

func (m mutationOptions) string() string {
	return fmt.Sprintf("hasMutation=%s_hasDropJob=%s",
		strconv.FormatBool(m.hasMutation),
		strconv.FormatBool(m.hasDropJob))
}

func TestRegistryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// This test directly modifies `system.jobs` and makes over its contents
				// by querying it. We disable the auto span config reconciliation job
				// from getting created so that we don't have to special case it in the
				// test itself.
				ManagerDisableJobCreation: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := timeutil.Now()
	earlier := ts.Add(-1 * time.Hour)
	muchEarlier := ts.Add(-2 * time.Hour)

	setDropJob := func(dbName, tableName string) {
		desc := desctestutils.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, dbName, tableName)
		desc.DropJobID = 123
		if err := kvDB.Put(
			context.Background(),
			catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
			desc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
	}

	constructTableName := func(prefix string, mutOptions mutationOptions) string {
		return fmt.Sprintf("%s_%s", prefix, mutOptions.string())
	}

	writeJob := func(name string, created, finished time.Time, status Status, mutOptions mutationOptions) string {
		tableName := constructTableName(name, mutOptions)
		if _, err := sqlDB.Exec(fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS t;
CREATE TABLE t."%s" (k VARCHAR PRIMARY KEY DEFAULT 'default', v VARCHAR,i VARCHAR NOT NULL DEFAULT 'i');
INSERT INTO t."%s" VALUES('a', 'foo');
`, tableName, tableName)); err != nil {
			t.Fatal(err)
		}
		tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", tableName)
		if mutOptions.hasDropJob {
			setDropJob("t", tableName)
		}
		if mutOptions.hasMutation {
			writeColumnMutation(t, kvDB, tableDesc, "i", descpb.DescriptorMutation{State: descpb.
				DescriptorMutation_DELETE_AND_WRITE_ONLY, Direction: descpb.DescriptorMutation_DROP})
		}
		payload, err := protoutil.Marshal(&jobspb.Payload{
			Description: name,
			// register a mutation on the table so that jobs that reference
			// the table are not considered orphaned
			DescriptorIDs: []descpb.ID{
				tableDesc.GetID(),
				descpb.InvalidID, // invalid id to test handling of missing descriptors.
			},
			Details:        jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{}),
			StartedMicros:  timeutil.ToUnixMicros(created),
			FinishedMicros: timeutil.ToUnixMicros(finished),
		})
		if err != nil {
			t.Fatal(err)
		}
		progress, err := protoutil.Marshal(&jobspb.Progress{
			Details: jobspb.WrapProgressDetails(jobspb.SchemaChangeProgress{}),
		})
		if err != nil {
			t.Fatal(err)
		}

		var id jobspb.JobID
		db.QueryRow(t,
			`INSERT INTO system.jobs (status, payload, progress, created) VALUES ($1, $2, $3, $4) RETURNING id`,
			status, payload, progress, created).Scan(&id)
		return strconv.Itoa(int(id))
	}

	// Test the descriptor when any of the following are set.
	// 1. Mutations
	// 2. A drop job
	for _, hasMutation := range []bool{true, false} {
		for _, hasDropJob := range []bool{true, false} {
			if !hasMutation && !hasDropJob {
				continue
			}
			mutOptions := mutationOptions{
				hasMutation: hasMutation,
				hasDropJob:  hasDropJob,
			}
			oldRunningJob := writeJob("old_running", muchEarlier, time.Time{}, StatusRunning, mutOptions)
			oldSucceededJob := writeJob("old_succeeded", muchEarlier, muchEarlier.Add(time.Minute), StatusSucceeded, mutOptions)
			oldFailedJob := writeJob("old_failed", muchEarlier, muchEarlier.Add(time.Minute),
				StatusFailed, mutOptions)
			oldRevertFailedJob := writeJob("old_revert_failed", muchEarlier, muchEarlier.Add(time.Minute),
				StatusRevertFailed, mutOptions)
			oldCanceledJob := writeJob("old_canceled", muchEarlier, muchEarlier.Add(time.Minute),
				StatusCanceled, mutOptions)
			newRunningJob := writeJob("new_running", earlier, earlier.Add(time.Minute), StatusRunning,
				mutOptions)
			newSucceededJob := writeJob("new_succeeded", earlier, earlier.Add(time.Minute), StatusSucceeded, mutOptions)
			newFailedJob := writeJob("new_failed", earlier, earlier.Add(time.Minute), StatusFailed, mutOptions)
			newRevertFailedJob := writeJob("new_revert_failed", earlier, earlier.Add(time.Minute), StatusRevertFailed, mutOptions)
			newCanceledJob := writeJob("new_canceled", earlier, earlier.Add(time.Minute),
				StatusCanceled, mutOptions)

			db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
				{oldRunningJob}, {oldSucceededJob}, {oldFailedJob}, {oldRevertFailedJob}, {oldCanceledJob},
				{newRunningJob}, {newSucceededJob}, {newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

			if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
				t.Fatal(err)
			}
			db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
				{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newSucceededJob},
				{newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

			if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
				t.Fatal(err)
			}
			db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
				{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newRevertFailedJob}})

			// Delete the revert failed, and running jobs for the next run of the
			// test.
			_, err := sqlDB.Exec(`DELETE FROM system.jobs WHERE id = $1 OR id = $2 OR id = $3 OR id = $4`,
				oldRevertFailedJob, newRevertFailedJob, oldRunningJob, newRunningJob)
			require.NoError(t, err)
		}
	}
}

func TestRegistryGCPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// This test directly modifies `system.jobs` and makes over its contents
				// by querying it. We disable the auto span config reconciliation job
				// from getting created so that we don't have to special case it in the
				// test itself.
				ManagerDisableJobCreation: true,
			},
		},
	})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(ctx)

	for i := 0; i < 2*cleanupPageSize+1; i++ {
		payload, err := protoutil.Marshal(&jobspb.Payload{})
		require.NoError(t, err)
		db.Exec(t,
			`INSERT INTO system.jobs (status, created, payload) VALUES ($1, $2, $3)`,
			StatusCanceled, timeutil.Now().Add(-time.Hour), payload)
	}

	ts := timeutil.Now()
	require.NoError(t, s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(-10*time.Minute)))
	var count int
	db.QueryRow(t, `SELECT count(1) FROM system.jobs`).Scan(&count)
	require.Zero(t, count)
}

func TestBatchJobsCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, test := range []struct {
		name      string
		batchSize int
	}{
		{"small batch", 10},
		{"medium batch", 501},
		{"large batch", 1001},
		{"extra large batch", 5001},
	} {
		t.Run(test.name, func(t *testing.T) {
			{
				if test.batchSize > 10 {
					skip.UnderStress(t, "skipping stress test for batch size ", test.batchSize)
					skip.UnderRace(t, "skipping test for batch size ", test.batchSize)
				}

				args := base.TestServerArgs{
					Knobs: base.TestingKnobs{
						// Avoiding jobs to be adopted.
						JobsTestingKnobs: &TestingKnobs{
							DisableAdoptions: true,
						},
					},
				}

				ctx := context.Background()
				s, sqlDB, kvDB := serverutils.StartServer(t, args)
				tdb := sqlutils.MakeSQLRunner(sqlDB)
				defer s.Stopper().Stop(ctx)
				r := s.JobRegistry().(*Registry)

				RegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
					return FakeResumer{
						OnResume: func(ctx context.Context) error {
							return nil
						},
					}
				})

				// Create a batch of job specifications.
				var records []*Record
				for i := 0; i < test.batchSize; i++ {
					records = append(records, &Record{
						JobID:    r.MakeJobID(),
						Details:  jobspb.ImportDetails{},
						Progress: jobspb.ImportProgress{},
					})
				}
				// Create jobs in a batch.
				var jobIDs []jobspb.JobID
				require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					var err error
					jobIDs, err = r.CreateJobsWithTxn(ctx, txn, records)
					return err
				}))
				require.Equal(t, len(jobIDs), test.batchSize)
				tdb.CheckQueryResults(t, "SELECT count(*) FROM [SHOW JOBS]",
					[][]string{{fmt.Sprintf("%d", test.batchSize)}})
			}
		})
	}
}

// TestRetriesWithExponentialBackoff tests the working of exponential delays
// when jobs are retried. Moreover, it tests the effectiveness of the upper
// bound on the retry delay.
func TestRetriesWithExponentialBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		// Number of retries should be reasonably large such that they are sufficient
		// to test long delays but not too many to increase the test time.
		retries = 50

		unitTime = time.Millisecond
		// initDelay and maxDelay can be large as we jump in time through a fake clock.
		initialDelay = time.Second
		maxDelay     = time.Hour

		pause  = true
		cancel = false
	)

	// createJob creates a mock job.
	createJob := func(
		ctx context.Context, s serverutils.TestServerInterface, r *Registry, tdb *sqlutils.SQLRunner, kvDB *kv.DB,
	) (jobspb.JobID, time.Time) {
		jobID := r.MakeJobID()
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := r.CreateJobWithTxn(ctx, Record{
				Details:  jobspb.ImportDetails{},
				Progress: jobspb.ImportProgress{},
			}, jobID, txn)
			return err
		}))
		var lastRun time.Time
		tdb.QueryRow(t,
			"SELECT created FROM system.jobs where id = $1", jobID,
		).Scan(&lastRun)
		return jobID, lastRun
	}
	waitUntilCount := func(t *testing.T, counter *metric.Counter, count int64) {
		testutils.SucceedsSoon(t, func() error {
			cnt := counter.Count()
			if cnt >= count {
				return nil
			}
			return errors.Errorf(
				"waiting for %v to reach %d, currently at %d", counter.GetName(), count, cnt,
			)
		})
	}
	waitUntilStatus := func(t *testing.T, tdb *sqlutils.SQLRunner, jobID jobspb.JobID, status Status) {
		tdb.CheckQueryResultsRetry(t,
			fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", jobID),
			[][]string{{string(status)}})
	}
	// pauseOrCancelJob pauses or cancels a job. If pauseJob is true, the job is paused,
	// otherwise the job is canceled.
	pauseOrCancelJob := func(
		t *testing.T, ctx context.Context, db *kv.DB, registry *Registry, jobID jobspb.JobID, pauseJob bool,
	) {
		assert.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if pauseJob {
				return registry.PauseRequested(ctx, txn, jobID, "")
			}
			return registry.CancelRequested(ctx, txn, jobID)
		}))
	}
	// nextDelay returns the next delay based calculated from the given retryCnt
	// and exponential-backoff parameters.
	nextDelay := func(retryCnt int, initDelay time.Duration, maxDelay time.Duration) time.Duration {
		delay := initDelay * ((1 << int(math.Min(62, float64(retryCnt)))) - 1)
		if delay < 0 {
			delay = maxDelay
		}
		return time.Duration(math.Min(float64(delay), float64(maxDelay)))
	}

	type BackoffTestInfra struct {
		s                        serverutils.TestServerInterface
		tdb                      *sqlutils.SQLRunner
		kvDB                     *kv.DB
		registry                 *Registry
		clock                    *timeutil.ManualTime
		resumeCh                 chan struct{}
		failOrCancelCh           chan struct{}
		transitionCh             chan struct{}
		errCh                    chan error
		done                     atomic.Value
		jobMetrics               *JobTypeMetrics
		adopted                  *metric.Counter
		resumed                  *metric.Counter
		afterJobStateMachineKnob func()
		// expectImmediateRetry is true if the test should expect immediate
		// resumption on retry, such as after pausing and resuming job.
		expectImmediateRetry bool
	}
	testInfraSetUp := func(ctx context.Context, bti *BackoffTestInfra) func() {
		// We use a manual clock to control and evaluate job execution times.
		// We initialize the clock with Now() because the job-creation timestamp,
		// 'created' column in system.jobs, of a new job is set from txn's time.
		bti.clock = timeutil.NewManualTime(timeutil.Now())
		timeSource := hlc.NewClock(func() int64 {
			return bti.clock.Now().UnixNano()
		}, base.DefaultMaxClockOffset)
		// Set up the test cluster.
		knobs := &TestingKnobs{
			TimeSource: timeSource,
		}
		if bti.afterJobStateMachineKnob != nil {
			knobs.AfterJobStateMachine = bti.afterJobStateMachineKnob
		}
		cs := cluster.MakeTestingClusterSettings()
		// Set a small adopt and cancel intervals to reduce test time.
		adoptIntervalSetting.Override(ctx, &cs.SV, unitTime)
		cancelIntervalSetting.Override(ctx, &cs.SV, unitTime)
		retryInitialDelaySetting.Override(ctx, &cs.SV, initialDelay)
		retryMaxDelaySetting.Override(ctx, &cs.SV, maxDelay)
		args := base.TestServerArgs{
			Settings: cs,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: knobs,
				SpanConfig: &spanconfig.TestingKnobs{
					// This test directly modifies `system.jobs` and makes over its contents
					// by querying it. We disable the auto span config reconciliation job
					// from getting created so that we don't have to special case it in the
					// test itself.
					ManagerDisableJobCreation: true,
				},
			},
		}
		var sqlDB *gosql.DB
		bti.s, sqlDB, bti.kvDB = serverutils.StartServer(t, args)
		cleanup := func() {
			close(bti.errCh)
			close(bti.resumeCh)
			close(bti.failOrCancelCh)
			bti.s.Stopper().Stop(ctx)
		}
		bti.tdb = sqlutils.MakeSQLRunner(sqlDB)
		bti.registry = bti.s.JobRegistry().(*Registry)
		bti.resumeCh = make(chan struct{})
		bti.failOrCancelCh = make(chan struct{})
		bti.transitionCh = make(chan struct{})
		bti.errCh = make(chan error)
		bti.done.Store(false)
		bti.jobMetrics = bti.registry.metrics.JobMetrics[jobspb.TypeImport]
		bti.adopted = bti.registry.metrics.AdoptIterations
		bti.resumed = bti.registry.metrics.ResumedJobs
		RegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
			return FakeResumer{
				OnResume: func(ctx context.Context) error {
					if bti.done.Load().(bool) {
						return nil
					}
					bti.resumeCh <- struct{}{}
					return <-bti.errCh
				},
				FailOrCancel: func(ctx context.Context) error {
					if bti.done.Load().(bool) {
						return nil
					}
					bti.failOrCancelCh <- struct{}{}
					return <-bti.errCh
				},
			}
		})
		return cleanup
	}

	runTest := func(t *testing.T, jobID jobspb.JobID, retryCnt int, expectedResumed int64, lastRun time.Time, bti *BackoffTestInfra, waitFn func(int64)) {
		// We retry for a fixed number. For each retry:
		// - We first advance the clock such that it is a little behind the
		//   next retry time.
		// - We then wait until a few adopt-loops complete to ensure that
		//   the registry gets a chance to pick up a job if it can.
		// - We validate that the number of resumed jobs has not increased to
		//   ensure that a job is not started/resumed/reverted before its next
		//   retry time.
		// - We then advance the clock to the exact next retry time. Now the job
		//   can be retried in the next adopt-loop.
		// - We wait until the resumer completes one execution and the job needs
		//   to be picked up again in the next adopt-loop.
		// - Now we validate that resumedJobs counter has incremented, which ensures
		//   that the job has completed only one cycle in this time.
		//
		// If retries do not happen based on exponential-backoff times, our counters
		// will not match, causing the test to fail in validateCount.

		for i := 0; i < retries; i++ {
			// Exponential delay in the next retry.
			delay := nextDelay(retryCnt, initialDelay, maxDelay)
			// The delay must not exceed the max delay setting. It ensures that
			// we are expecting correct timings from our test code, which in turn
			// ensures that the jobs are resumed with correct exponential delays.
			require.GreaterOrEqual(t, maxDelay, delay, "delay exceeds the max")
			// Advance the clock such that it is before the next expected retry time.
			bti.clock.AdvanceTo(lastRun.Add(delay - unitTime))
			// This allows adopt-loops to run for a few times, which ensures that
			// adopt-loops do not resume jobs without correctly following the job
			// schedules.
			waitUntilCount(t, bti.adopted, bti.adopted.Count()+2)
			if bti.expectImmediateRetry && i > 0 {
				// Validate that the job did not wait to resume on retry.
				require.Equal(t, expectedResumed+1, bti.resumed.Count(), "unexpected number of jobs resumed in retry %d", i)
			} else {
				// Validate that the job is not resumed yet.
				require.Equal(t, expectedResumed, bti.resumed.Count(), "unexpected number of jobs resumed in retry %d", i)
			}
			// Advance the clock by delta from the expected time of next retry.
			bti.clock.Advance(unitTime)
			// Wait until the resumer completes its execution.
			waitFn(int64(retryCnt))
			expectedResumed++
			retryCnt++
			// Validate that the job is resumed only once.
			require.Equal(t, expectedResumed, bti.resumed.Count(), "unexpected number of jobs resumed in retry %d", i)
			lastRun = bti.clock.Now()
		}
		bti.done.Store(true)
		// Let the job be retried one more time.
		bti.clock.Advance(nextDelay(retryCnt, initialDelay, maxDelay))
		// Wait until the job completes.
		testutils.SucceedsSoon(t, func() error {
			var found Status
			bti.tdb.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", jobID).Scan(&found)
			if found.Terminal() {
				return nil
			}
			retryCnt++
			bti.clock.Advance(nextDelay(retryCnt, initialDelay, maxDelay))
			return errors.Errorf("waiting job %d to reach a terminal state, currently %s", jobID, found)
		})
	}

	t.Run("running", func(t *testing.T) {
		ctx := context.Background()
		bti := BackoffTestInfra{}
		bti.afterJobStateMachineKnob = func() {
			if bti.done.Load().(bool) {
				return
			}
			bti.transitionCh <- struct{}{}
		}
		cleanup := testInfraSetUp(ctx, &bti)
		defer cleanup()

		jobID, lastRun := createJob(ctx, bti.s, bti.registry, bti.tdb, bti.kvDB)
		retryCnt := 0
		expectedResumed := int64(0)
		runTest(t, jobID, retryCnt, expectedResumed, lastRun, &bti, func(_ int64) {
			<-bti.resumeCh
			bti.errCh <- MarkAsRetryJobError(errors.New("injecting error to retry running"))
			<-bti.transitionCh
		})
	})

	t.Run("pause running", func(t *testing.T) {
		ctx := context.Background()
		bti := BackoffTestInfra{expectImmediateRetry: true}
		skip.WithIssue(t, 74399)
		bti.afterJobStateMachineKnob = func() {
			if bti.done.Load().(bool) {
				return
			}
			bti.transitionCh <- struct{}{}
		}
		cleanup := testInfraSetUp(ctx, &bti)
		defer cleanup()

		jobID, lastRun := createJob(ctx, bti.s, bti.registry, bti.tdb, bti.kvDB)
		retryCnt := 0
		expectedResumed := int64(0)
		runTest(t, jobID, retryCnt, expectedResumed, lastRun, &bti, func(_ int64) {
			<-bti.resumeCh
			pauseOrCancelJob(t, ctx, bti.kvDB, bti.registry, jobID, pause)
			bti.errCh <- nil
			<-bti.transitionCh
			waitUntilStatus(t, bti.tdb, jobID, StatusPaused)
			require.NoError(t, bti.registry.Unpause(ctx, nil, jobID))
		})
	})

	t.Run("revert on fail", func(t *testing.T) {
		ctx := context.Background()
		bti := BackoffTestInfra{}
		bti.afterJobStateMachineKnob = func() {
			if bti.done.Load().(bool) {
				return
			}
			bti.transitionCh <- struct{}{}
		}
		cleanup := testInfraSetUp(ctx, &bti)
		defer cleanup()

		jobID, lastRun := createJob(ctx, bti.s, bti.registry, bti.tdb, bti.kvDB)
		bti.clock.AdvanceTo(lastRun)
		<-bti.resumeCh
		bti.errCh <- errors.Errorf("injecting error to revert")
		<-bti.failOrCancelCh
		bti.errCh <- MarkAsRetryJobError(errors.New("injecting error in reverting state to retry"))
		<-bti.transitionCh
		expectedResumed := bti.resumed.Count()
		retryCnt := 1
		runTest(t, jobID, retryCnt, expectedResumed, lastRun, &bti, func(_ int64) {
			<-bti.failOrCancelCh
			bti.errCh <- MarkAsRetryJobError(errors.New("injecting error in reverting state to retry"))
			<-bti.transitionCh
		})
	})

	t.Run("revert on cancel", func(t *testing.T) {
		ctx := context.Background()
		bti := BackoffTestInfra{}
		cleanup := testInfraSetUp(ctx, &bti)
		defer cleanup()

		jobID, lastRun := createJob(ctx, bti.s, bti.registry, bti.tdb, bti.kvDB)
		bti.clock.AdvanceTo(lastRun)
		<-bti.resumeCh
		pauseOrCancelJob(t, ctx, bti.kvDB, bti.registry, jobID, cancel)
		bti.errCh <- nil
		<-bti.failOrCancelCh
		bti.errCh <- MarkAsRetryJobError(errors.New("injecting error in reverting state"))
		expectedResumed := bti.resumed.Count()
		retryCnt := 1
		runTest(t, jobID, retryCnt, expectedResumed, lastRun, &bti, func(retryCnt int64) {
			<-bti.failOrCancelCh
			bti.errCh <- MarkAsRetryJobError(errors.New("injecting error in reverting state"))
			waitUntilCount(t, bti.jobMetrics.FailOrCancelRetryError, retryCnt+1)
		})
	})

	t.Run("pause reverting", func(t *testing.T) {
		ctx := context.Background()
		bti := BackoffTestInfra{expectImmediateRetry: true}
		skip.WithIssue(t, 74399)

		bti.afterJobStateMachineKnob = func() {
			if bti.done.Load().(bool) {
				return
			}
			bti.transitionCh <- struct{}{}
		}
		cleanup := testInfraSetUp(ctx, &bti)
		defer cleanup()

		jobID, lastRun := createJob(ctx, bti.s, bti.registry, bti.tdb, bti.kvDB)
		bti.clock.AdvanceTo(lastRun)
		<-bti.resumeCh
		bti.errCh <- errors.Errorf("injecting error to revert")
		<-bti.failOrCancelCh
		bti.errCh <- MarkAsRetryJobError(errors.New("injecting error in reverting state to retry"))
		<-bti.transitionCh
		expectedResumed := bti.resumed.Count()
		retryCnt := 1
		runTest(t, jobID, retryCnt, expectedResumed, lastRun, &bti, func(_ int64) {
			<-bti.failOrCancelCh
			pauseOrCancelJob(t, ctx, bti.kvDB, bti.registry, jobID, pause)
			// We have to return error here because, otherwise, the job will be marked as
			// failed regardless of the fact that it is currently pause-requested in the
			// jobs table. This is because we currently do not check the current status
			// of a job before marking it as failed.
			bti.errCh <- MarkAsRetryJobError(errors.New("injecting error in reverting state to retry"))
			<-bti.transitionCh
			waitUntilStatus(t, bti.tdb, jobID, StatusPaused)
			require.NoError(t, bti.registry.Unpause(ctx, nil, jobID))
		})
	})
}

// TestExponentialBackoffSettings tests the cluster settings of exponential backoff delays.
func TestExponentialBackoffSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, test := range [...]struct {
		name string // Test case ID.
		// The setting to test.
		settingKey string
		// The value of the setting to set.
		value time.Duration
	}{
		{
			name:       "backoff initial delay setting",
			settingKey: retryInitialDelaySettingKey,
			value:      2 * time.Millisecond,
		},
		{
			name:       "backoff max delay setting",
			settingKey: retryMaxDelaySettingKey,
			value:      2 * time.Millisecond,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var tdb *sqlutils.SQLRunner
			var finished atomic.Value
			finished.Store(false)
			var intercepted atomic.Value
			intercepted.Store(false)
			intercept := func(orig, updated JobMetadata) error {
				// If this updated is not to mark as succeeded or the test has already failed.
				if updated.Status != StatusSucceeded {
					return nil
				}

				// If marking the first time, prevent the marking and update the cluster
				// setting based on test params. The setting value should be reduced
				// from a large value to a small value.
				if !intercepted.Load().(bool) {
					tdb.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%v'", test.settingKey, test.value))
					intercepted.Store(true)
					return errors.Errorf("preventing the job from succeeding")
				}
				// Let the job to succeed. As we began with a long interval and prevented
				// the job than succeeding in the first attempt, its re-execution
				// indicates that the setting is updated successfully and is in effect.
				finished.Store(true)
				return nil
			}

			// Setup the test cluster.
			cs := cluster.MakeTestingClusterSettings()
			// Set a small adopt interval to reduce test time.
			adoptIntervalSetting.Override(ctx, &cs.SV, 2*time.Millisecond)
			// Begin with a long delay.
			retryInitialDelaySetting.Override(ctx, &cs.SV, time.Hour)
			retryMaxDelaySetting.Override(ctx, &cs.SV, time.Hour)
			args := base.TestServerArgs{
				Settings: cs,
				Knobs:    base.TestingKnobs{JobsTestingKnobs: &TestingKnobs{BeforeUpdate: intercept}},
			}
			s, sdb, kvDB := serverutils.StartServer(t, args)
			defer s.Stopper().Stop(ctx)
			tdb = sqlutils.MakeSQLRunner(sdb)
			// Create and run a dummy job.
			RegisterConstructor(jobspb.TypeImport, func(_ *Job, cs *cluster.Settings) Resumer {
				return FakeResumer{}
			})
			registry := s.JobRegistry().(*Registry)
			id := registry.MakeJobID()
			require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := registry.CreateJobWithTxn(ctx, Record{
					// Job does not accept an empty Details field, so arbitrarily provide
					// ImportDetails.
					Details:  jobspb.ImportDetails{},
					Progress: jobspb.ImportProgress{},
				}, id, txn)
				return err
			}))

			// Wait for the job to be succeed.
			testutils.SucceedsSoon(t, func() error {
				if finished.Load().(bool) {
					return nil
				}
				return errors.Errorf("waiting for the job to complete")
			})
		})
	}
}

func TestRegistryUsePartialIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Mock args.
	const (
		id  = 1
		sid = "bytes"
		iid = 1
		lim = 10
		d   = time.Millisecond
	)
	ts := timeutil.Now()

	for _, test := range []struct {
		name      string
		query     string
		queryArgs []interface{}
	}{
		{"remove claims", RemoveClaimsQuery, []interface{}{sid, lim}},
		{"claim jobs", AdoptQuery, []interface{}{sid, iid, lim}},
		{"process claimed jobs", ProcessJobsQuery, []interface{}{sid, iid, ts, d, d}},
		{"serve cancel and pause", CancelQuery, []interface{}{sid, iid}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			tdb := sqlutils.MakeSQLRunner(sqlDB)

			usingIndex := false
			rows := tdb.Query(t, "EXPLAIN "+test.query, test.queryArgs...)
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				if strings.Contains(line, "table: jobs@jobs_run_stats_idx (partial index)") {
					usingIndex = true
					break
				}
			}
			require.NoError(t, rows.Close())
			require.True(t, usingIndex, "partial index is not used")
		})
	}
}

// TestJitterCalculation tests the correctness of jitter calculation function
// in jobs/config.go.
func TestJitterCalculation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		minFactor = 1 - (1.0 / 6.0)
		maxFactor = 1 + (1.0 / 6.0)
	)

	outputRange := func(input time.Duration) (time.Duration, time.Duration) {
		return time.Duration(float64(input) * minFactor), time.Duration(float64(input) * maxFactor)
	}

	for _, test := range []struct {
		name  string
		input time.Duration
	}{
		{
			"zero",
			0,
		},
		{
			"small",
			time.Millisecond,
		},
		{
			"large",
			100 * time.Hour,
		},
		{
			"default",
			defaultAdoptInterval,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			interval := jitter(test.input)
			rangeMin, rangeMax := outputRange(test.input)
			require.GreaterOrEqual(t, rangeMax, interval)
			require.LessOrEqual(t, rangeMin, interval)
			if test.input != 0 {
				require.NotEqual(t, test.input, interval)
			}
		})
	}
}

// TestRunWithoutLoop tests that Run calls will trigger the execution of a
// job even when the adoption loop is set to infinitely slow and that the
// observation of the completion of the job using the notification channel
// for local jobs works.
func TestRunWithoutLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer ResetConstructors()
	var shouldFailCounter int64
	var ran, failure int64
	RegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
		var successDone, failureDone int64
		shouldFail := atomic.AddInt64(&shouldFailCounter, 1)%2 == 0
		maybeIncrementCounter := func(check, counter *int64) {
			if atomic.CompareAndSwapInt64(check, 0, 1) {
				atomic.AddInt64(counter, 1)
			}
		}
		return FakeResumer{
			OnResume: func(ctx context.Context) error {
				maybeIncrementCounter(&successDone, &ran)
				if shouldFail {
					return errors.New("boom")
				}
				return nil
			},
			FailOrCancel: func(ctx context.Context) error {
				maybeIncrementCounter(&failureDone, &failure)
				return nil
			},
		}
	})

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	intervalBaseSetting.Override(ctx, &settings.SV, 1e6)
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})

	defer s.Stopper().Stop(ctx)
	r := s.JobRegistry().(*Registry)
	var records []*Record
	const N = 10
	for i := 0; i < N; i++ {
		records = append(records, &Record{
			JobID:       r.MakeJobID(),
			Description: "testing",
			Username:    security.RootUserName(),
			Details:     jobspb.ImportDetails{},
			Progress:    jobspb.ImportProgress{},
		})
	}
	var jobIDs []jobspb.JobID
	require.NoError(t, kvDB.Txn(ctx, func(
		ctx context.Context, txn *kv.Txn,
	) (err error) {
		jobIDs, err = r.CreateJobsWithTxn(ctx, txn, records)
		return err
	}))
	require.EqualError(t, r.Run(
		ctx, s.InternalExecutor().(sqlutil.InternalExecutor), jobIDs,
	), "boom")
	// No adoption loops should have been run.
	require.Equal(t, int64(0), r.metrics.AdoptIterations.Count())
	require.Equal(t, int64(N), atomic.LoadInt64(&ran))
	require.Equal(t, int64(N/2), atomic.LoadInt64(&failure))
}

func TestJobIdleness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	intervalOverride := time.Millisecond
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		// Ensure no other jobs are created and adoptions and cancellations are quick
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true,
			},
			JobsTestingKnobs: &TestingKnobs{
				IntervalOverrides: TestingIntervalOverrides{
					Adopt:  &intervalOverride,
					Cancel: &intervalOverride,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	r := s.JobRegistry().(*Registry)

	resumeStartChan := make(chan struct{})
	resumeErrChan := make(chan error)
	defer close(resumeErrChan)
	RegisterConstructor(jobspb.TypeImport, func(_ *Job, cs *cluster.Settings) Resumer {
		return FakeResumer{
			OnResume: func(ctx context.Context) error {
				resumeStartChan <- struct{}{}
				return <-resumeErrChan
			},
		}
	})

	currentlyIdle := r.MetricsStruct().JobMetrics[jobspb.TypeImport].CurrentlyIdle

	createJob := func() *Job {
		jobID := r.MakeJobID()
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := r.CreateJobWithTxn(ctx, Record{
				Details:  jobspb.ImportDetails{},
				Progress: jobspb.ImportProgress{},
			}, jobID, txn)
			return err
		}))
		job, err := r.LoadJob(ctx, jobID)
		require.NoError(t, err)
		<-resumeStartChan
		return job
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	waitUntilStatus := func(t *testing.T, jobID jobspb.JobID, status Status) {
		tdb.CheckQueryResultsRetry(t,
			fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", jobID),
			[][]string{{string(status)}})
	}

	t.Run("MarkIdle", func(t *testing.T) {
		job1 := createJob()
		job2 := createJob()

		require.False(t, r.TestingIsJobIdle(job1.ID()))

		r.MarkIdle(job1, true)
		r.MarkIdle(job2, true)
		require.True(t, r.TestingIsJobIdle(job1.ID()))
		require.Equal(t, int64(2), currentlyIdle.Value())

		// Repeated calls should not increase metric
		r.MarkIdle(job1, true)
		r.MarkIdle(job1, true)
		require.Equal(t, int64(2), currentlyIdle.Value())

		r.MarkIdle(job1, false)
		require.Equal(t, int64(1), currentlyIdle.Value())
		require.False(t, r.TestingIsJobIdle(job1.ID()))
		r.MarkIdle(job2, false)
		require.Equal(t, int64(0), currentlyIdle.Value())

		// Let the jobs complete
		resumeErrChan <- nil
		resumeErrChan <- nil
	})

	t.Run("idleness disabled on state updates", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			update func(jobID jobspb.JobID)
		}{
			{"pause", func(jobID jobspb.JobID) {
				resumeErrChan <- MarkPauseRequestError(errors.Errorf("pause error"))
				waitUntilStatus(t, jobID, StatusPaused)
			}},
			{"succeeded", func(jobID jobspb.JobID) {
				resumeErrChan <- nil
				waitUntilStatus(t, jobID, StatusSucceeded)
			}},
			{"failed", func(jobID jobspb.JobID) {
				resumeErrChan <- errors.Errorf("error")
				waitUntilStatus(t, jobID, StatusFailed)
			}},
			{"cancel", func(jobID jobspb.JobID) {
				resumeErrChan <- errJobCanceled
				waitUntilStatus(t, jobID, StatusCanceled)
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				job := createJob()

				require.Equal(t, int64(0), currentlyIdle.Value())
				r.MarkIdle(job, true)
				require.Equal(t, int64(1), currentlyIdle.Value())

				test.update(job.ID())

				testutils.SucceedsSoon(t, func() error {
					if r.TestingIsJobIdle(job.ID()) {
						return errors.Errorf("waiting for job to unmark idle")
					}
					return nil
				})
				require.Equal(t, int64(0), currentlyIdle.Value())
			})
		}
	})

}
