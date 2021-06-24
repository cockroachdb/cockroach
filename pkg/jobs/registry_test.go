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
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	if err := catalog.ValidateSelf(tableDesc); err != nil {
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

func writeGCMutation(
	t *testing.T,
	kvDB *kv.DB,
	tableDesc *tabledesc.Mutable,
	m descpb.TableDescriptor_GCDescriptorMutation,
) {
	tableDesc.GCMutations = append(tableDesc.GCMutations, m)
	tableDesc.Version++
	if err := catalog.ValidateSelf(tableDesc); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

type mutationOptions struct {
	// Set if the desc should have any mutations of any sort.
	hasMutation bool
	// Set if the mutation being inserted is a GCMutation.
	hasGCMutation bool
	// Set if the desc should have a job that is dropping it.
	hasDropJob bool
}

func (m mutationOptions) string() string {
	return fmt.Sprintf("hasMutation=%s_hasGCMutation=%s_hasDropJob=%s",
		strconv.FormatBool(m.hasMutation), strconv.FormatBool(m.hasGCMutation),
		strconv.FormatBool(m.hasDropJob))
}

func TestRegistryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := timeutil.Now()
	earlier := ts.Add(-1 * time.Hour)
	muchEarlier := ts.Add(-2 * time.Hour)

	setDropJob := func(dbName, tableName string) {
		desc := catalogkv.TestingGetMutableExistingTableDescriptor(
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
		tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", tableName)
		if mutOptions.hasDropJob {
			setDropJob("t", tableName)
		}
		if mutOptions.hasMutation {
			writeColumnMutation(t, kvDB, tableDesc, "i", descpb.DescriptorMutation{State: descpb.
				DescriptorMutation_DELETE_AND_WRITE_ONLY, Direction: descpb.DescriptorMutation_DROP})
		}
		if mutOptions.hasGCMutation {
			writeGCMutation(t, kvDB, tableDesc, descpb.TableDescriptor_GCDescriptorMutation{})
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
	// 2. GC Mutations
	// 3. A drop job
	for _, hasMutation := range []bool{true, false} {
		for _, hasGCMutation := range []bool{true, false} {
			for _, hasDropJob := range []bool{true, false} {
				if !hasMutation && !hasGCMutation && !hasDropJob {
					continue
				}
				mutOptions := mutationOptions{
					hasMutation:   hasMutation,
					hasGCMutation: hasGCMutation,
					hasDropJob:    hasDropJob,
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
}

func TestRegistryGCPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
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

// TestRetriesWithExponentialBackoff tests job retries with exponentially growing
// intervals. Moreover, it tests the effectiveness of the upper bound on the
// retry delay.
func TestRetriesWithExponentialBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	unitTime := 1 * time.Millisecond
	// Interval to process claimed jobs.
	adoptInterval := unitTime

	clusterSettings := func(
		ctx context.Context, retryBase time.Duration, retryMax time.Duration,
	) *cluster.Settings {
		s := cluster.MakeTestingClusterSettings()
		// Set a small adopt interval to reduce test time.
		adoptIntervalSetting.Override(ctx, &s.SV, adoptInterval)
		// Set exponential backoff base and max retry interval based on the tests.
		retryBaseSetting.Override(ctx, &s.SV, retryBase)
		retryMaxDelaySetting.Override(ctx, &s.SV, retryMax)
		return s
	}

	for _, test := range [...]struct {
		name         string // Test case ID.
		maxRetries   int
		retryInitVal time.Duration
		retryMax     time.Duration
	}{
		{
			name:         "exponential backoff",
			retryInitVal: 2 * time.Millisecond,
			retryMax:     100 * time.Millisecond,
			// Number of retries should be large enough such that the delay becomes
			// larger than the error margin. Moreover, it should be large enough
			// to exceed the backoff time from retryMax.
			maxRetries: 6,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var registry *Registry
			const (
				running = iota
				passed
				failed
			)
			status := int32(running)
			retries := -1 // -1 because the counter has to start from zero.
			var lastRetry time.Time
			// We use a manual clock to control and evaluate job execution times.
			t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
			timeSource := timeutil.NewManualTime(t0)
			clock := hlc.NewClock(func() int64 {
				return timeSource.Now().UnixNano()
			}, base.DefaultMaxClockOffset)
			// Intercept intercepts the update call to mark the job as succeeded and
			// prevents the job from succeeding for a fixed number of runs. In each
			// run, it validates whether the job is run at the expected time, which
			// follows an exponentially increasing retry delays.
			intercept := func(orig, updated JobMetadata) error {
				// If updated is not marking as succeeded or if the test has already failed.
				if updated.Status != StatusSucceeded || atomic.LoadInt32(&status) == failed {
					return nil
				}
				retries++
				now := clock.Now().GoTime()
				if retries == 0 { // If the job is run the first time.
					lastRetry = now
				}
				// Expected next retry time.
				backoff := test.retryInitVal * ((1 << int(math.Min(float64(retries), 62))) - 1)
				delay := time.Duration(math.Min(float64(backoff), float64(test.retryMax)))
				expected := lastRetry.Add(delay)

				require.Equal(t, expected, now, "job executed at an unexpected time: "+
					"expected = %v, now = %v", expected, now)

				// The test passes if the job keeps running at expected times for a sufficient
				// number of times.
				if retries >= test.maxRetries {
					atomic.StoreInt32(&status, passed)
					return nil
				}
				lastRetry = now
				return errors.Errorf(
					"Preventing the job from succeeding in try %d, delayed by %d ms", retries, delay/1e6)
			}

			// Setup the test cluster.
			cs := clusterSettings(ctx, test.retryInitVal, test.retryMax)
			args := base.TestServerArgs{
				Settings: cs,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: &TestingKnobs{
						BeforeUpdate: intercept,
						TimeSource:   clock,
					},
				},
			}
			s, _, kvDB := serverutils.StartServer(t, args)
			defer s.Stopper().Stop(ctx)
			// Create and run a dummy job.
			RegisterConstructor(jobspb.TypeImport, func(_ *Job, cs *cluster.Settings) Resumer {
				return FakeResumer{}
			})
			registry = s.JobRegistry().(*Registry)
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
				if atomic.LoadInt32(&status) != running {
					return nil
				}
				return errors.Errorf("waiting for the job to complete")
			})
			require.Equal(t, int32(passed), atomic.LoadInt32(&status))
		})
	}
}

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
			name:       "backoff base setting",
			settingKey: retryBaseSettingKey,
			value:      2 * time.Millisecond,
		},
		{
			name:       "backoff max setting",
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
			// Begin with a very long delay.
			retryBaseSetting.Override(ctx, &cs.SV, time.Hour)
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
