// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	col, err := catalog.MustFindColumnByName(tableDesc, column)
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
	if err := desctestutils.TestingValidateSelf(tableDesc); err != nil {
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// This test directly modifies `system.jobs` and makes over its contents
				// by querying it. We disable the auto span config reconciliation job
				// from getting created so that we don't have to special case it in the
				// test itself.
				ManagerDisableJobCreation: true,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				// This test wants to look at job records.
				DontUseJobs:                           true,
				SkipJobMetricsPollingJobBootstrap:     true,
				SkipMVCCStatisticsJobBootstrap:        true,
				SkipUpdateTableMetadataCacheBootstrap: true,
				SkipSqlActivityFlushJobBootstrap:      true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
			JobsTestingKnobs: NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

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

	writeJob := func(name string, created, finished time.Time, state State, mutOptions mutationOptions) string {
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
				DescriptorMutation_WRITE_ONLY, Direction: descpb.DescriptorMutation_DROP})
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
			`INSERT INTO system.jobs (status, created, job_type) VALUES ($1, $2, 'SCHEMA CHANGE') RETURNING id`, state, created).Scan(&id)
		db.Exec(t, `INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`, id, GetLegacyPayloadKey(), payload)
		db.Exec(t, `INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`, id, GetLegacyProgressKey(), progress)
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
			oldRunningJob := writeJob("old_running", muchEarlier, time.Time{}, StateRunning, mutOptions)
			oldSucceededJob := writeJob("old_succeeded", muchEarlier, muchEarlier.Add(time.Minute), StateSucceeded, mutOptions)
			oldFailedJob := writeJob("old_failed", muchEarlier, muchEarlier.Add(time.Minute),
				StateFailed, mutOptions)
			oldRevertFailedJob := writeJob("old_revert_failed", muchEarlier, muchEarlier.Add(time.Minute),
				StateRevertFailed, mutOptions)
			oldCanceledJob := writeJob("old_canceled", muchEarlier, muchEarlier.Add(time.Minute),
				StateCanceled, mutOptions)
			newRunningJob := writeJob("new_running", earlier, earlier.Add(time.Minute), StateRunning,
				mutOptions)
			newSucceededJob := writeJob("new_succeeded", earlier, earlier.Add(time.Minute), StateSucceeded, mutOptions)
			newFailedJob := writeJob("new_failed", earlier, earlier.Add(time.Minute), StateFailed, mutOptions)
			newRevertFailedJob := writeJob("new_revert_failed", earlier, earlier.Add(time.Minute), StateRevertFailed, mutOptions)
			newCanceledJob := writeJob("new_canceled", earlier, earlier.Add(time.Minute),
				StateCanceled, mutOptions)

			selectJobsQuery := `SELECT id FROM system.jobs WHERE job_type = 'SCHEMA CHANGE' ORDER BY id`
			db.CheckQueryResults(t, selectJobsQuery, [][]string{
				{oldRunningJob}, {oldSucceededJob}, {oldFailedJob}, {oldRevertFailedJob}, {oldCanceledJob},
				{newRunningJob}, {newSucceededJob}, {newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

			testutils.SucceedsSoon(t, func() error {
				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
					return err
				}
				return nil
			})

			db.CheckQueryResults(t, selectJobsQuery, [][]string{
				{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob},
				{newSucceededJob}, {newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

			if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
				t.Fatal(err)
			}
			db.CheckQueryResults(t, selectJobsQuery, [][]string{
				{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newRevertFailedJob}})

			// Delete the revert failed, and running jobs for the next run of the
			// test.
			_, err := sqlDB.Exec(`DELETE FROM system.jobs WHERE id IN ($1, $2, $3, $4)`,
				oldRevertFailedJob, newRevertFailedJob, oldRunningJob, newRunningJob)
			require.NoError(t, err)
		}
	}
}

func TestRegistryGCPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// This test directly modifies `system.jobs` and makes over its contents
				// by querying it. We disable the auto span config reconciliation job
				// from getting created so that we don't have to special case it in the
				// test itself.
				ManagerDisableJobCreation: true,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				// This test wants to count job records.
				DontUseJobs:                           true,
				SkipJobMetricsPollingJobBootstrap:     true,
				SkipUpdateSQLActivityJobBootstrap:     true,
				SkipMVCCStatisticsJobBootstrap:        true,
				SkipUpdateTableMetadataCacheBootstrap: true,
				SkipSqlActivityFlushJobBootstrap:      true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
			JobsTestingKnobs: NewTestingKnobsWithShortIntervals(),
		},
	})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	for i := 0; i < 2*cleanupPageSize+1; i++ {
		payload, err := protoutil.Marshal(&jobspb.Payload{})
		require.NoError(t, err)
		var jobID jobspb.JobID
		db.QueryRow(t,
			`INSERT INTO system.jobs (status, created) VALUES ($1, $2) RETURNING id`,
			StateCanceled, timeutil.Now().Add(-time.Hour)).Scan(&jobID)
		db.Exec(t, `INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
			jobID, GetLegacyPayloadKey(), payload)
	}

	ts := timeutil.Now()
	require.NoError(t, s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(-10*time.Minute)))
	var count int
	db.QueryRow(t, `SELECT count(1) FROM system.jobs WHERE status = $1`, StateCanceled).Scan(&count)
	require.Zero(t, count)
}

func TestRegistryAbandedJobInfoRowsCleanupQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer srv.Stopper().Stop(ctx)

	var (
		now                  = timeutil.Now()
		insertAge            = now.Add(-12 * time.Hour)
		cleanupStartTsTooOld = now.Add(-24 * time.Hour)
		cleanupStartTs       = now.Add(-6 * time.Hour)
	)

	assertAbandonedJobInfoCount := func(expected int) {
		t.Helper()
		var count int
		db.QueryRow(t, "SELECT count(1) FROM system.job_info WHERE job_id NOT IN (SELECT id FROM system.jobs)").Scan(&count)
		require.Equal(t, expected, count)
	}

	assertDeletedCount := func(r gosql.Result, expected int64) {
		t.Helper()
		actual, err := r.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}

	// Insert 10 bad rows.
	db.Exec(t, "INSERT INTO system.job_info (job_id, info_key, value, written) (SELECT id, 'hello', 'world', $1 FROM  generate_series(2000, 2009) as id)",
		insertAge)
	assertAbandonedJobInfoCount(10)

	// Delete with a time before the inserts finds nothing.
	assertDeletedCount(db.Exec(t, AbandonedJobInfoRowsCleanupQuery, cleanupStartTsTooOld, 5),
		0)

	// Delete with limit 5 only deletes 5.
	assertDeletedCount(db.Exec(t, AbandonedJobInfoRowsCleanupQuery, cleanupStartTs, 5),
		5)

	// We should still have 5 left.
	assertAbandonedJobInfoCount(5)

	// Delete with limit 20 deletes 5 more
	assertDeletedCount(db.Exec(t, AbandonedJobInfoRowsCleanupQuery, cleanupStartTs, 20),
		5)

	// We should still have none left.
	assertAbandonedJobInfoCount(0)

	// Delete should find nothing
	assertDeletedCount(db.Exec(t, AbandonedJobInfoRowsCleanupQuery, cleanupStartTs, 5),
		0)
	assertDeletedCount(db.Exec(t, AbandonedJobInfoRowsCleanupQuery, timeutil.Now(), 5),
		0)
}

// TestCreateJobWritesToJobInfo tests that the `Create` methods exposed by the
// registry to create a job write the job payload and progress to the
// system.job_info table alongwith creating a job record in the system.jobs
// table.
func TestCreateJobWritesToJobInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Avoiding jobs to be adopted.
			JobsTestingKnobs: &TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, args)
	ief := s.InternalDB().(isql.DB)
	defer s.Stopper().Stop(ctx)
	r := s.JobRegistry().(*Registry)

	cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				return nil
			},
		}
	}, UsesTenantCostControl)
	defer cleanup()

	record := &Record{
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.RootUserName(),
	}

	verifyPayloadAndProgress := func(t *testing.T, createdJob *Job, txn isql.Txn, expectedPayload jobspb.Payload,
		expectedProgress jobspb.Progress) {
		infoStorage := createdJob.InfoStorage(txn)

		// Verify the payload in the system.job_info is the same as what we read
		// from system.jobs.
		require.NoError(t, infoStorage.Iterate(ctx, LegacyPayloadKey, func(infoKey string, value []byte) error {
			data, err := protoutil.Marshal(&expectedPayload)
			if err != nil {
				panic(err)
			}
			require.Equal(t, data, value)
			return nil
		}))

		// Verify the progress in the system.job_info is the same as what we read
		// from system.jobs.
		require.NoError(t, infoStorage.Iterate(ctx, LegacyProgressKey, func(infoKey string, value []byte) error {
			data, err := protoutil.Marshal(&expectedProgress)
			if err != nil {
				panic(err)
			}
			require.Equal(t, data, value)
			return nil
		}))
	}

	runTests := func(t *testing.T, createdJob *Job) {
		t.Run("verify against system.jobs", func(t *testing.T) {
			require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				countSystemJobs := `SELECT count(*)  FROM system.jobs`
				row, err := txn.QueryRowEx(ctx, "verify-job-query", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, countSystemJobs)
				if err != nil {
					return err
				}
				jobsCount := tree.MustBeDInt(row[0])

				countSystemJobInfo := `SELECT count(*)  FROM system.job_info;`
				row, err = txn.QueryRowEx(ctx, "verify-job-query", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, countSystemJobInfo)
				if err != nil {
					return err
				}
				jobInfoCount := tree.MustBeDInt(row[0])
				require.Equal(t, jobsCount*2, jobInfoCount)

				return nil
			}))
		})

		t.Run("verify against in-memory job", func(t *testing.T) {
			require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				verifyPayloadAndProgress(t, createdJob, txn, createdJob.Payload(), createdJob.Progress())
				return nil
			}))
		})
	}

	t.Run("CreateJobWithTxn", func(t *testing.T) {
		var createdJob *Job
		require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			record.JobID = r.MakeJobID()
			var err error
			createdJob, err = r.CreateJobWithTxn(ctx, *record, record.JobID, txn)
			return err
		}))
		runTests(t, createdJob)
	})

	t.Run("CreateAdoptableJobWithTxn", func(t *testing.T) {
		var createdJob *Job
		require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			record.JobID = r.MakeJobID()
			var err error
			createdJob, err = r.CreateAdoptableJobWithTxn(ctx, *record, record.JobID, txn)
			if err != nil {
				return err
			}
			return nil
		}))
		runTests(t, createdJob)
	})

	t.Run("CreateIfNotExistAdoptableJobWithTxn", func(t *testing.T) {
		tempRecord := Record{
			JobID:    r.MakeJobID(),
			Details:  jobspb.ImportDetails{},
			Progress: jobspb.ImportProgress{},
			Username: username.RootUserName(),
		}

		// loop to verify no errors if create if not exist is called multiple times
		for i := 0; i < 3; i++ {
			err := ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return r.CreateIfNotExistAdoptableJobWithTxn(ctx, tempRecord, txn)
			})
			require.NoError(t, err)
		}

		require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			row, err := txn.QueryRowEx(
				ctx,
				"check if job exists",
				txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"SELECT id FROM system.jobs WHERE id = $1",
				tempRecord.JobID,
			)
			if err != nil {
				return err
			}
			require.NotNil(t, row)
			return nil
		}))
	})
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
					skip.UnderDeadlock(t, "skipping test for batch size ", test.batchSize)
				}

				args := base.TestServerArgs{
					Knobs: base.TestingKnobs{
						// Avoiding jobs to be adopted.
						JobsTestingKnobs: &TestingKnobs{
							DisableAdoptions: true,
						},
						// DisableAdoptions needs this.
						UpgradeManager: &upgradebase.TestingKnobs{
							DontUseJobs:                       true,
							SkipJobMetricsPollingJobBootstrap: true,
						},
						KeyVisualizer: &keyvisualizer.TestingKnobs{
							SkipJobBootstrap: true,
						},
					},
				}

				ctx := context.Background()
				s, sqlDB, _ := serverutils.StartServer(t, args)
				ief := s.InternalDB().(isql.DB)
				tdb := sqlutils.MakeSQLRunner(sqlDB)
				defer s.Stopper().Stop(ctx)
				r := s.JobRegistry().(*Registry)

				cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
					return jobstest.FakeResumer{
						OnResume: func(ctx context.Context) error {
							return nil
						},
					}
				}, UsesTenantCostControl)
				defer cleanup()

				// Create a batch of job specifications.
				var records []*Record
				var jobIDStrings []string
				for i := 0; i < test.batchSize; i++ {
					jobID := r.MakeJobID()
					jobIDStrings = append(jobIDStrings, fmt.Sprintf("%d", jobID))
					records = append(records, &Record{
						JobID:    jobID,
						Details:  jobspb.ImportDetails{},
						Progress: jobspb.ImportProgress{},
						Username: username.RootUserName(),
					})
				}
				jobIdsClause := fmt.Sprint(strings.Join(jobIDStrings, ", "))
				// Create jobs in a batch.
				var jobIDs []jobspb.JobID
				require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					var err error
					jobIDs, err = r.CreateJobsWithTxn(ctx, txn, records)
					return err
				}))
				require.Equal(t, len(jobIDs), test.batchSize)
				tdb.CheckQueryResults(t, fmt.Sprintf("SELECT count(*) FROM [SHOW JOBS] WHERE job_id IN (%s)", jobIdsClause),
					[][]string{{fmt.Sprintf("%d", test.batchSize)}})

				// Ensure that we are also writing the payload and progress to the job_info table.
				tdb.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM system.job_info WHERE info_key = 'legacy_payload' AND job_id IN (%s)`, jobIdsClause),
					[][]string{{fmt.Sprintf("%d", test.batchSize)}})
				tdb.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM system.job_info WHERE info_key = 'legacy_progress' AND job_id IN (%s)`, jobIdsClause),
					[][]string{{fmt.Sprintf("%d", test.batchSize)}})
			}
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
	)

	for _, test := range []struct {
		name      string
		query     string
		queryArgs []interface{}
	}{
		{"remove claims", RemoveClaimsQuery, []interface{}{sid, lim}},
		{"claim jobs", AdoptQuery, []interface{}{sid, iid, lim}},
		{"process claimed jobs", ProcessJobsQuery, []interface{}{sid, iid}},
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

// BenchmarkRunEmptyJob benchmarks how quickly we can create and wait
// for a job that does no work.
func BenchmarkRunEmptyJob(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	defer ResetConstructors()
	cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				return nil
			},
			FailOrCancel: func(ctx context.Context) error {
				return nil
			},
		}
	}, UsesTenantCostControl)
	defer cleanup()

	ctx := context.Background()

	b.Run("run empty job", func(b *testing.B) {
		s := serverutils.StartServerOnly(b, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		r := s.ApplicationLayer().JobRegistry().(*Registry)
		idb := s.ApplicationLayer().InternalDB().(isql.DB)

		for n := 0; n < b.N; n++ {
			jobID := r.MakeJobID()
			require.NoError(b, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
				_, err = r.CreateJobWithTxn(ctx, Record{
					JobID:       jobID,
					Description: "testing",
					Username:    username.RootUserName(),
					Details:     jobspb.ImportDetails{},
					Progress:    jobspb.ImportProgress{},
				}, jobID, txn)
				return err
			}))
			require.NoError(b, r.Run(ctx, []jobspb.JobID{jobID}))
		}
	})
}

func TestDeleteTerminalJobByID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ResetConstructors()

	errCh := make(chan error)
	cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case err := <-errCh:
					return err
				}
			},
			FailOrCancel: func(ctx context.Context) error {
				return nil
			},
		}
	}, UsesTenantCostControl)
	defer cleanup()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: NewTestingKnobsWithShortIntervals(),
		},
	})
	defer s.Stopper().Stop(ctx)

	var (
		r   = s.ApplicationLayer().JobRegistry().(*Registry)
		idb = s.ApplicationLayer().InternalDB().(isql.DB)
	)

	createStartableJob := func() *StartableJob {
		jobID := r.MakeJobID()
		var j *StartableJob
		err := idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return r.CreateStartableJobWithTxn(ctx, &j, jobID, txn, Record{
				JobID:       jobID,
				Description: "testing",
				Username:    username.RootUserName(),
				Details:     jobspb.ImportDetails{},
				Progress:    jobspb.ImportProgress{},
			})
		})
		require.NoError(t, err)
		return j
	}

	assertValidDeletion := func(id jobspb.JobID) {
		var count int
		require.NoError(t, sqlDB.QueryRow("SELECT count(*) FROM system.jobs WHERE id = $1", id).Scan(&count))
		require.Equal(t, 0, count, "expected no job rows")
		sqlDB.QueryRow("SELECT count(*) FROM system.job_info WHERE job_id = $1", id)
		require.Equal(t, 0, count, "expected no job_info rows")
	}

	t.Run("running job is not deleted", func(t *testing.T) {
		j := createStartableJob()
		require.NoError(t, j.Start(ctx))
		require.Error(t, r.DeleteTerminalJobByID(ctx, j.ID()))
		errCh <- nil
		require.NoError(t, j.AwaitCompletion(ctx))
	})
	t.Run("paused job is not deleted", func(t *testing.T) {
		j := createStartableJob()
		require.NoError(t, j.Start(ctx))
		require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return r.PauseRequested(ctx, txn, j.ID(), "test paused")
		}))
		require.Error(t, j.AwaitCompletion(ctx))
		_ = r.WaitForJobs(ctx, []jobspb.JobID{j.ID()})
		require.Error(t, r.DeleteTerminalJobByID(ctx, j.ID()))
	})
	t.Run("successful job is deleted", func(t *testing.T) {
		j := createStartableJob()
		require.NoError(t, j.Start(ctx))
		errCh <- nil
		require.NoError(t, j.AwaitCompletion(ctx))
		_ = r.WaitForJobs(ctx, []jobspb.JobID{j.ID()})
		require.NoError(t, r.DeleteTerminalJobByID(ctx, j.ID()))
		assertValidDeletion(j.ID())
	})
	t.Run("failed job is deleted", func(t *testing.T) {
		j := createStartableJob()
		require.NoError(t, j.Start(ctx))
		errCh <- errors.New("test error")
		require.Error(t, j.AwaitCompletion(ctx))
		_ = r.WaitForJobs(ctx, []jobspb.JobID{j.ID()})
		require.NoError(t, r.DeleteTerminalJobByID(ctx, j.ID()))
		assertValidDeletion(j.ID())
	})
	t.Run("canceled job is deleted", func(t *testing.T) {
		j := createStartableJob()
		require.NoError(t, j.Start(ctx))
		require.NoError(t, j.Cancel(ctx))
		require.Error(t, j.AwaitCompletion(ctx))
		_ = r.WaitForJobs(ctx, []jobspb.JobID{j.ID()})
		require.NoError(t, r.DeleteTerminalJobByID(ctx, j.ID()))
		assertValidDeletion(j.ID())
	})

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
	cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
		var successDone, failureDone int64
		shouldFail := atomic.AddInt64(&shouldFailCounter, 1)%2 == 0
		maybeIncrementCounter := func(check, counter *int64) {
			if atomic.CompareAndSwapInt64(check, 0, 1) {
				atomic.AddInt64(counter, 1)
			}
		}
		return jobstest.FakeResumer{
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
	}, UsesTenantCostControl)
	defer cleanup()

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	intervalBaseSetting.Override(ctx, &settings.SV, 1e6)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: settings,
	})

	idb := s.InternalDB().(isql.DB)

	defer s.Stopper().Stop(ctx)
	r := s.JobRegistry().(*Registry)
	var records []*Record
	const N = 10
	for i := 0; i < N; i++ {
		records = append(records, &Record{
			JobID:       r.MakeJobID(),
			Description: "testing",
			Username:    username.RootUserName(),
			Details:     jobspb.ImportDetails{},
			Progress:    jobspb.ImportProgress{},
		})
	}
	var jobIDs []jobspb.JobID
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		jobIDs, err = r.CreateJobsWithTxn(ctx, txn, records)
		return err
	}))
	require.EqualError(t, r.Run(ctx, jobIDs), "boom")
	// No adoption loops should have been run.
	require.Equal(t, int64(0), r.metrics.AdoptIterations.Count())
	require.Equal(t, int64(N), atomic.LoadInt64(&ran))
	require.Equal(t, int64(N/2), atomic.LoadInt64(&failure))
}

func TestJobIdleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	intervalOverride := time.Millisecond
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
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
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	idb := s.InternalDB().(isql.DB)
	r := s.JobRegistry().(*Registry)

	resumeStartChan := make(chan struct{})
	resumeErrChan := make(chan error)
	defer close(resumeErrChan)
	cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(_ *Job, cs *cluster.Settings) Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				resumeStartChan <- struct{}{}
				return <-resumeErrChan
			},
		}
	}, UsesTenantCostControl)
	defer cleanup()

	currentlyIdle := r.MetricsStruct().JobMetrics[jobspb.TypeImport].CurrentlyIdle

	createJob := func() *Job {
		jobID := r.MakeJobID()
		require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := r.CreateJobWithTxn(ctx, Record{
				Details:  jobspb.ImportDetails{},
				Progress: jobspb.ImportProgress{},
				Username: username.TestUserName(),
			}, jobID, txn)
			return err
		}))
		job, err := r.LoadJob(ctx, jobID)
		require.NoError(t, err)
		<-resumeStartChan
		return job
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	waitUntilState := func(t *testing.T, jobID jobspb.JobID, state State) {
		tdb.CheckQueryResultsRetry(t,
			fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", jobID),
			[][]string{{string(state)}})
	}

	t.Run("MarkIdle", func(t *testing.T) {
		job1 := createJob()
		job2 := createJob()

		require.False(t, r.TestingIsJobIdle(job1.ID()))
		require.EqualValues(t, 2, r.metrics.RunningNonIdleJobs.Value())
		r.MarkIdle(job1, true)
		r.MarkIdle(job2, true)
		require.True(t, r.TestingIsJobIdle(job1.ID()))
		require.Equal(t, int64(2), currentlyIdle.Value())
		require.EqualValues(t, 0, r.metrics.RunningNonIdleJobs.Value())

		// Repeated calls should not increase metric
		r.MarkIdle(job1, true)
		r.MarkIdle(job1, true)
		require.Equal(t, int64(2), currentlyIdle.Value())
		require.EqualValues(t, 0, r.metrics.RunningNonIdleJobs.Value())

		r.MarkIdle(job1, false)
		require.Equal(t, int64(1), currentlyIdle.Value())
		require.False(t, r.TestingIsJobIdle(job1.ID()))
		require.EqualValues(t, 1, r.metrics.RunningNonIdleJobs.Value())
		r.MarkIdle(job2, false)
		require.Equal(t, int64(0), currentlyIdle.Value())
		require.EqualValues(t, 2, r.metrics.RunningNonIdleJobs.Value())

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
				waitUntilState(t, jobID, StatePaused)
			}},
			{"succeeded", func(jobID jobspb.JobID) {
				resumeErrChan <- nil
				waitUntilState(t, jobID, StateSucceeded)
			}},
			{"failed", func(jobID jobspb.JobID) {
				resumeErrChan <- errors.Errorf("error")
				waitUntilState(t, jobID, StateFailed)
			}},
			{"cancel", func(jobID jobspb.JobID) {
				resumeErrChan <- errJobCanceled
				waitUntilState(t, jobID, StateCanceled)
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

// TestDisablingJobAdoptionClearsClaimSessionID tests that jobs adopted by a
// registry for which job adoption has been disabled, have their
// claim_session_id cleared prior to having their context cancelled. This will
// allow other job registries in the cluster to claim and run this job.
func TestDisablingJobAdoptionClearsClaimSessionID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	intervalOverride := time.Millisecond
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: &TestingKnobs{
				DisableAdoptions: true,
				IntervalOverrides: TestingIntervalOverrides{
					Adopt:  &intervalOverride,
					Cancel: &intervalOverride,
				},
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs: true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	r := s.JobRegistry().(*Registry)
	session, err := r.sqlInstance.Session(ctx)
	require.NoError(t, err)

	// Insert a running job with a `claim_session_id` equal to our overridden test
	// session.
	tdb.Exec(t,
		"INSERT INTO system.jobs (id, status, created, claim_session_id) values ($1, $2, $3, $4)",
		1, StateRunning, timeutil.Now(), session.ID(),
	)

	// We expect the adopt loop to clear the claim session since job adoption is
	// disabled on this registry.
	tdb.CheckQueryResultsRetry(t, `SELECT claim_session_id FROM system.jobs WHERE id = 1`, [][]string{{"NULL"}})
}

// TestJobRecordMissingUsername tests that we get an error when
// trying to produce jobs given records without usernames.
func TestJobRecordMissingUsername(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	r := s.JobRegistry().(*Registry)

	// This record is missing a username.
	invalidRecord := Record{
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
	}
	{
		err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := r.CreateAdoptableJobWithTxn(ctx, invalidRecord, 0, txn)
			return err
		})
		assert.EqualError(t, err, "job record missing username; could not make payload")
	}
	{
		err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := r.CreateJobWithTxn(ctx, invalidRecord, 0, txn)
			return err
		})
		assert.EqualError(t, err, "job record missing username; could not make payload")
	}
	{
		err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := r.CreateJobsWithTxn(ctx, txn, []*Record{&invalidRecord})
			return err
		})
		assert.EqualError(t, err, "job record missing username; could not make payload")
	}
}

func TestGetClaimedResumerFromRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	intervalOverride := time.Millisecond
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
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
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	idb := s.InternalDB().(isql.DB)
	r := s.JobRegistry().(*Registry)

	resumeStartChan := make(chan struct{})
	resumeErrChan := make(chan error)
	defer close(resumeErrChan)
	var counter int
	cleanup := TestingRegisterConstructor(jobspb.TypeImport, func(_ *Job, cs *cluster.Settings) Resumer {
		return jobstest.FakeResumer{
			OnResume: func(ctx context.Context) error {
				resumeStartChan <- struct{}{}
				return <-resumeErrChan
			},
			FailOrCancel: func(ctx context.Context) error {
				counter++
				return nil
			},
		}
	}, UsesTenantCostControl)
	defer cleanup()

	createJob := func() *Job {
		jobID := r.MakeJobID()
		require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := r.CreateJobWithTxn(ctx, Record{
				Details:  jobspb.ImportDetails{},
				Progress: jobspb.ImportProgress{},
				Username: username.TestUserName(),
			}, jobID, txn)
			return err
		}))
		job, err := r.LoadJob(ctx, jobID)
		require.NoError(t, err)
		<-resumeStartChan
		return job
	}

	job1 := createJob()
	resumer, err := r.GetResumerForClaimedJob(job1.ID())
	require.NoError(t, err)
	require.NoError(t, resumer.OnFailOrCancel(ctx, nil, nil))
	require.Equal(t, 1, counter)
}
