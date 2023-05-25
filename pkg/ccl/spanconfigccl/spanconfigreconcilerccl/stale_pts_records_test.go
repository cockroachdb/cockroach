// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package spanconfigreconcilerccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type fakeResumer struct {
}

var _ jobs.Resumer = (*fakeResumer)(nil)

func (d *fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return nil
}

func (d *fakeResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	return nil
}

type releasedRecordInfo struct {
	record  ptpb.Record
	deleted bool
	updated hlc.Timestamp
}

func TestBlah(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	releasedPTSRecordChan := make(chan releasedRecordInfo, 1)

	scKnobs := &spanconfig.TestingKnobs{
		SQLWatcherCheckpointNoopsEveryDurationOverride: 100 * time.Millisecond,
	}
	scKnobs.PTSCleanup.ForcePTSRecordCleanup = true
	scKnobs.PTSCleanup.RecordOrphanedPTSRecord = func(record ptpb.Record, deleted bool, updated hlc.Timestamp) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		select {
		case releasedPTSRecordChan <- releasedRecordInfo{record: record, deleted: deleted, updated: updated}:
		case <-timeoutCtx.Done():
			t.Fatal("timeout when reporting released PTS records")
		}
		cancel()
	}

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: scKnobs,
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)

	reg := tc.Server(0).JobRegistry().(*jobs.Registry)
	reg.TestingWrapResumerConstructor(jobspb.TypeChangefeed,
		func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{}
			return &r
		})
	reg.TestingWrapResumerConstructor(jobspb.TypeBackup,
		func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{}
			return &r
		})

	backupJobRecord := jobs.Record{
		Details:  jobspb.BackupDetails{},
		Progress: jobspb.BackupProgress{},
		Username: username.TestUserName(),
	}
	changefeedJobRecord := jobs.Record{
		Details:  jobspb.ChangefeedDetails{},
		Progress: jobspb.ChangefeedProgress{},
		Username: username.TestUserName(),
	}

	createJob := func(jobRecord jobs.Record) (job *jobs.Job) {
		err := ts.InternalDB().(*sql.InternalDB).Txn(ctx, func(ctx2 context.Context, txn isql.Txn) error {
			var err2 error
			job, err2 = reg.CreateJobWithTxn(ctx2, jobRecord, reg.MakeJobID(), txn)
			require.NoError(t, err2)
			return err2
		})
		require.NoError(t, err)
		return job
	}

	createPTSRecord := func(job *jobs.Job, timestamp hlc.Timestamp) (record *ptpb.Record) {
		err := ts.InternalDB().(*sql.InternalDB).Txn(ctx, func(ctx2 context.Context, txn isql.Txn) error {
			ptsStore := ts.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider.WithTxn(txn)
			recordID := uuid.MakeV4()

			record = jobsprotectedts.MakeRecord(
				recordID, int64(job.ID()), timestamp, nil,
				jobsprotectedts.Jobs, ptpb.MakeSchemaObjectsTarget(descpb.IDs{}))
			return ptsStore.Protect(ctx, record)
		})
		require.NoError(t, err)
		return record
	}

	nonChangefeedJob := createJob(backupJobRecord)
	changefeedJob := createJob(changefeedJobRecord)

	checkRecords := func(numRecords int) []releasedRecordInfo {
		var records []releasedRecordInfo
		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for numRecords > 0 {
			select {
			case rec := <-releasedPTSRecordChan:
				jobID, err := jobsprotectedts.DecodeID(rec.record.Meta)
				require.NoError(t, err)
				require.NotEqual(t, jobID, nonChangefeedJob.ID())

				records = append(records, rec)
			case <-timeoutCtx.Done():
				t.Fatal("timeout when receiving PTS records")
			}
			numRecords -= 1
		}
		return records
	}

	// Create stale non-changefeed PTS records and one most recent record.
	// checkRecords() asserts that we do not attempt to clean the
	// stale records up.
	_ = createPTSRecord(nonChangefeedJob, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	_ = createPTSRecord(nonChangefeedJob, hlc.Timestamp{WallTime: timeutil.Now().Add(-24 * time.Hour).UnixNano()})
	_ = createPTSRecord(nonChangefeedJob, hlc.Timestamp{WallTime: timeutil.Now().Add(-128 * 24 * time.Hour).UnixNano()})

	// Setup:
	// 1. currentPTSRecord @ nowTs
	// 2. _ @ nowTS - 1 day
	// 3. _ @ nowTS - 1 day
	// 4. _ @ nowTS - 1 day
	// 5. _ @ nowTS - 5 days
	// 6. _ @ nowTS - 5 days
	nowTs := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	currentPTSRecord := createPTSRecord(changefeedJob, nowTs)
	_ = createPTSRecord(changefeedJob, nowTs.Add((-1*24*time.Hour).Nanoseconds(), 0))
	_ = createPTSRecord(changefeedJob, nowTs.Add((-5*24*time.Hour).Nanoseconds(), 0))
	_ = createPTSRecord(changefeedJob, nowTs.Add((-1*24*time.Hour).Nanoseconds(), 0))
	_ = createPTSRecord(changefeedJob, nowTs.Add((-5*24*time.Hour).Nanoseconds(), 0))
	_ = createPTSRecord(changefeedJob, nowTs.Add((-1*24*time.Hour).Nanoseconds(), 0))
	// New State:
	// 1. currentPTSRecord @ nowTs
	// 2. _ @ nowTS - 1 day
	// 3. _ @ nowTS - 1 day
	records := checkRecords(5)
	numDeleted := 0
	for i := 0; i < len(records); i++ {
		if records[i].deleted {
			numDeleted += 1
		} else {
			require.Equal(t, records[i].record.Timestamp.Add((4*24*time.Hour).Nanoseconds(), 0),
				records[i].updated)
		}
		require.NotEqual(t, records[i].record.ID, currentPTSRecord.ID)
	}
	require.Equal(t, numDeleted, 3)
	// New State:
	// 1. currentPTSRecord @ nowTs
	records = checkRecords(2)
	for i := 0; i < len(records); i++ {
		require.True(t, records[i].deleted)
	}

	// Setup:
	// 1. currentPTSRecord @ nowTs
	// 2. _ @ nowTS - 9 days
	// 3. _ @ nowTS - 9 days
	_ = createPTSRecord(changefeedJob, nowTs.Add((-9*24*time.Hour).Nanoseconds(), 0))
	_ = createPTSRecord(changefeedJob, nowTs.Add((-9*24*time.Hour).Nanoseconds(), 0))
	// New State:
	// 1. currentPTSRecord @ nowTs
	// 2. _ @ nowTS - 5 days
	// 3. _ @ nowTS - 5 days
	records = checkRecords(2)
	for i := 0; i < len(records); i++ {
		require.Equal(t, records[i].record.Timestamp.Add((4*24*time.Hour).Nanoseconds(), 0),
			records[i].updated)
	}
	// New State:
	// 1. currentPTSRecord @ nowTs
	// 2. _ @ nowTS - 1 day
	// 3. _ @ nowTS - 1 day
	records = checkRecords(2)
	for i := 0; i < len(records); i++ {
		require.Equal(t, records[i].record.Timestamp.Add((4*24*time.Hour).Nanoseconds(), 0),
			records[i].updated)
	}
	// New State:
	// 1. currentPTSRecord @ nowTs
	records = checkRecords(2)
	for i := 0; i < len(records); i++ {
		require.True(t, records[i].deleted)
	}

	// Setup:
	// 1. _ @ nowTS + 5 days
	// 2. currentPTSRecord/newlyOutdatedPTSRecord @ nowTs
	newlyOutdatedPTSRecord := currentPTSRecord
	_ = createPTSRecord(changefeedJob, nowTs.Add((5*24*time.Hour).Nanoseconds(), 0))
	// New State:
	// 1. _ @ nowTS + 1 day
	// 2. newlyOutdatedPTSRecord @ nowTs
	records = checkRecords(1)
	require.Equal(t, newlyOutdatedPTSRecord.ID, records[0].record.ID)
	require.Equal(t, newlyOutdatedPTSRecord.Timestamp.Add((4*24*time.Hour).Nanoseconds(), 0), records[0].updated)
	// New State:
	// 1. _ @ nowTS + 1 day
	records = checkRecords(1)
	require.Equal(t, newlyOutdatedPTSRecord.ID, records[0].record.ID)
	require.Equal(t, records[0].deleted, true)
}
