// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// completeStreamIngestion terminates the stream as of specified time.
func completeStreamIngestion(
	evalCtx *tree.EvalContext,
	txn *kv.Txn,
	streamID streaming.StreamID,
	cutoverTimestamp hlc.Timestamp,
) error {
	// Get the job payload for job_id.
	const jobsQuery = `SELECT progress FROM system.jobs WHERE id=$1 FOR UPDATE`
	row, err := evalCtx.Planner.QueryRowEx(evalCtx.Context,
		"get-stream-ingestion-job-metadata",
		txn, sessiondata.NodeUserSessionDataOverride, jobsQuery, streamID)
	if err != nil {
		return err
	}
	// If an entry does not exist for the provided job_id we return an
	// error.
	if row == nil {
		return errors.Newf("job %d: not found in system.jobs table", streamID)
	}

	progress, err := jobs.UnmarshalProgress(row[0])
	if err != nil {
		return err
	}
	var sp *jobspb.Progress_StreamIngest
	var ok bool
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return errors.Newf("job %d: not of expected type StreamIngest", streamID)
	}

	// Check that the supplied cutover time is a valid one.
	// TODO(adityamaru): This will change once we allow a future cutover time to
	// be specified.
	hw := progress.GetHighWater()
	if hw == nil || hw.Less(cutoverTimestamp) {
		var highWaterTimestamp hlc.Timestamp
		if hw != nil {
			highWaterTimestamp = *hw
		}
		return errors.Newf("cannot cutover to a timestamp %s that is after the latest resolved time"+
			" %s for job %d", cutoverTimestamp.String(), highWaterTimestamp.String(), streamID)
	}

	// Reject setting a cutover time, if an earlier request to cutover has already
	// been set.
	// TODO(adityamaru): This should change in the future, a user should be
	// allowed to correct their cutover time if the process of reverting the job
	// has not started.
	if !sp.StreamIngest.CutoverTime.IsEmpty() {
		return errors.Newf("cutover timestamp already set to %s, "+
			"job %d is in the process of cutting over", sp.StreamIngest.CutoverTime.String(), streamID)
	}

	// Update the sentinel being polled by the stream ingestion job to
	// check if a complete has been signaled.
	sp.StreamIngest.CutoverTime = cutoverTimestamp
	progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
	progressBytes, err := protoutil.Marshal(progress)
	if err != nil {
		return err
	}
	updateJobQuery := `UPDATE system.jobs SET progress=$1 WHERE id=$2`
	_, err = evalCtx.Planner.QueryRowEx(evalCtx.Context,
		"set-stream-ingestion-job-metadata", txn,
		sessiondata.NodeUserSessionDataOverride, updateJobQuery, progressBytes, streamID)
	return err
}

// startReplicationStreamJob initializes a replication stream producer job on the source cluster that
// 1. Tracks the liveness of the replication stream consumption
// 2. TODO(casper): Updates the protected timestamp for spans being replicated
func startReplicationStreamJob(
	evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64,
) (streaming.StreamID, error) {
	execConfig := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	hasAdminRole, err := evalCtx.SessionAccessor.HasAdminRole(evalCtx.Ctx())

	if err != nil {
		return streaming.InvalidStreamID, err
	}

	if !hasAdminRole {
		return streaming.InvalidStreamID, errors.New("admin role required to start stream replication jobs")
	}

	registry := execConfig.JobRegistry
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&evalCtx.Settings.SV)
	ptsID := uuid.MakeV4()
	jr := makeProducerJobRecord(registry, tenantID, timeout, evalCtx.SessionData().User(), ptsID)
	if _, err := registry.CreateAdoptableJobWithTxn(evalCtx.Ctx(), jr, jr.JobID, txn); err != nil {
		return streaming.InvalidStreamID, err
	}

	ptp := execConfig.ProtectedTimestampProvider
	statementTime := hlc.Timestamp{
		WallTime: evalCtx.GetStmtTimestamp().UnixNano(),
	}

	deprecatedSpansToProtect := roachpb.Spans{*makeTenantSpan(tenantID)}
	targetToProtect := ptpb.MakeRecordTenantsTarget([]roachpb.TenantID{roachpb.MakeTenantID(tenantID)})

	pts := jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), statementTime,
		deprecatedSpansToProtect, jobsprotectedts.Jobs, targetToProtect)

	if err := ptp.Protect(evalCtx.Ctx(), txn, pts); err != nil {
		return streaming.InvalidStreamID, err
	}
	return streaming.StreamID(jr.JobID), nil
}

// updateReplicationStreamProgress updates the job progress for an active replication
// stream specified by 'streamID' and returns error if the stream is no longer active.
func updateReplicationStreamProgress(
	ctx context.Context,
	expiration time.Time,
	ptsProvider protectedts.Provider,
	registry *jobs.Registry,
	streamID streaming.StreamID,
	ts hlc.Timestamp,
	txn *kv.Txn,
) (status jobspb.StreamReplicationStatus, err error) {
	const useReadLock = false
	err = registry.UpdateJobWithTxn(ctx, jobspb.JobID(streamID), txn, useReadLock,
		func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if md.Status == jobs.StatusRunning {
				status.StreamStatus = jobspb.StreamReplicationStatus_STREAM_ACTIVE
			} else if md.Status == jobs.StatusPaused {
				status.StreamStatus = jobspb.StreamReplicationStatus_STREAM_PAUSED
			} else if md.Status.Terminal() {
				status.StreamStatus = jobspb.StreamReplicationStatus_STREAM_INACTIVE
			} else {
				status.StreamStatus = jobspb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY
			}
			// Skip checking PTS record in cases that it might already be released
			if status.StreamStatus != jobspb.StreamReplicationStatus_STREAM_ACTIVE &&
				status.StreamStatus != jobspb.StreamReplicationStatus_STREAM_PAUSED {
				return nil
			}

			ptsID := *md.Payload.GetStreamReplication().ProtectedTimestampRecord
			ptsRecord, err := ptsProvider.GetRecord(ctx, txn, ptsID)
			if err != nil {
				return err
			}
			status.ProtectedTimestamp = &ptsRecord.Timestamp
			if status.StreamStatus != jobspb.StreamReplicationStatus_STREAM_ACTIVE {
				return nil
			}

			if shouldUpdatePTS := ptsRecord.Timestamp.Less(ts); shouldUpdatePTS {
				if err = ptsProvider.UpdateTimestamp(ctx, txn, ptsID, ts); err != nil {
					return err
				}
				status.ProtectedTimestamp = &ts
			}

			if p := md.Progress; expiration.After(p.GetStreamReplication().Expiration) {
				p.GetStreamReplication().Expiration = expiration
				ju.UpdateProgress(p)
			}
			return nil
		})

	if jobs.HasJobNotFoundError(err) || testutils.IsError(err, "not found in system.jobs table") {
		status.StreamStatus = jobspb.StreamReplicationStatus_STREAM_INACTIVE
		err = nil
	}

	return status, err
}

// heartbeatReplicationStream updates replication stream progress and advances protected timestamp
// record to the specified frontier.
func heartbeatReplicationStream(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, frontier hlc.Timestamp, txn *kv.Txn,
) (jobspb.StreamReplicationStatus, error) {

	execConfig := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&evalCtx.Settings.SV)
	expirationTime := timeutil.Now().Add(timeout)

	return updateReplicationStreamProgress(evalCtx.Ctx(),
		expirationTime, execConfig.ProtectedTimestampProvider, execConfig.JobRegistry, streamID, frontier, txn)
}
