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
	"fmt"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func makeTenantSpan(tenantID uint64) *roachpb.Span {
	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID))
	return &roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

func makeProducerJobRecord(
	registry *jobs.Registry,
	tenantID uint64,
	timeout time.Duration,
	username security.SQLUsername,
	ptsID uuid.UUID,
) jobs.Record {
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("stream replication for tenant %d", tenantID),
		Username:    username,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecord: &ptsID,
			Spans:                    []*roachpb.Span{makeTenantSpan(tenantID)},
		},
		Progress: jobspb.StreamReplicationProgress{
			Expiration: timeutil.Now().Add(timeout),
		},
	}
}

type producerJobResumer struct {
	job *jobs.Job

	timeSource timeutil.TimeSource
	timer      timeutil.TimerI
}

// Resume is part of the jobs.Resumer interface.
func (p *producerJobResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()
	isTimedOut := func(job *jobs.Job) bool {
		progress := p.job.Progress()
		return progress.GetStreamReplication().Expiration.Before(p.timeSource.Now())
	}
	trackFrequency := streamingccl.StreamReplicationStreamLivenessTrackFrequency.Get(execCfg.SV())
	if isTimedOut(p.job) {
		return errors.Errorf("replication stream %d timed out", p.job.ID())
	}
	p.timer.Reset(trackFrequency)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.timer.Ch():
			p.timer.MarkRead()
			p.timer.Reset(trackFrequency)
			j, err := execCfg.JobRegistry.LoadJob(ctx, p.job.ID())
			if err != nil {
				return err
			}
			if isTimedOut(j) {
				return errors.Errorf("replication stream %d timed out", p.job.ID())
			}
		}
	}
}

// OnFailOrCancel implements jobs.Resumer interface
func (p *producerJobResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()

	// Releases the protected timestamp record.
	ptr := p.job.Details().(jobspb.StreamReplicationDetails).ProtectedTimestampRecord
	return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := execCfg.ProtectedTimestampProvider.Release(ctx, txn, *ptr)
		// In case that a retry happens, the record might have been released.
		if errors.Is(err, exec.ErrNotFound) {
			return nil
		}
		return err
	})
}

// doStartReplicationStream initializes a replication stream producer job on the source cluster that
// 1. Tracks the liveness of the replication stream consumption
// 2. TODO(casper): Updates the protected timestamp for spans being replicated
func doStartReplicationStream(
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
	pts := jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), statementTime,
		[]roachpb.Span{*makeTenantSpan(tenantID)}, jobsprotectedts.Jobs)

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

func doUpdateReplicationStreamProgress(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, frontier hlc.Timestamp, txn *kv.Txn,
) (jobspb.StreamReplicationStatus, error) {

	execConfig := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&evalCtx.Settings.SV)
	expirationTime := timeutil.Now().Add(timeout)

	return updateReplicationStreamProgress(evalCtx.Ctx(),
		expirationTime, execConfig.ProtectedTimestampProvider, execConfig.JobRegistry, streamID, frontier, txn)
}

func init() {
	streamingccl.StartReplicationStreamHook = doStartReplicationStream
	streamingccl.UpdateReplicationStreamProgressHook = doUpdateReplicationStreamProgress
	jobs.RegisterConstructor(
		jobspb.TypeStreamReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			ts := timeutil.DefaultTimeSource{}
			return &producerJobResumer{
				job:        job,
				timeSource: ts,
				timer:      ts.NewTimer(),
			}
		},
	)
}
