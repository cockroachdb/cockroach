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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func makeTenantSpan(tenantID uint64) *roachpb.Span {
	prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenantID))
	return &roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

func makeProducerJobRecord(
	registry *jobs.Registry,
	tenantID uint64,
	timeout time.Duration,
	user username.SQLUsername,
	ptsID uuid.UUID,
) jobs.Record {
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("stream replication for tenant %d", tenantID),
		Username:    user,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecordID: ptsID,
			Spans:                      []*roachpb.Span{makeTenantSpan(tenantID)},
			TenantID:                   roachpb.MustMakeTenantID(tenantID),
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

// Releases the protected timestamp record associated with the producer
// job if it exists.
func (p *producerJobResumer) releaseProtectedTimestamp(
	ctx context.Context, executorConfig *sql.ExecutorConfig,
) error {
	ptr := p.job.Details().(jobspb.StreamReplicationDetails).ProtectedTimestampRecordID
	return executorConfig.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		err := executorConfig.ProtectedTimestampProvider.WithTxn(txn).Release(ctx, ptr)
		// In case that a retry happens, the record might have been released.
		if errors.Is(err, exec.ErrNotFound) {
			return nil
		}
		return err
	})
}

// Resume is part of the jobs.Resumer interface.
func (p *producerJobResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()

	// Fire the timer immediately to start an initial progress check
	p.timer.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.timer.Ch():
			p.timer.MarkRead()
			p.timer.Reset(streamingccl.StreamReplicationStreamLivenessTrackFrequency.Get(execCfg.SV()))
			j, err := execCfg.JobRegistry.LoadJob(ctx, p.job.ID())
			if err != nil {
				return err
			}

			prog := j.Progress()
			switch prog.GetStreamReplication().StreamIngestionStatus {
			case jobspb.StreamReplicationProgress_FINISHED_SUCCESSFULLY:
				return p.releaseProtectedTimestamp(ctx, execCfg)
			case jobspb.StreamReplicationProgress_FINISHED_UNSUCCESSFULLY:
				return j.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
					ju.UpdateStatus(jobs.StatusCancelRequested)
					return nil
				})
			case jobspb.StreamReplicationProgress_NOT_FINISHED:
				// Check if the job timed out.
				if prog.GetStreamReplication().Expiration.Before(p.timeSource.Now()) {
					return errors.Errorf("replication stream %d timed out", p.job.ID())
				}
			default:
				return errors.New("unrecognized stream ingestion status")
			}
		}
	}
}

// OnFailOrCancel implements jobs.Resumer interface
func (p *producerJobResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()
	// Releases the protected timestamp record.
	return p.releaseProtectedTimestamp(ctx, execCfg)
}

func init() {
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
		jobs.UsesTenantCostControl,
	)
}
