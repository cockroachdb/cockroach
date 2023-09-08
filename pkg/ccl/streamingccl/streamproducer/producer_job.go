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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func makeTenantSpan(tenantID uint64) roachpb.Span {
	tenID := roachpb.MustMakeTenantID(tenantID)
	return keys.MakeTenantSpan(tenID)
}

func makeProducerJobRecord(
	registry *jobs.Registry,
	tenantInfo *mtinfopb.TenantInfo,
	timeout time.Duration,
	user username.SQLUsername,
	ptsID uuid.UUID,
) jobs.Record {
	tenantID := tenantInfo.ID
	tenantName := tenantInfo.Name
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("Physical replication stream producer for %q (%d)", tenantName, tenantID),
		Username:    user,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecordID: ptsID,
			Spans:                      []roachpb.Span{makeTenantSpan(tenantID)},
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
			progress, err := replicationutils.LoadReplicationProgress(ctx, execCfg.InternalDB, p.job.ID())
			if knobs := execCfg.StreamingTestingKnobs; knobs != nil && knobs.AfterResumerJobLoad != nil {
				err = knobs.AfterResumerJobLoad(err)
			}
			if err != nil {
				if jobs.HasJobNotFoundError(err) {
					return errors.Wrapf(err, "replication stream %d failed loading producer job progress", p.job.ID())
				}
				log.Errorf(ctx,
					"replication stream %d failed loading producer job progress (retrying): %v", p.job.ID(), err)
				continue
			}
			if progress == nil {
				log.Errorf(ctx, "replication stream %d cannot find producer job progress (retrying)", p.job.ID())
				continue
			}

			switch progress.StreamIngestionStatus {
			case jobspb.StreamReplicationProgress_FINISHED_SUCCESSFULLY:
				return p.releaseProtectedTimestamp(ctx, execCfg)
			case jobspb.StreamReplicationProgress_FINISHED_UNSUCCESSFULLY:
				return errors.New("destination cluster job finished unsuccessfully")
			case jobspb.StreamReplicationProgress_NOT_FINISHED:
				expiration := progress.Expiration
				log.VEventf(ctx, 1, "checking if stream replication expiration %s timed out", expiration)
				if expiration.Before(p.timeSource.Now()) {
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
		jobspb.TypeReplicationStreamProducer,
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
