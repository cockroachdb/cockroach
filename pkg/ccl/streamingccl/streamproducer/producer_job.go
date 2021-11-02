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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func makeProducerJobRecord(
	registry *jobs.Registry, tenantID uint64, timeout time.Duration, username security.SQLUsername,
) (jobspb.JobID, jobs.Record) {
	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID))
	spans := []*roachpb.Span{{Key: prefix, EndKey: prefix.PrefixEnd()}}
	jr := jobs.Record{
		Description: fmt.Sprintf("stream replication for tenant %d", tenantID),
		Username:    username,
		Details: jobspb.StreamReplicationDetails{
			Spans: spans,
		},
		Progress: jobspb.StreamReplicationProgress{
			Expiration: timeutil.Now().Add(timeout),
		},
	}
	return registry.MakeJobID(), jr
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
	return nil
}

// doStartReplicationStream initializes a replication stream producer job on the source cluster that
// 1. Tracks the liveness of the replication stream consumption
// 2. TODO(casper): Updates the protected timestamp for spans being replicated
func doStartReplicationStream(
	evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64,
) (streaming.StreamID, error) {
	hasAdminRole, err := evalCtx.SessionAccessor.HasAdminRole(evalCtx.Ctx())

	if err != nil {
		return streaming.InvalidStreamID, err
	}

	if !hasAdminRole {
		return streaming.InvalidStreamID, errors.New("admin role required to start stream replication jobs")
	}

	registry := evalCtx.ExecConfigAccessor.JobRegistry().(*jobs.Registry)
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&evalCtx.Settings.SV)
	jobID, jr := makeProducerJobRecord(registry, tenantID, timeout, evalCtx.SessionData().User())
	if _, err := registry.CreateAdoptableJobWithTxn(evalCtx.Ctx(), jr, jobID, txn); err != nil {
		return streaming.InvalidStreamID, err
	}
	return streaming.StreamID(jobID), nil
}

func init() {
	streamingccl.StartReplicationStreamHook = doStartReplicationStream
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
