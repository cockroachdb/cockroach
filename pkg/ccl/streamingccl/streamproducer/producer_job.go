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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func producerJob(
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
			Expiration: timeutil.Now().Add(timeout)},
	}
	return registry.MakeJobID(), jr
}

// Initialize a replication stream producer job on the source cluster that
// 1. Tracks the liveness of the replication stream consumption
// 2. TODO(casper): Updates the protected timestamp for spans being replicated
func doInitStream(evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64) (jobspb.JobID, error) {
	registry := evalCtx.ExecConfigAccessor.JobRegistry().(*jobs.Registry)
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&evalCtx.Settings.SV)

	jobID, jr := producerJob(registry, tenantID, timeout, evalCtx.Username)
	if _, err := registry.CreateAdoptableJobWithTxn(evalCtx.Ctx(), jr, jobID, txn); err != nil {
		return 0, err
	}
	return jobID, nil
}

type producerJobResumer struct {
	job *jobs.Job

	timeSource timeutil.TimeSource
}

// Resume is part of the jobs.Resumer interface.
func (p *producerJobResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()
	isTimedOut := func(job *jobs.Job) bool {
		progress := p.job.Progress()
		return progress.GetStreamReplication().Expiration.Before(p.timeSource.Now())
	}
	if isTimedOut(p.job) {
		return errors.Errorf("replication stream %d timed out", p.job.ID())
	}

	t := p.timeSource.NewTimer()
	t.Reset(streamingccl.DefaultJobLivenessTrackingFrequency)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.Ch():
			t.MarkRead()
			t.Reset(streamingccl.DefaultJobLivenessTrackingFrequency)
			if j, err := execCfg.JobRegistry.LoadJob(ctx, p.job.ID()); err != nil {
				return err
			} else if isTimedOut(j) {
				return errors.Errorf("replication stream %d timed out", p.job.ID())
			}
		}
	}
}

func (p *producerJobResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	streamingccl.RegisterStreamAPI("init_stream",
		func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			tenantID := uint64(*args[0].(*tree.DInt))
			jobID, err := doInitStream(evalCtx, evalCtx.Txn, tenantID)
			return tree.NewDInt(tree.DInt(jobID)), err
		})
	jobs.RegisterConstructor(
		jobspb.TypeStreamReplication,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &producerJobResumer{
				job:        job,
				timeSource: timeutil.DefaultTimeSource{},
			}
		},
	)
}
