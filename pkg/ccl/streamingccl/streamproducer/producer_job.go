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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func makeTenantSpan(tenantID uint64) roachpb.Span {
	tenID := roachpb.MustMakeTenantID(tenantID)
	return keys.MakeTenantSpan(tenID)
}

func makeProducerJobRecord(
	registry *jobs.Registry,
	tenantInfo *mtinfopb.TenantInfo,
	expirationWindow time.Duration,
	user username.SQLUsername,
	ptsID uuid.UUID,
) jobs.Record {
	tenantID := tenantInfo.ID
	tenantName := tenantInfo.Name
	currentTime := timeutil.Now()
	expiration := currentTime.Add(expirationWindow)
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("History Retention for Physical Replication of %s", tenantName),
		Username:    user,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecordID: ptsID,
			Spans:                      []roachpb.Span{makeTenantSpan(tenantID)},
			TenantID:                   roachpb.MustMakeTenantID(tenantID),
			ExpirationWindow:           expirationWindow,
		},
		Progress: jobspb.StreamReplicationProgress{
			Expiration: expiration,
		},
	}
}

func makeJobRecordForClusterPTSRetention(
	registry *jobs.Registry,
	expirationWindow time.Duration,
	user username.SQLUsername,
	desc string,
	ptsID uuid.UUID,
) jobs.Record {
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("History Retention for %s", desc),
		Username:    user,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecordID: ptsID,
			ExpirationWindow:           expirationWindow,
		},
		Progress: jobspb.StreamReplicationProgress{
			Expiration: timeutil.Now().Add(expirationWindow),
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
					return errors.Wrapf(err, "%s failed loading job progress", p.jobDescription())
				}
				log.Errorf(ctx,
					"%s failed loading job progress (retrying): %v", p.jobDescription(), err)
				continue
			}
			if progress == nil {
				log.Errorf(ctx, "%s cannot find job progress (retrying)", p.jobDescription())
				continue
			}

			switch progress.StreamIngestionStatus {
			case jobspb.StreamReplicationProgress_FINISHED_SUCCESSFULLY:
				// Retain the pts until the expiration period elapses to allow for fast
				// fail back.
				if progress.Expiration.After(p.timeSource.Now()) {
					continue
				}
				if err := p.removeJobFromTenantRecord(ctx, execCfg); err != nil {
					return err
				}
				return p.releaseProtectedTimestamp(ctx, execCfg)
			case jobspb.StreamReplicationProgress_FINISHED_UNSUCCESSFULLY:
				return errors.New("destination cluster job finished unsuccessfully")
			case jobspb.StreamReplicationProgress_NOT_FINISHED:
				expiration := progress.Expiration
				log.VEventf(ctx, 1, "checking if expiration %s timed out", expiration)
				if expiration.Before(p.timeSource.Now()) {
					return errors.Errorf("%s timed out", p.jobDescription())
				}
			default:
				return errors.New("unrecognized stream ingestion status")
			}
		}
	}
}

func (p *producerJobResumer) jobDescription() redact.SafeString {
	tenantID := p.job.Details().(jobspb.StreamReplicationDetails).TenantID
	if !tenantID.IsSet() {
		return redact.SafeString(fmt.Sprintf("history retention %d", p.job.ID()))
	}
	return redact.SafeString(fmt.Sprintf("replication producer stream %d", p.job.ID()))
}

// OnFailOrCancel implements jobs.Resumer interface
func (p *producerJobResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	jobExec := execCtx.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()

	if err := p.removeJobFromTenantRecord(ctx, execCfg); err != nil {
		return err
	}

	return p.releaseProtectedTimestamp(ctx, execCfg)
}

func (p *producerJobResumer) removeJobFromTenantRecord(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) error {
	tenantID := p.job.Details().(jobspb.StreamReplicationDetails).TenantID
	if !tenantID.IsSet() {
		return nil
	}
	jobID := p.job.ID()
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantRecord, err := sql.GetTenantRecordByID(ctx, txn, tenantID, execCfg.Settings)
		if err != nil {
			if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
				// Tenant is already gone. Nothing more to do here.
				return nil
			}
			return err
		}

		ourIdx := -1
		for i, jid := range tenantRecord.PhysicalReplicationProducerJobIDs {
			if jobID == jid {
				ourIdx = i
				break
			}
		}
		if ourIdx != -1 {
			l := len(tenantRecord.PhysicalReplicationProducerJobIDs)
			tenantRecord.PhysicalReplicationProducerJobIDs[ourIdx] = tenantRecord.PhysicalReplicationProducerJobIDs[l-1]
			tenantRecord.PhysicalReplicationProducerJobIDs = tenantRecord.PhysicalReplicationProducerJobIDs[:l-1]
		}
		if err := sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, tenantRecord); err != nil {
			return err
		}
		return err
	})
}

// CollectProfile implements jobs.Resumer interface
func (p *producerJobResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
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
