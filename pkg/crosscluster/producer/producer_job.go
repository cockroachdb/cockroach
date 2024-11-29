// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
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
)

// makeTenantSpan returns the span of all tables that make up the tenant, which
// for non-system tenants is their whole span and for the system tenant is the
// span in which its tables exist.
func makeTenantSpan(tenantID uint64) roachpb.Span {
	// TODO(dt): remove conditional if we make MakeTenantSpan do this.
	if tenantID == 1 {
		return roachpb.Span{
			Key: keys.TableDataMin, EndKey: keys.TableDataMax,
		}
	}
	tenID := roachpb.MustMakeTenantID(tenantID)
	return keys.MakeTenantSpan(tenID)
}

func makeProducerJobRecord(
	registry *jobs.Registry,
	tenantInfo *mtinfopb.TenantInfo,
	expirationWindow time.Duration,
	user username.SQLUsername,
	ptsID uuid.UUID,
	assumeSucceeded bool,
) jobs.Record {
	tenantID := tenantInfo.ID
	tenantName := tenantInfo.Name
	currentTime := timeutil.Now()
	expiration := currentTime.Add(expirationWindow)
	status := jobspb.StreamReplicationProgress_NOT_FINISHED
	if assumeSucceeded {
		status = jobspb.StreamReplicationProgress_FINISHED_SUCCESSFULLY
	}
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
			Expiration:            expiration,
			StreamIngestionStatus: status,
		},
	}
}

func makeProducerJobRecordForLogicalReplication(
	registry *jobs.Registry,
	expirationWindow time.Duration,
	user username.SQLUsername,
	ptsID uuid.UUID,
	spans []roachpb.Span,
	tableIDs []uint32,
	desc string,
) jobs.Record {
	expiration := timeutil.Now().Add(expirationWindow)
	status := jobspb.StreamReplicationProgress_NOT_FINISHED
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("History Retention for Logical Replication of %s", desc),
		Username:    user,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecordID: ptsID,
			Spans:                      spans,
			ExpirationWindow:           expirationWindow,
			TableIDs:                   tableIDs,
		},
		Progress: jobspb.StreamReplicationProgress{
			Expiration:            expiration,
			StreamIngestionStatus: status,
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
			p.timer.Reset(crosscluster.StreamReplicationStreamLivenessTrackFrequency.Get(execCfg.SV()))
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

	if err := p.removeJobFromTenantRecord(ctx, execCfg); err != nil {
		return err
	}

	details := p.job.Details().(jobspb.StreamReplicationDetails)
	if err := replicationutils.UnlockLDRTables(ctx, execCfg, details.TableIDs, p.job.ID()); err != nil {
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
