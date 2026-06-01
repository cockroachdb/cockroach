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

	// If the source tenant of a PCR stream is the system tenant, then the
	// producer job will also be copied to the destination tenant, along with the
	// associated PTS. After cutover, this blocks span config reconciliation since
	// a tenant cannot lay a PTS on spans that another tenant owns. The producer
	// job of course is not needed on the destination tenant, so we check for a
	// mismatched cluster ID here and fast-exit the job.
	if p.job.Payload().CreationClusterID != execCfg.NodeInfo.LogicalClusterID() {
		return jobs.MarkAsPermanentJobError(errors.Newf(
			"replication stream %d belongs to cluster %s, cannot resume on cluster %s",
			p.job.ID(), p.job.Payload().CreationClusterID, execCfg.NodeInfo.LogicalClusterID(),
		))
	}

	// Fire the timer immediately to start an initial progress check
	p.timer.Reset(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.timer.Ch():
			p.timer.Reset(crosscluster.StreamReplicationStreamLivenessTrackFrequency.Get(execCfg.SV()))

			progress, err := replicationutils.LoadReplicationProgress(ctx, execCfg.InternalDB, p.job.ID())
			if knobs := execCfg.StreamingTestingKnobs; knobs != nil && knobs.AfterResumerJobLoad != nil {
				err = knobs.AfterResumerJobLoad(err)
			}
			if err != nil {
				if jobs.HasJobNotFoundError(err) {
					return errors.Wrapf(err, "replication stream %d failed loading producer job progress", p.job.ID())
				}
				log.Dev.Errorf(ctx,
					"replication stream %d failed loading producer job progress (retrying): %v", p.job.ID(), err)
				continue
			}
			if progress == nil {
				log.Dev.Errorf(ctx, "replication stream %d cannot find producer job progress (retrying)", p.job.ID())
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

				details := p.job.Details().(jobspb.StreamReplicationDetails)
				if details.TenantID.IsSet() {
					isLive, err := checkTenantLiveness(ctx, execCfg, details)
					if err != nil {
						log.Dev.Errorf(ctx,
							"replication stream %d failed checking tenant liveness (retrying): %v",
							p.job.ID(), err)
						continue
					}
					if !isLive {
						return errors.Newf("source tenant %d is no longer online", details.TenantID)
					}
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

// checkTenantLiveness validates that the tenant this producer is streaming is currently online.
//
// TODO(art):
//
// This current approach of checking the liveness within the producer job's poller has some core
// correctness issues that are non-trivial to address. The crux of the problem is that this approach
// leaves a window of time where the rangefeeds in the producer's underlying event streams can emit
// non MVCC-compliant values while the tenant is offline, but before the poller calling this
// function notices. This leaves open the possibility that a cutover could happen against a resolved
// timestamp which includes these non-compliant values, putting us in the relm of undefined behavior.
// In practice, at present, the non-compliant values we would see will be the result of revert
// range commands driven by cutover events.
//
// We additionally considered using an additional rangefeed in EventStream which watches the tenant
// record span, and tears down the stream when a tenant goes offline. This is essentially just a faster
// version of the current approach. While it would mitigate the probability that we have issues by
// making the producer-side reaction happen within a smaller window, it is not fundamentally
// more correct.
//
// For a more robust approach, we considered two options:
//
//  1. Make rangefeeds trustworthy.
//     Currently, rangefeeds are not robust enough in their reporting of non-MVCC compliant values.
//     While in theory, we could use the knowledge that we have seen non-MVCC compliant values to
//     avoid cutting over to an invalid checkpoint, the reporting we currently get from rangefeeds
//     is not robust enough to be viable. Namely, seeing this information requires a happy path
//     connected rangefeed, and we can potentially lose this knowledge during, for example, a
//     spurious network blip. We can also lose this knowlege in cases where we need to do a catchup
//     scan after a non-MVCC compliant value has been emitted.
//
//  2. Disallow rangefeeds on offline tenants.
//     Conceptually this approach is relatively straightforward. We are essentially moving the logic
//     we have here currently to within rangefeeds, namely to avoid non-MVCC compliance in the first
//     place. To pull this off we would need an additional mechanism. We considered using leases, or
//     potentially having a registry of active rangefeeds which can control such feeds during a
//     tenant transitioning it's liveness.
func checkTenantLiveness(
	ctx context.Context, execCfg *sql.ExecutorConfig, details jobspb.StreamReplicationDetails,
) (bool, error) {
	var tenantInfo *mtinfopb.TenantInfo
	err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, t isql.Txn) error {
		var err error
		tenantInfo, err = sql.GetTenantRecordByID(ctx, t, details.TenantID, execCfg.Settings)
		return err
	})
	if err != nil {
		return false, err
	} else if tenantInfo.ServiceMode == mtinfopb.ServiceModeNone ||
		tenantInfo.ServiceMode == mtinfopb.ServiceModeStopping {
		return false, nil
	}
	return true, nil
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
