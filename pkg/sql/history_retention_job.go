// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var historyRetentionExpirationPollInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.history_retention_job.poll_interval",
	"controls how frequently the history retention job checks if the PTS record should be released",
	time.Minute,
)

func ExtendHistoryRetention(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, jobID jobspb.JobID,
) error {
	execConfig := evalCtx.Planner.ExecutorConfig().(*ExecutorConfig)
	registry := execConfig.JobRegistry
	return registry.UpdateJobWithTxn(ctx, jobID, txn, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		historyProgress := md.Progress.GetDetails().(*jobspb.Progress_HistoryRetentionProgress).HistoryRetentionProgress
		historyProgress.LastHeartbeatTime = timeutil.Now()
		ju.UpdateProgress(md.Progress)
		return nil
	})
}

// StartHistoryRetentionJob creates a cluster-level protected timestamp and a
// job that owns it.
func StartHistoryRetentionJob(
	ctx context.Context,
	evalCtx *eval.Context,
	txn isql.Txn,
	desc string,
	protectTime hlc.Timestamp,
	expiration time.Duration,
) (jobspb.JobID, error) {
	execConfig := evalCtx.Planner.ExecutorConfig().(*ExecutorConfig)
	registry := execConfig.JobRegistry
	ptsID := uuid.MakeV4()

	jr := jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("History Retention for %s", desc),
		Username:    evalCtx.SessionData().User(),
		Details: jobspb.HistoryRetentionDetails{
			ProtectedTimestampRecordID: ptsID,
			ExpirationWindow:           expiration,
		},
		Progress: jobspb.HistoryRetentionProgress{
			LastHeartbeatTime: timeutil.Now(),
		},
	}

	targetToProtect := ptpb.MakeClusterTarget()
	allTablesSpan := roachpb.Span{
		Key:    execConfig.Codec.TablePrefix(0),
		EndKey: execConfig.Codec.TablePrefix(math.MaxUint32).PrefixEnd(),
	}
	ptp := execConfig.ProtectedTimestampProvider.WithTxn(txn)
	pts := jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), protectTime,
		[]roachpb.Span{allTablesSpan}, jobsprotectedts.Jobs, targetToProtect)

	if err := ptp.Protect(ctx, pts); err != nil {
		return 0, err
	}
	if _, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn); err != nil {
		return 0, err
	}
	return jr.JobID, nil
}

type historyRetentionResumer struct {
	job *jobs.Job
}

// Resume is part of the jobs.Resumer interface.
func (h *historyRetentionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	execCfg := execCtx.(JobExecContext).ExecCfg()
	exprWindow := h.job.Payload().Details.(*jobspb.Payload_HistoryRetentionDetails).HistoryRetentionDetails.ExpirationWindow

	var t timeutil.Timer
	t.Reset(0)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			t.Read = true
			t.Reset(historyRetentionExpirationPollInterval.Get(execCfg.SV()))
			p, err := jobs.LoadJobProgress(ctx, execCfg.InternalDB, h.job.ID())
			if err != nil {
				if jobs.HasJobNotFoundError(err) {
					return errors.Wrapf(err, "job progress not found")
				}
				log.Errorf(ctx,
					"failed loading job progress (retrying): %v", err)
				continue
			}
			if p == nil {
				log.Errorf(ctx, "job progress not found (retrying)")
				continue
			}
			historyProgress := p.GetDetails().(*jobspb.Progress_HistoryRetentionProgress).HistoryRetentionProgress
			expiration := historyProgress.LastHeartbeatTime.Add(exprWindow)
			if expiration.Before(timeutil.Now()) {
				return errors.Errorf("reached history protection expiration")
			}
		}
	}
}

// OnFailOrCancel implements jobs.Resumer interface
func (h *historyRetentionResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	execCfg := execCtx.(JobExecContext).ExecCfg()
	ptr := h.job.Details().(jobspb.HistoryRetentionDetails).ProtectedTimestampRecordID
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		err := execCfg.ProtectedTimestampProvider.WithTxn(txn).Release(ctx, ptr)
		// In case that a retry happens, the record might have been released.
		if errors.Is(err, exec.ErrNotFound) {
			return nil
		}
		return err
	})
}

// CollectProfile implements jobs.Resumer interface
func (h *historyRetentionResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeHistoryRetention,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &historyRetentionResumer{
				job: job,
			}
		},
		jobs.UsesTenantCostControl,
	)
}
