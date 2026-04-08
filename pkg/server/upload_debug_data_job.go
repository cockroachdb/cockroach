// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type uploadDebugDataResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &uploadDebugDataResumer{}

func (r *uploadDebugDataResumer) Resume(
	ctx context.Context, execCtx interface{},
) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()

	if execCfg.UploadDebugDataFanOut == nil {
		return errors.New("upload debug data is not available (system tenant required)")
	}

	details := r.job.Details().(jobspb.UploadDebugDataDetails)

	r.setStatus(ctx, "Creating upload session")

	clusterID, nodeCount, err := execCfg.UploadDebugDataClusterInfo(ctx)
	if err != nil {
		return errors.Wrap(err, "getting cluster info")
	}

	client := newUploadServerClientForRPC(details.ServerUrl, details.ApiKey, 5*time.Minute)
	defer func() { _ = client.closeGCS() }()

	if details.ReuploadSessionId != "" {
		if err := client.reuploadSession(
			ctx, details.ReuploadSessionId, "reupload via job", details.NodeIds,
		); err != nil {
			return errors.Wrap(err, "reopening upload session")
		}
		log.Ops.Infof(ctx, "upload session reopened: %s", client.sessionID)
	} else {
		labels := details.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		if err := client.createSession(
			ctx, clusterID, nodeCount, details.Redact, labels,
		); err != nil {
			return errors.Wrap(err, "creating upload session")
		}
		log.Ops.Infof(ctx, "upload session created: %s", client.sessionID)
	}

	if err := client.initGCSClient(ctx); err != nil {
		return errors.Wrap(err, "initializing GCS client")
	}

	// Save session ID in progress so it's visible.
	if err := r.updateProgress(ctx, func(p *jobspb.UploadDebugDataProgress) {
		p.SessionId = client.sessionID
	}); err != nil {
		return err
	}

	r.setStatus(ctx, "Uploading cluster data")

	// --- Phase 1: Cluster-wide data (via SQL) ---
	// For the job-based path, cluster-level data collection is
	// delegated to the sync RPC if needed, or handled by the
	// per-node fan-out. The job focuses on the per-node fan-out
	// which is the bulk of the work.
	r.setFraction(ctx, 0.05)

	// --- Phase 2: Per-node fan-out ---
	r.setStatus(ctx, "Uploading node data")

	nodeReq := &serverpb.UploadNodeDebugDataRequest{
		ServerUrl:              details.ServerUrl,
		SessionId:              client.sessionID,
		UploadToken:            client.uploadToken,
		Redact:                 details.Redact,
		CpuProfSeconds:         details.CpuProfSeconds,
		IncludeRangeInfo:       details.IncludeRangeInfo,
		IncludeGoroutineStacks: details.IncludeGoroutineStacks,
		GcsAccessToken:         client.gcsAccessToken,
		GcsBucket:              client.gcsBucket,
		GcsPrefix:              client.gcsPrefix,
	}

	var mu syncutil.Mutex
	var totalArtifacts int32
	var nodesSucceeded, nodesFailed int32
	var nodesCompleted []int32
	var uploadErrors []string
	totalNodes := nodeCount
	if len(details.NodeIds) > 0 {
		totalNodes = len(details.NodeIds)
	}

	progressFn := func(nodeID roachpb.NodeID, resp *serverpb.UploadNodeDebugDataResponse) {
		mu.Lock()
		defer mu.Unlock()
		totalArtifacts += resp.ArtifactsUploaded
		if len(resp.Errors) > 0 {
			nodesFailed++
			for _, e := range resp.Errors {
				uploadErrors = append(uploadErrors, fmt.Sprintf("node %d: %s", nodeID, e))
			}
		} else {
			nodesSucceeded++
			nodesCompleted = append(nodesCompleted, int32(nodeID))
		}
		done := nodesSucceeded + nodesFailed
		fraction := 0.05 + float32(done)/float32(totalNodes)*0.90

		_ = r.updateProgress(ctx, func(p *jobspb.UploadDebugDataProgress) {
			p.TotalNodes = int32(totalNodes)
			p.NodesCompleted = nodesSucceeded
			p.NodesFailed = nodesFailed
			p.ArtifactsUploaded = totalArtifacts
			p.Errors = uploadErrors
		})
		r.setFraction(ctx, fraction)
		r.setStatus(ctx, fmt.Sprintf(
			"Uploading node data: %d/%d nodes complete (%d artifacts)",
			done, totalNodes, totalArtifacts,
		))
	}

	errorFn := func(nodeID roachpb.NodeID, err error) {
		mu.Lock()
		defer mu.Unlock()
		nodesFailed++
		uploadErrors = append(uploadErrors,
			fmt.Sprintf("node %d: %s", nodeID, err))
		log.Dev.Warningf(ctx, "upload failed for node %d: %v", nodeID, err)
	}

	if err := execCfg.UploadDebugDataFanOut(
		ctx, nodeReq, details.NodeIds, progressFn, errorFn,
	); err != nil {
		return errors.Wrap(err, "fan-out upload")
	}

	// --- Phase 3: Complete session ---
	r.setStatus(ctx, "Completing upload session")
	r.setFraction(ctx, 0.95)

	if err := client.completeSession(
		ctx, int(totalArtifacts), nodesCompleted,
	); err != nil {
		log.Dev.Warningf(ctx, "failed to complete upload session: %v", err)
	}

	if err := r.updateProgress(ctx, func(p *jobspb.UploadDebugDataProgress) {
		p.TotalNodes = int32(totalNodes)
		p.NodesCompleted = nodesSucceeded
		p.NodesFailed = nodesFailed
		p.ArtifactsUploaded = totalArtifacts
		p.Errors = uploadErrors
	}); err != nil {
		return err
	}

	r.setFraction(ctx, 1.0)
	r.setStatus(ctx, fmt.Sprintf(
		"Upload complete: %d artifacts from %d nodes (session %s)",
		totalArtifacts, nodesSucceeded, client.sessionID,
	))

	if nodesFailed > 0 {
		return errors.Newf(
			"%d node(s) failed during upload (session %s); use --reupload-session to retry",
			nodesFailed, client.sessionID,
		)
	}
	return nil
}

func (r *uploadDebugDataResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	log.Ops.Infof(ctx, "upload debug data job failed or canceled: %v", jobErr)
	return nil
}

func (r *uploadDebugDataResumer) CollectProfile(
	ctx context.Context, execCtx interface{},
) error {
	return nil
}

// setFraction is a convenience wrapper around FractionProgressed.
func (r *uploadDebugDataResumer) setFraction(ctx context.Context, f float32) {
	if err := r.job.NoTxn().FractionProgressed(
		ctx, jobs.FractionUpdater(f),
	); err != nil {
		log.Dev.Warningf(ctx, "error updating fraction: %v", err)
	}
}

// setStatus is a convenience wrapper around UpdateStatusMessage.
func (r *uploadDebugDataResumer) setStatus(ctx context.Context, msg string) {
	if err := r.job.NoTxn().UpdateStatusMessage(
		ctx, jobs.StatusMessage(msg),
	); err != nil {
		log.Dev.Warningf(ctx, "error updating status: %v", err)
	}
}

// updateProgress atomically updates the job's progress details.
func (r *uploadDebugDataResumer) updateProgress(
	ctx context.Context,
	fn func(p *jobspb.UploadDebugDataProgress),
) error {
	return r.job.NoTxn().FractionProgressed(
		ctx,
		func(ctx context.Context, details jobspb.ProgressDetails) float32 {
			if p, ok := details.(*jobspb.UploadDebugDataProgress); ok {
				fn(p)
			}
			return r.job.FractionCompleted()
		},
	)
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeUploadDebugData,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &uploadDebugDataResumer{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
