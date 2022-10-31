// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// CompleteStreamIngestion implements eval.StreamIngestManageNew interface.
func (p *planner) CompleteStreamIngestion(
	ctx context.Context, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	return streamingest.CompleteStreamIngestion(ctx, p, ingestionJobID, cutoverTimestamp)
}

// GetStreamIngestionStats implements eval.StreamIngestManageNew interface.
func (p *planner) GetStreamIngestionStats(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, error) {
	return streamingest.GetStreamIngestionStats(ctx, p, ingestionJobID)
}

// StartReplicationStream implements eval.ReplicationStreamManager interface.
func (p *planner) StartReplicationStream(
	ctx context.Context, tenantID uint64,
) (streaming.StreamID, error) {
	return streamproducer.StartReplicationStreamJob(ctx, p, tenantID)
}

// HeartbeatReplicationStream implements eval.ReplicationStreamManager interface.
func (p *planner) HeartbeatReplicationStream(
	ctx context.Context, streamID streaming.StreamID, frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	return streamproducer.HeartbeatReplicationStream(ctx, p, streamID, frontier)
}

// StreamPartition implements eval.ReplicationStreamManager interface.
func (p *planner) StreamPartition(
	ctx context.Context, streamID streaming.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	return streamproducer.StreamPartition(ctx, p, p.SessionData(), streamID, opaqueSpec)
}

// GetReplicationStreamSpec implements eval.ReplicationStreamManager interface.
func (p *planner) GetReplicationStreamSpec(
	ctx context.Context, streamID streaming.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	return streamproducer.GetReplicationStreamSpec(ctx, p, streamID)
}

// CompleteReplicationStream implements eval.ReplicationStreamManager interface.
func (p *planner) CompleteReplicationStream(
	ctx context.Context, streamID streaming.StreamID, successfulIngestion bool,
) error {
	return streamproducer.CompleteReplicationStream(ctx, p, streamID, successfulIngestion)
}
