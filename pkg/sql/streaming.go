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

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// The following functions are injected by `streamproducer` and `streamingest`
// packages.

// CompleteStreamIngestion implements eval.StreamIngestManager interface.
var CompleteStreamIngestion func(
	ctx context.Context,
	planner JobExecContext,
	ingestionJobID jobspb.JobID,
	cutoverTimestamp hlc.Timestamp,
) error

// GetStreamIngestionStats implements eval.StreamIngestManager interface.
var GetStreamIngestionStats func(
	ctx context.Context, planner JobExecContext, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, error)

// StartReplicationStream implements eval.ReplicationStreamManager interface.
var StartReplicationStream func(
	ctx context.Context, planner PlanHookState, tenantID uint64,
) (streaming.StreamID, error)

// HeartbeatReplicationStream implements eval.ReplicationStreamManager interface.
var HeartbeatReplicationStream func(
	ctx context.Context,
	planner PlanHookState,
	streamID streaming.StreamID,
	frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error)

// StreamPartition implements eval.ReplicationStreamManager interface.
var StreamPartition func(
	ctx context.Context,
	planner eval.Planner,
	sd *sessiondata.SessionData,
	streamID streaming.StreamID,
	opaqueSpec []byte,
) (eval.ValueGenerator, error)

// GetReplicationStreamSpec implements eval.ReplicationStreamManager interface.
var GetReplicationStreamSpec func(
	ctx context.Context, planner JobExecContext, streamID streaming.StreamID,
) (*streampb.ReplicationStreamSpec, error)

// CompleteReplicationStream implements eval.ReplicationStreamManager interface.
var CompleteReplicationStream func(
	ctx context.Context,
	planner PlanHookState,
	streamID streaming.StreamID,
	successfulIngestion bool,
) error

// CompleteStreamIngestion implements eval.StreamIngestManager interface.
func (p *planner) CompleteStreamIngestion(
	ctx context.Context, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	return CompleteStreamIngestion(ctx, p, ingestionJobID, cutoverTimestamp)
}

// GetStreamIngestionStats implements eval.StreamIngestManager interface.
func (p *planner) GetStreamIngestionStats(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, error) {
	return GetStreamIngestionStats(ctx, p, ingestionJobID)
}

// StartReplicationStream implements eval.ReplicationStreamManager interface.
func (p *planner) StartReplicationStream(
	ctx context.Context, tenantID uint64,
) (streaming.StreamID, error) {
	return StartReplicationStream(ctx, p, tenantID)
}

// HeartbeatReplicationStream implements eval.ReplicationStreamManager interface.
func (p *planner) HeartbeatReplicationStream(
	ctx context.Context, streamID streaming.StreamID, frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	return HeartbeatReplicationStream(ctx, p, streamID, frontier)
}

// StreamPartition implements eval.ReplicationStreamManager interface.
func (p *planner) StreamPartition(
	ctx context.Context, streamID streaming.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	return StreamPartition(ctx, p, p.SessionData(), streamID, opaqueSpec)
}

// GetReplicationStreamSpec implements eval.ReplicationStreamManager interface.
func (p *planner) GetReplicationStreamSpec(
	ctx context.Context, streamID streaming.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	return GetReplicationStreamSpec(ctx, p, streamID)
}

// CompleteReplicationStream implements eval.ReplicationStreamManager interface.
func (p *planner) CompleteReplicationStream(
	ctx context.Context, streamID streaming.StreamID, successfulIngestion bool,
) error {
	return CompleteReplicationStream(ctx, p, streamID, successfulIngestion)
}

func init() {
	streamclient.TestTableCreator = func(
		ctx context.Context,
		parentID, id descpb.ID,
		schema string,
		privileges *catpb.PrivilegeDescriptor,
	) (*tabledesc.Mutable, error) {
		return CreateTestTableDescriptor(ctx, parentID, id, schema, privileges, nil /* txn */, nil /* collection */)
	}
}
