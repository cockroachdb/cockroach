// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// StreamingManager stores methods that help with streaming job.
type StreamingManager interface {
	ReplicationStreamManager
	StreamIngestManager
}

// ReplicationStreamManager represents a collection of APIs that streaming replication supports
// on the production side.
type ReplicationStreamManager interface {
	// StartReplicationStream starts a stream replication job for the specified tenant on the producer side.
	StartReplicationStream(
		ctx context.Context,
		tenantID uint64,
	) (streaming.StreamID, error)

	// HeartbeatReplicationStream sends a heartbeat to the replication stream producer, indicating
	// consumer has consumed until the given 'frontier' timestamp. This updates the producer job
	// progress and extends its life, and the new producer progress will be returned.
	// If 'frontier' is hlc.MaxTimestamp, returns the producer progress without updating it.
	HeartbeatReplicationStream(
		ctx context.Context,
		streamID streaming.StreamID,
		frontier hlc.Timestamp,
	) (streampb.StreamReplicationStatus, error)

	// StreamPartition starts streaming replication on the producer side for the partition specified
	// by opaqueSpec which contains serialized streampb.StreamPartitionSpec protocol message and
	// returns a value generator which yields events for the specified partition.
	StreamPartition(
		ctx context.Context,
		streamID streaming.StreamID,
		opaqueSpec []byte,
	) (ValueGenerator, error)

	// GetReplicationStreamSpec gets a stream replication spec on the producer side.
	GetReplicationStreamSpec(
		ctx context.Context,
		streamID streaming.StreamID,
	) (*streampb.ReplicationStreamSpec, error)

	// CompleteReplicationStream completes a replication stream job on the producer side.
	// 'successfulIngestion' indicates whether the stream ingestion finished successfully and
	// determines the fate of the producer job, succeeded or canceled.
	CompleteReplicationStream(
		ctx context.Context,
		streamID streaming.StreamID,
		successfulIngestion bool,
	) error
}

// StreamIngestManager represents a collection of APIs that streaming replication supports
// on the ingestion side.
type StreamIngestManager interface {
	// CompleteStreamIngestion signals a running stream ingestion job to complete on the consumer side.
	CompleteStreamIngestion(
		ctx context.Context,
		ingestionJobID jobspb.JobID,
		cutoverTimestamp hlc.Timestamp,
	) error

	// GetStreamIngestionStats gets a statistics summary for a stream ingestion job.
	GetStreamIngestionStats(ctx context.Context, ingestionJobID jobspb.JobID) (*streampb.StreamIngestionStats, error)
}
