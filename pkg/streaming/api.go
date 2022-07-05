// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streaming

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// StreamID is the ID of a replication stream.
type StreamID int64

// SafeValue implements the redact.SafeValue interface.
func (j StreamID) SafeValue() {}

// InvalidStreamID is the zero value for StreamID corresponding to no stream.
const InvalidStreamID StreamID = 0

// GetReplicationStreamManagerHook is the hook to get access to the producer side replication APIs.
// Used by builtin functions to trigger streaming replication.
var GetReplicationStreamManagerHook func(evalCtx *eval.Context) (ReplicationStreamManager, error)

// GetStreamIngestManagerHook is the hook to get access to the ingestion side replication APIs.
// Used by builtin functions to trigger streaming replication.
var GetStreamIngestManagerHook func(evalCtx *eval.Context) (StreamIngestManager, error)

// ReplicationStreamManager represents a collection of APIs that streaming replication supports
// on the production side.
type ReplicationStreamManager interface {
	// StartReplicationStream starts a stream replication job for the specified tenant on the producer side.
	StartReplicationStream(
		evalCtx *eval.Context,
		txn *kv.Txn,
		tenantID uint64,
	) (StreamID, error)

	// HeartbeatReplicationStream sends a heartbeat to the replication stream producer, indicating
	// consumer has consumed until the given 'frontier' timestamp. This updates the producer job
	// progress and extends its life, and the new producer progress will be returned.
	// If 'frontier' is hlc.MaxTimestamp, returns the producer progress without updating it.
	HeartbeatReplicationStream(
		evalCtx *eval.Context,
		streamID StreamID,
		frontier hlc.Timestamp,
		txn *kv.Txn) (streampb.StreamReplicationStatus, error)

	// StreamPartition starts streaming replication on the producer side for the partition specified
	// by opaqueSpec which contains serialized streampb.StreamPartitionSpec protocol message and
	// returns a value generator which yields events for the specified partition.
	StreamPartition(
		evalCtx *eval.Context,
		streamID StreamID,
		opaqueSpec []byte,
	) (eval.ValueGenerator, error)

	// GetReplicationStreamSpec gets a stream replication spec on the producer side.
	GetReplicationStreamSpec(
		evalCtx *eval.Context,
		txn *kv.Txn,
		streamID StreamID,
	) (*streampb.ReplicationStreamSpec, error)

	// CompleteReplicationStream completes a replication stream job on the producer side.
	// 'successfulIngestion' indicates whether the stream ingestion finished successfully and
	// determines the fate of the producer job, succeeded or canceled.
	CompleteReplicationStream(
		evalCtx *eval.Context, txn *kv.Txn, streamID StreamID, successfulIngestion bool,
	) error
}

// StreamIngestManager represents a collection of APIs that streaming replication supports
// on the ingestion side.
type StreamIngestManager interface {
	// CompleteStreamIngestion signals a running stream ingestion job to complete on the consumer side.
	CompleteStreamIngestion(
		evalCtx *eval.Context,
		txn *kv.Txn,
		ingestionJobID jobspb.JobID,
		cutoverTimestamp hlc.Timestamp,
	) error

	// GetStreamIngestionStats gets a statistics summary for a stream ingestion job.
	GetStreamIngestionStats(
		evalCtx *eval.Context,
		txn *kv.Txn,
		ingestionJobID jobspb.JobID,
	) (*streampb.StreamIngestionStats, error)
}

// GetReplicationStreamManager returns a ReplicationStreamManager if a CCL binary is loaded.
func GetReplicationStreamManager(evalCtx *eval.Context) (ReplicationStreamManager, error) {
	if GetReplicationStreamManagerHook == nil {
		return nil, errors.New("replication streaming requires a CCL binary")
	}
	return GetReplicationStreamManagerHook(evalCtx)
}

// GetStreamIngestManager returns a StreamIngestManager if a CCL binary is loaded.
func GetStreamIngestManager(evalCtx *eval.Context) (StreamIngestManager, error) {
	if GetReplicationStreamManagerHook == nil {
		return nil, errors.New("replication streaming requires a CCL binary")
	}
	return GetStreamIngestManagerHook(evalCtx)
}
