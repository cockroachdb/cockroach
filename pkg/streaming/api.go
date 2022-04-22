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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// StreamID is the ID of a replication stream.
type StreamID int64

// SafeValue implements the redact.SafeValue interface.
func (j StreamID) SafeValue() {}

// InvalidStreamID is the zero value for StreamID corresponding to no stream.
const InvalidStreamID StreamID = 0

// GetReplicationStreamManagerHook is the hook to get access to the replication API.
// Used by builtin functions to trigger streaming replication.
var GetReplicationStreamManagerHook func(evalCtx *tree.EvalContext) (ReplicationStreamManager, error)

// ReplicationStreamManager represents a collection of APIs that streaming replication supports.
type ReplicationStreamManager interface {
	// CompleteStreamIngestion signals a running stream ingestion job to complete on the consumer side.
	CompleteStreamIngestion(
		evalCtx *tree.EvalContext,
		streamID StreamID,
		cutoverTimestamp hlc.Timestamp,
	) error

	// StartReplicationStream starts a stream replication job for the specified tenant on the producer side.
	StartReplicationStream(
		evalCtx *tree.EvalContext,
		txn *kv.Txn,
		tenantID uint64,
	) (StreamID, error)

	// UpdateReplicationStreamProgress updates the progress of a replication stream on the producer side.
	UpdateReplicationStreamProgress(
		evalCtx *tree.EvalContext,
		streamID StreamID,
		frontier hlc.Timestamp,
		txn *kv.Txn) (streampb.StreamReplicationStatus, error)

	// StreamPartition starts streaming replication on the producer side for the partition specified
	// by opaqueSpec which contains serialized streampb.StreamPartitionSpec protocol message.
	StreamPartition(
		evalCtx *tree.EvalContext,
		streamID StreamID,
		opaqueSpec []byte,
	) (tree.ValueGenerator, error)

	// GetReplicationStreamSpec gets a stream replication spec on the producer side.
	GetReplicationStreamSpec(
		evalCtx *tree.EvalContext,
		txn *kv.Txn,
		streamID StreamID,
	) (*streampb.ReplicationStreamSpec, error)

	// CompleteReplicationStream completes a replication stream job on the producer side.
	CompleteReplicationStream(
		evalCtx *tree.EvalContext, txn *kv.Txn, streamID StreamID,
	) error
}

// GetReplicationStreamManager returns a ReplicationStreamManager if a CCL binary is loaded.
func GetReplicationStreamManager(evalCtx *tree.EvalContext) (ReplicationStreamManager, error) {
	if GetReplicationStreamManagerHook == nil {
		return nil, errors.New("replication streaming requires a CCL binary")
	}
	return GetReplicationStreamManagerHook(evalCtx)
}
