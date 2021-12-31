// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type replicationStreamManagerImpl struct{}

// CompleteStreamIngestion implements ReplicationStreamManager interface.
func (r replicationStreamManagerImpl) CompleteStreamIngestion(
	evalCtx *tree.EvalContext,
	txn *kv.Txn,
	streamID streaming.StreamID,
	cutoverTimestamp hlc.Timestamp,
) error {
	return completeStreamIngestion(evalCtx, txn, streamID, cutoverTimestamp)
}

// StartReplicationStream implements ReplicationStreamManager interface.
func (r replicationStreamManagerImpl) StartReplicationStream(
	evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64,
) (streaming.StreamID, error) {
	return startReplicationStreamJob(evalCtx, txn, tenantID)
}

// UpdateReplicationStreamProgress implements ReplicationStreamManager interface.
func (r replicationStreamManagerImpl) UpdateReplicationStreamProgress(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, frontier hlc.Timestamp, txn *kv.Txn,
) (streampb.StreamReplicationStatus, error) {
	return heartbeatReplicationStream(evalCtx, streamID, frontier, txn)
}

// StreamPartition returns a value generator which yields events for the specified partition.
// opaqueSpec contains streampb.PartitionSpec protocol message.
// streamID specifies the streaming job this partition belongs too.
func (r replicationStreamManagerImpl) StreamPartition(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, opaqueSpec []byte,
) (tree.ValueGenerator, error) {
	return streamPartition(evalCtx, streamID, opaqueSpec)
}

// GetReplicationStreamSpec returns a specification for a replication stream which consumer
// uses to start the replication.
func (r replicationStreamManagerImpl) GetReplicationStreamSpec(
	evalCtx *tree.EvalContext, txn *kv.Txn, streamID streaming.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	return getReplicationStreamSpec(evalCtx, txn, streamID)
}

func init() {
	streaming.GetReplicationStreamManagerHook = func() (streaming.ReplicationStreamManager, error) {
		return &replicationStreamManagerImpl{}, nil
	}
}
