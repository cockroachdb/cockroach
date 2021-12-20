// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type replicationStreamManagerImpl struct{}

// CompleteStreamIngestionHook hooks a CompleteStreamIngestion implementation
// inside streamingccl package.
var CompleteStreamIngestionHook func(evalCtx *tree.EvalContext, txn *kv.Txn, jobID int, cutoverTimestamp hlc.Timestamp) error

// CompleteStreamIngestion implements ReplicationStreamManager interface.
func (r replicationStreamManagerImpl) CompleteStreamIngestion(
	evalCtx *tree.EvalContext, txn *kv.Txn, jobID int, cutoverTimestamp hlc.Timestamp,
) error {
	if CompleteStreamIngestionHook == nil {
		return errors.New("CompleteStreamIngestionHook is not registered")
	}
	return CompleteStreamIngestionHook(evalCtx, txn, jobID, cutoverTimestamp)
}

// StartReplicationStreamHook hooks an StartReplicationStream implementation inside streamingccl package.
var StartReplicationStreamHook func(evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64) (streaming.StreamID, error)

// StartReplicationStream implements ReplicationStreamManager interface.
func (r replicationStreamManagerImpl) StartReplicationStream(
	evalCtx *tree.EvalContext, txn *kv.Txn, tenantID uint64,
) (streaming.StreamID, error) {
	if StartReplicationStreamHook == nil {
		return streaming.InvalidStreamID, errors.New("StartReplicationStreamHook is not registered")
	}
	return StartReplicationStreamHook(evalCtx, txn, tenantID)
}

// UpdateReplicationStreamProgressHook hooks an UpdateReplicationStreamProgress implementation
// inside streamingccl package.
var UpdateReplicationStreamProgressHook func(evalCtx *tree.EvalContext, streamID streaming.StreamID,
	frontier hlc.Timestamp, txn *kv.Txn) (jobspb.StreamReplicationStatus, error)

// UpdateReplicationStreamProgress implements ReplicationStreamManager interface.
func (r replicationStreamManagerImpl) UpdateReplicationStreamProgress(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, frontier hlc.Timestamp, txn *kv.Txn,
) (jobspb.StreamReplicationStatus, error) {
	if UpdateReplicationStreamProgressHook == nil {
		return jobspb.StreamReplicationStatus{},
			errors.New("UpdateReplicationStreamProgress is not registered")
	}
	return UpdateReplicationStreamProgressHook(evalCtx, streamID, frontier, txn)
}

// StreamPartitionHook hooks a StreamPartition implementation inside streamingccl package.
var StreamPartitionHook func(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, opaqueSpec []byte,
) (tree.ValueGenerator, error)

// StreamPartition returns a value generator which yields events for the specified partition.
// opaqueSpec contains streampb.PartitionSpec protocol message.
// streamID specifies the streaming job this partition belongs too.
func (r replicationStreamManagerImpl) StreamPartition(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, opaqueSpec []byte,
) (tree.ValueGenerator, error) {
	if StreamPartitionHook == nil {
		return nil, errors.New("StreamPartitionHook is not registered")
	}
	return StreamPartitionHook(evalCtx, streamID, opaqueSpec)
}
