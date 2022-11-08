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
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/errors"
)

// StreamID is the ID of a replication stream.
type StreamID = streampb.StreamID

// GetReplicationStreamManagerHook is the hook to get access to the producer side replication APIs.
// Used by builtin functions to trigger streaming replication.
var GetReplicationStreamManagerHook func(ctx context.Context, evalCtx *eval.Context, txn *kv.Txn) (eval.ReplicationStreamManager, error)

// GetStreamIngestManagerHook is the hook to get access to the ingestion side replication APIs.
// Used by builtin functions to trigger streaming replication.
var GetStreamIngestManagerHook func(ctx context.Context, evalCtx *eval.Context, txn *kv.Txn) (eval.StreamIngestManager, error)

// GetReplicationStreamManager returns a ReplicationStreamManager if a CCL binary is loaded.
func GetReplicationStreamManager(
	ctx context.Context, evalCtx *eval.Context, txn *kv.Txn,
) (eval.ReplicationStreamManager, error) {
	if GetReplicationStreamManagerHook == nil {
		return nil, errors.New("replication streaming requires a CCL binary")
	}
	return GetReplicationStreamManagerHook(ctx, evalCtx, txn)
}

// GetStreamIngestManager returns a StreamIngestManager if a CCL binary is loaded.
func GetStreamIngestManager(
	ctx context.Context, evalCtx *eval.Context, txn *kv.Txn,
) (eval.StreamIngestManager, error) {
	if GetReplicationStreamManagerHook == nil {
		return nil, errors.New("replication streaming requires a CCL binary")
	}
	return GetStreamIngestManagerHook(ctx, evalCtx, txn)
}
