// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rspb"
)

type LogEngine struct {
	c rspb.Codec
	e LLEngine

	buf []byte // scratch buf
}

func (le *LogEngine) Append(ctx context.Context, b LLBatchBase, entry LogEntry) error {
	// TODO(raft-store-efficiency): see `(*logstore.LogStore).StoreEntries`. We
	// want to either incorporate logstore into this package or (at least at
	// first) call it here.
	panic("implement me")
}

func (le *LogEngine) Create(
	ctx context.Context, b LLSyncedBatch, req CreateRequest,
) (rspb.CreateOp, error) {
	lid := rspb.LogID(1)        // TODO(tbg): allocate
	wix := rspb.WAGIndex(123)   // TODO(tbg): allocate
	ridx := rspb.RaftIndex(456) // TODO(tbg): from metadata

	op := rspb.CreateOp{
		RangeID:   req.RangeID,
		ReplicaID: req.ReplicaID,
		LogID:     lid,
		WAGIndex:  wix,
	}

	data, err := op.Marshal()
	if err != nil {
		return rspb.CreateOp{}, err
	}

	b.Put(ctx, le.c.Encode(nil, rspb.KeyKindRaftLogInit, op.RangeID, op.LogID, ridx), data)

	return op, nil
}
