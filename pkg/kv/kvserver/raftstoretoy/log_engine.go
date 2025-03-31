// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/rscodec"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftstoretoy/wagpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// LogEngine represents the interface to the engine used for write-ahead
// logging. All operations on this engine are immediately durable.
type LogEngine interface {
	// Append appends a LogEntry. The WAGIndex returned is equal to the previously
	// assigned one unless the LogEntry contains a Split or Merge.
	Append(ctx context.Context, id rspb.FullLogID, entry LogEntry) error

	// Create initializes a new log via a snapshot. The assigned LogID and
	// WAGIndex are returned.
	Create(ctx context.Context, req CreateRequest) (rspb.FullLogID, WAGIndex, error)

	// Destroy marks a replica for destruction.
	//Destroy(ctx context.Context, id rscodec.FullLogID, req Destroy) (WAGIndex, error)
}

type logEng struct {
	c rspb.Codec
	e LLEngine

	buf []byte // scratch buf
}

func (le *logEng) Append(ctx context.Context, id rspb.FullLogID, entry LogEntry) error {
	//TODO implement me
	panic("implement me")
}

func (le *logEng) Create(
	ctx context.Context, req CreateRequest,
) (rspb.FullLogID, WAGIndex, error) {
	b := le.e.NewBatch()
	defer b.Close()

	lid := rspb.LogID(1) // TODO(tbg): allocate
	wix := WAGIndex(123) // TODO(tbg): allocate

	op := rspb.CreateOp{
		RangeID: roachpb.RangeID(req.RangeID),
		ReplicaID: roachpb.ReplicaID(req.ReplicaID)
	}
	rspb.LogID()
	op := WAGCreate{
		ID: rspb.FullLogID{
			RangeID:   req.RangeID,
			ReplicaID: req.ReplicaID,
			LogID:     lid,
		},
	}
	_ = op

	// le.c.Encode(le.buf[:0], op)
	err := errors.New("fixme")
	// err := b.Put(ctx, MakeKey(req.RangeID, req.ReplicaID), []byte("hi"))
	return rspb.FullLogID{}, 0, err
}

//func (llle *logEng) get(ctx context.Context, k Key) ([]byte, error) {
//	r := llle.e.eng.NewReader(storage.StandardDurability)
//	defer r.Close()
//	res, err := storage.MVCCGet(ctx, r, k.Encode(), hlc.Timestamp{}, storage.MVCCGetOptions{})
//	if err != nil {
//		return nil, err
//	}
//	if !res.Value.IsPresent() {
//		return nil, nil
//	}
//	return res.Value.GetBytes()
//}

func (le *logEng) Destroy(
	ctx context.Context, id rspb.FullLogID, req Destroy,
) (WAGIndex, error) {
	//TODO implement me
	panic("implement me")
}
