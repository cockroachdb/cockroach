// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.RevertRange, declareKeysRevertRange, RevertRange)
}

func declareKeysRevertRange(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, spans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

func isEmptyKeyTimeRange(
	batch engine.ReadWriter, from, to engine.MVCCKey, since hlc.Timestamp,
) (bool, error) {
	// Use a TBI to check if there is anyting to delete.
	iter := batch.NewIterator(engine.IterOptions{
		LowerBound: from.Key, UpperBound: to.Key, MinTimestampHint: since,
	})
	defer iter.Close()
	iter.Seek(from)
	ok, err := iter.Valid()
	return !ok, err
}

// RevertRange wipes all MVCC versions more recent than TargetTime of the keys
// covered by the specified span, adjusting the MVCC stats accordingly.
//
// Note: this should only be used when there is no user traffic writing to the
// target span at or above the target time.
func RevertRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, errors.New("cannot execute RevertRange within a transaction")
	}
	log.VEventf(ctx, 2, "RevertRange %+v", cArgs.Args)

	// Encode MVCCKey values for start and end of clear span.
	args := cArgs.Args.(*roachpb.RevertRangeRequest)
	from := engine.MVCCKey{Key: args.Key}
	to := engine.MVCCKey{Key: args.EndKey}
	var pd result.Result

	clearStartTime := args.TargetTime.Next()

	if empty, err := isEmptyKeyTimeRange(batch, from, to, clearStartTime); err != nil || empty {
		return result.Result{}, err
	}
	log.VEventf(ctx, 2, "clearing keys with timestamp > %v", args.TargetTime)
	statsBefore, err := computeStatsDelta(ctx, batch, cArgs, from, to)
	if err != nil {
		return result.Result{}, err
	}

	err = engine.MVCCClearTimeRange(ctx, batch, from, to, clearStartTime, cArgs.Header.Timestamp)
	if err != nil {
		return result.Result{}, err
	}

	iter := batch.NewIterator(engine.IterOptions{UpperBound: to.Key})
	statsAfter, err := iter.ComputeStats(from, to, cArgs.Header.Timestamp.WallTime)
	iter.Close()
	if err != nil {
		return result.Result{}, err
	}
	statsAfter.Subtract(statsBefore)

	*cArgs.Stats = statsAfter

	return pd, nil
}
