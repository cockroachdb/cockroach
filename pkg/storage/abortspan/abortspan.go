// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package abortspan

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// An AbortSpan sets markers for aborted transactions to provide protection
// against an aborted but active transaction not reading values it wrote (due to
// its intents having been removed).
//
// The span is range-specific. It is updated when an intent for an aborted txn
// is cleared from a range, and is consulted before read commands are processed
// on a range.
//
// An AbortSpan is not thread safe.
type AbortSpan struct {
	rangeID roachpb.RangeID
}

// New returns a new AbortSpan. Every range replica
// maintains an AbortSpan, not just the lease holder.
func New(rangeID roachpb.RangeID) *AbortSpan {
	return &AbortSpan{
		rangeID: rangeID,
	}
}

func fillUUID(b byte) uuid.UUID {
	var ret uuid.UUID
	for i := range ret.GetBytes() {
		ret.UUID[i] = b
	}
	return ret
}

var txnIDMin = fillUUID('\x00')
var txnIDMax = fillUUID('\xff')

// MinKey returns the lower bound of the key span associated to an instance for the given RangeID.
func MinKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortSpanKey(rangeID, txnIDMin)
}

func (sc *AbortSpan) min() roachpb.Key {
	return MinKey(sc.rangeID)
}

// MaxKey returns the upper bound of the key span associated to an instance for the given RangeID.
func MaxKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortSpanKey(rangeID, txnIDMax)
}

func (sc *AbortSpan) max() roachpb.Key {
	return MaxKey(sc.rangeID)
}

// ClearData removes all persisted items stored in the cache.
func (sc *AbortSpan) ClearData(e engine.Engine) error {
	iter := e.NewIterator(engine.IterOptions{UpperBound: sc.max()})
	defer iter.Close()
	b := e.NewWriteOnlyBatch()
	defer b.Close()
	err := b.ClearIterRange(iter, engine.MakeMVCCMetadataKey(sc.min()),
		engine.MakeMVCCMetadataKey(sc.max()))
	if err != nil {
		return err
	}
	return b.Commit(false /* sync */)
}

// Get looks up an AbortSpan entry recorded for this transaction ID.
// Returns whether an abort record was found and any error.
func (sc *AbortSpan) Get(
	ctx context.Context, e engine.Reader, txnID uuid.UUID, entry *roachpb.AbortSpanEntry,
) (bool, error) {

	// Pull response from disk and read into reply if available.
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	ok, err := engine.MVCCGetProto(ctx, e, key, hlc.Timestamp{}, true /* consistent */, nil /* txn */, entry)
	return ok, err
}

// Iterate walks through the AbortSpan, invoking the given callback for
// each unmarshaled entry with the MVCC key and the decoded entry.
func (sc *AbortSpan) Iterate(
	ctx context.Context, e engine.Reader, f func(roachpb.Key, roachpb.AbortSpanEntry) error,
) error {
	_, err := engine.MVCCIterate(ctx, e, sc.min(), sc.max(), hlc.Timestamp{},
		true /* consistent */, false /* tombstones */, nil /* txn */, false, /* reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.AbortSpanEntry
			if _, err := keys.DecodeAbortSpanKey(kv.Key, nil); err != nil {
				return false, err
			}
			if err := kv.Value.GetProto(&entry); err != nil {
				return false, err
			}
			return false, f(kv.Key, entry)
		})
	return err
}

// Del removes all AbortSpan entries for the given transaction.
func (sc *AbortSpan) Del(
	ctx context.Context, e engine.ReadWriter, ms *enginepb.MVCCStats, txnID uuid.UUID,
) error {
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	return engine.MVCCDelete(ctx, e, ms, key, hlc.Timestamp{}, nil /* txn */)
}

// Put writes an entry for the specified transaction ID.
func (sc *AbortSpan) Put(
	ctx context.Context,
	e engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txnID uuid.UUID,
	entry *roachpb.AbortSpanEntry,
) error {
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	return engine.MVCCPutProto(ctx, e, ms, key, hlc.Timestamp{}, nil /* txn */, entry)
}
