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

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterCommand(roachpb.GC, declareKeysGC, GC)
}

func declareKeysGC(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// Intentionally don't call DefaultDeclareKeys: the key range in the header
	// is usually the whole range (pending resolution of #7880).
	gcr := req.(*roachpb.GCRequest)
	for _, key := range gcr.Keys {
		spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: key.Key})
	}
	// Be smart here about blocking on the threshold keys. The GC queue can send an empty
	// request first to bump the thresholds, and then another one that actually does work
	// but can avoid declaring these keys below.
	if gcr.Threshold != (hlc.Timestamp{}) {
		spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
	}
	if gcr.TxnSpanGCThreshold != (hlc.Timestamp{}) {
		spans.Add(spanset.SpanReadWrite, roachpb.Span{
			// TODO(bdarnell): since this must be checked by all
			// reads, this should be factored out into a separate
			// waiter which blocks only those reads far enough in the
			// past to be affected by the in-flight GCRequest (i.e.
			// normally none). This means this key would be special
			// cased and not tracked by the command queue.
			Key: keys.RangeTxnSpanGCThresholdKey(header.RangeID),
		})
	}
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

// GC iterates through the list of keys to garbage collect
// specified in the arguments. MVCCGarbageCollect is invoked on each
// listed key along with the expiration timestamp. The GC metadata
// specified in the args is persisted after GC.
func GC(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.GCRequest)
	h := cArgs.Header

	// All keys must be inside the current replica range. Keys outside
	// of this range in the GC request are dropped silently, which is
	// safe because they can simply be re-collected later on the correct
	// replica. Discrepancies here can arise from race conditions during
	// range splitting.
	keys := make([]roachpb.GCRequest_GCKey, 0, len(args.Keys))
	for _, k := range args.Keys {
		if cArgs.EvalCtx.ContainsKey(k.Key) {
			keys = append(keys, k)
		}
	}

	// Garbage collect the specified keys by expiration timestamps.
	if err := engine.MVCCGarbageCollect(
		ctx, batch, cArgs.Stats, keys, h.Timestamp,
	); err != nil {
		return result.Result{}, err
	}

	// Protect against multiple GC requests arriving out of order; we track
	// the maximum timestamps.

	var newThreshold hlc.Timestamp
	if args.Threshold != (hlc.Timestamp{}) {
		oldThreshold := cArgs.EvalCtx.GetGCThreshold()
		newThreshold = oldThreshold
		newThreshold.Forward(args.Threshold)
	}

	var newTxnSpanGCThreshold hlc.Timestamp
	if args.TxnSpanGCThreshold != (hlc.Timestamp{}) {
		oldTxnSpanGCThreshold := cArgs.EvalCtx.GetTxnSpanGCThreshold()
		newTxnSpanGCThreshold = oldTxnSpanGCThreshold
		newTxnSpanGCThreshold.Forward(args.TxnSpanGCThreshold)
	}

	var pd result.Result
	stateLoader := MakeStateLoader(cArgs.EvalCtx)

	// Don't write these keys unless we have to. We also don't declare these
	// keys unless we have to (to allow the GC queue to batch requests more
	// efficiently), and we must honor what we declare.

	var replState storagebase.ReplicaState
	if newThreshold != (hlc.Timestamp{}) {
		replState.GCThreshold = &newThreshold
		if err := stateLoader.SetGCThreshold(ctx, batch, cArgs.Stats, &newThreshold); err != nil {
			return result.Result{}, err
		}
	}

	if newTxnSpanGCThreshold != (hlc.Timestamp{}) {
		replState.TxnSpanGCThreshold = &newTxnSpanGCThreshold
		if err := stateLoader.SetTxnSpanGCThreshold(ctx, batch, cArgs.Stats, &newTxnSpanGCThreshold); err != nil {
			return result.Result{}, err
		}
	}

	// Only set ReplicatedEvalResult.ReplicaState if at least one of the GC keys
	// was written. Leaving the field nil to signify that no changes to the
	// Replica state occurred allows replicas to perform less work beneath Raft.
	if replState != (storagebase.ReplicaState{}) {
		pd.Replicated.State = &replState
	}
	return pd, nil
}
