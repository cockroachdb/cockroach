// Copyright 2018 The Cockroach Authors.
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

package storage

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
var RangefeedEnabled = settings.RegisterBoolSetting(
	"kv.rangefeed.enabled",
	"if set, rangefeed registration is enabled",
	false,
)

// lockedRangefeedStream is an implementation of rangefeed.Stream which provides
// support for concurrent calls to Send. Note that the default implementation of
// grpc.Stream is not safe for concurrent calls to Send.
type lockedRangefeedStream struct {
	roachpb.Internal_RangeFeedServer
	sendMu syncutil.Mutex
}

func (s *lockedRangefeedStream) Send(e *roachpb.RangeFeedEvent) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.Internal_RangeFeedServer.Send(e)
}

// rangefeedTxnPusher is a shim around intentResolver that implements the
// rangefeed.TxnPusher interface.
type rangefeedTxnPusher struct {
	ir *intentResolver
	r  *Replica
}

// PushTxns is part of the rangefeed.TxnPusher interface. It performs a
// high-priority push at the specified timestamp to each of the specified
// transactions.
func (tp *rangefeedTxnPusher) PushTxns(
	ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
) ([]roachpb.Transaction, error) {
	pushTxnMap := make(map[uuid.UUID]enginepb.TxnMeta, len(txns))
	for _, txn := range txns {
		pushTxnMap[txn.ID] = txn
	}

	h := roachpb.Header{
		Timestamp: ts,
		Txn: &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: roachpb.MaxTxnPriority,
			},
		},
	}

	pushedTxnMap, pErr := tp.ir.maybePushTransactions(
		ctx, pushTxnMap, h, roachpb.PUSH_TIMESTAMP, false, /* skipIfInFlight */
	)
	if pErr != nil {
		return nil, pErr.GoError()
	}

	pushedTxns := make([]roachpb.Transaction, 0, len(pushedTxnMap))
	for _, txn := range pushedTxnMap {
		pushedTxns = append(pushedTxns, txn)
	}
	return pushedTxns, nil
}

// CleanupTxnIntentsAsync is part of the rangefeed.TxnPusher interface.
func (tp *rangefeedTxnPusher) CleanupTxnIntentsAsync(
	ctx context.Context, txns []roachpb.Transaction,
) error {
	endTxns := make([]result.EndTxnIntents, len(txns))
	for i, txn := range txns {
		endTxns[i].Txn = txn
	}
	return tp.ir.cleanupTxnIntentsAsync(ctx, tp.r, endTxns, true /* allowSyncProcessing */)
}

// RangeFeed registers a rangefeed over the specified span. It sends updates to
// the provided stream and returns with an optional error when the rangefeed is
// complete.
func (r *Replica) RangeFeed(
	ctx context.Context, args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) *roachpb.Error {
	if !RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		return roachpb.NewErrorf("rangefeeds are not enabled. See kv.rangefeed.enabled.")
	}

	var rspan roachpb.RSpan
	var err error
	rspan.Key, err = keys.Addr(args.Span.Key)
	if err != nil {
		return roachpb.NewError(err)
	}
	rspan.EndKey, err = keys.Addr(args.Span.EndKey)
	if err != nil {
		return roachpb.NewError(err)
	}

	checkTS := args.Timestamp
	if checkTS.IsEmpty() {
		checkTS = r.Clock().Now()
	}

	lockedStream := &lockedRangefeedStream{Internal_RangeFeedServer: stream}
	errC := make(chan *roachpb.Error)

	// Lock the raftMu, then register the stream as a new rangefeed registration.
	// raftMu is held so that the catch-up iterator is captured in the same
	// critical-section as the registration is established. This ensures that
	// the registration doesn't miss any events.
	r.raftMu.Lock()
	if err := r.requestCanProceed(rspan, checkTS); err != nil {
		r.raftMu.Unlock()
		return roachpb.NewError(err)
	}

	// Ensure that the rangefeed processor is running.
	p := r.maybeInitRangefeedRaftMuLocked()
	defer func() {
		r.raftMu.Lock()
		r.maybeDestroyRangefeedRaftMuLocked(p)
		r.raftMu.Unlock()
	}()

	// Register the stream with a catch-up iterator.
	var catchUpIter engine.SimpleIterator
	if !args.Timestamp.IsEmpty() {
		catchUpIter = r.Engine().NewIterator(engine.IterOptions{
			UpperBound:       rspan.EndKey.AsRawKey(),
			MinTimestampHint: args.Timestamp,
		})
	}
	p.Register(rspan, args.Timestamp, catchUpIter, lockedStream, errC)
	r.raftMu.Unlock()

	// Block on the registration's error channel.
	return <-errC
}

// maybeInitRangefeedRaftMuLocked initializes a rangefeed for the Replica if one
// is not already running. Requires raftMu be locked.
func (r *Replica) maybeInitRangefeedRaftMuLocked() *rangefeed.Processor {
	if r.raftMu.rangefeed != nil {
		return r.raftMu.rangefeed
	}

	// Create a new rangefeed.
	desc := r.mu.state.Desc
	tp := rangefeedTxnPusher{ir: r.store.intentResolver, r: r}
	cfg := rangefeed.Config{
		AmbientContext: r.AmbientContext,
		Clock:          r.Clock(),
		Span:           desc.RSpan(),
		TxnPusher:      &tp,
		EventChanCap:   128,
	}
	r.raftMu.rangefeed = rangefeed.NewProcessor(cfg)

	// Start it with an iterator to initialize the resolved timestamp.
	rtsIter := r.Engine().NewIterator(engine.IterOptions{
		UpperBound: desc.EndKey.AsRawKey(),
		// TODO(nvanbenschoten): To facilitate fast restarts of rangefeed
		// we should periodically persist the resolved timestamp so that we
		// can initialize the rangefeed using an iterator that only needs to
		// observe timestamps back to the last recorded resolved timestamp.
		// This is safe because we know that there are no unresolved intents
		// at times before a resolved timestamp.
		// MinTimestampHint: r.ResolvedTimestamp,
	})
	r.raftMu.rangefeed.Start(r.store.Stopper(), rtsIter)

	// TODO(nvanbenschoten): forward the rangefeed's closed timestamp if the
	// range has established a closed timestamp.
	// r.raftMu.rangefeed.ForwardClosedTS(r.ClosedTimestamp)
	return r.raftMu.rangefeed
}

// maybeDestroyRangefeedRaftMuLocked tears down the provided Processor if it is
// still active and if it no longer has any registrations. Requires raftMu to be
// locked.
func (r *Replica) maybeDestroyRangefeedRaftMuLocked(p *rangefeed.Processor) {
	if r.raftMu.rangefeed != p {
		return
	}
	if r.raftMu.rangefeed.Len() == 0 {
		r.raftMu.rangefeed.Stop()
		r.raftMu.rangefeed = nil
	}
}

// disconnectRangefeedWithErrRaftMuLocked broadcasts the provided error to all
// rangefeed registrations and tears down the active rangefeed Processor. No-op
// if a rangefeed is not active. Requires raftMu to be locked.
func (r *Replica) disconnectRangefeedWithErrRaftMuLocked(pErr *roachpb.Error) {
	if r.raftMu.rangefeed == nil {
		return
	}
	r.raftMu.rangefeed.StopWithErr(pErr)
	r.raftMu.rangefeed = nil
}

// handleLogicalOpLogRaftMuLocked passes the logical op log to the active
// rangefeed, if one is running. No-op if a rangefeed is not active. Requires
// raftMu to be locked.
func (r *Replica) handleLogicalOpLogRaftMuLocked(
	ctx context.Context, ops *storagebase.LogicalOpLog,
) {
	if r.raftMu.rangefeed == nil {
		return
	}
	if ops == nil {
		r.disconnectRangefeedWithErrRaftMuLocked(roachpb.NewErrorf(
			"unset LogicalOpLog provided to enabled rangefeed",
		))
		return
	}
	if len(ops.Ops) == 0 {
		return
	}

	// When reading straight from the Raft log, some logical ops will not be
	// fully populated. Read from the engine (under raftMu) to populate all
	// fields.
	for _, op := range ops.Ops {
		var key []byte
		var ts hlc.Timestamp
		var valPtr *[]byte
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCCommitIntentOp:
			key, ts, valPtr = t.Key, t.Timestamp, &t.Value
		case *enginepb.MVCCWriteIntentOp,
			*enginepb.MVCCUpdateIntentOp,
			*enginepb.MVCCAbortIntentOp:
			// Nothing to do.
			continue
		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Read the value directly from the Engine. This is performed in the
		// same raftMu critical section that the logical op's corresponding
		// WriteBatch is applied, so the value should exist.
		val, _, err := engine.MVCCGetWithTombstone(ctx, r.Engine(),
			key, ts, true /* consistent */, nil /* txn */)
		if val == nil && err == nil {
			err = errors.New("value missing in engine")
		}
		if err != nil {
			r.disconnectRangefeedWithErrRaftMuLocked(roachpb.NewErrorf(
				"error consuming %T for key %v @ ts %v: %v", op, key, ts, err,
			))
			return
		}
		*valPtr = val.RawBytes
	}

	// Pass the ops to the rangefeed processor.
	r.raftMu.rangefeed.ConsumeLogicalOps(ops.Ops...)
}
