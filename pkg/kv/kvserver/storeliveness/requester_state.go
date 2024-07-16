// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"context"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// requesterState stores all in-memory requester-related data.
// The typical interactions with requesterState are:
// - requesterState.getHeartbeatsToSend(now hlc.Timestamp, interval time.Duration)
// - requesterState.handleHeartbeatResponses(msgs []slpb.Message)
// - requesterState.getSupportFrom(id slpb.StoreIdent)
// - requesterState.addStore(id slpb.StoreIdent)
// - requesterState.removeStore(id slpb.StoreIdent)
// NB: getHeartbeatsToSend and handleHeartbeatResponses are not safe for concurrent use.

// TODO(mira): Currently we don't do anything to prevent concurrent use of
// getHeartbeatsToSend and handleHeartbeatResponses.
// Using mu.RLock() and mu.RUnlock() in checkOutUpdate and checkInUpdate,
// respectively, can help ensure safety, but I didn't find an easy way to do
// this that the linter doesn't complain about.
// Alternatively, we can add an atomic.Bool updateInProgress to requesterState
// and set it to true at the beginning of checkOutUpdate and to false at the end
// of checkInUpdate.
type requesterState struct {
	// storeID is the ID of this store.
	storeID slpb.StoreIdent
	// meta stores the RequesterMeta, including the max timestamp and max epoch at
	// which this store has requested support.
	meta slpb.RequesterMeta
	// supportFrom stores the SupportState for each remote store from which this
	// store has received support.
	supportFrom map[slpb.StoreIdent]slpb.SupportState
	// mu controls access to supportFrom.
	mu syncutil.RWMutex
}

// getSupportFrom returns the SupportState corresponding to the given store in
// requesterState.supportFrom. The returned boolean indicates whether the given
// store is present in the supportFrom map; it does NOT indicate whether support
// from that store is provided.
func (rs *requesterState) getSupportFrom(id slpb.StoreIdent) (slpb.SupportState, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	ss, ok := rs.supportFrom[id]
	return ss, ok
}

// addStore adds a store to the requesterState.supportFrom map, if not present.
func (rs *requesterState) addStore(id slpb.StoreIdent) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.supportFrom[id]; !ok {
		// TODO(mira): reading rs.meta.MaxEpoch can be unsafe if this call to
		// addStore comes as a result of a SupportFrom call, and there is a
		// concurrent update to rs.meta.MaxEpoch (processing heartbeat responses).
		// Should rs.mu protect rs.meta as well?
		rs.supportFrom[id] = slpb.SupportState{Target: id, Epoch: rs.meta.MaxEpoch}
	}
}

// removeStore removes a store from the requesterState.supportFrom map.
func (rs *requesterState) removeStore(id slpb.StoreIdent) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.supportFrom, id)
}

// requesterStateForUpdate is a helper struct that is instantiated whenever the
// requesterState is being updated. It is necessary only for batch updates where
// the individual updates need to see each other's changes, while concurrent
// calls to SupportFrom see the persisted-to-disk view until the entire batch is
// also successfully persisted.
type requesterStateForUpdate struct {
	// checkedIn is a reference to the original requesterState struct before the
	// update started. It is used to respond to calls to SupportFrom while an
	// update is in progress to provide a response consistent with the state
	// persisted on disk.
	checkedIn *requesterState
	// inProgress holds all the updates to requesterState that are in progress and
	// have not yet been reflected in the checkedIn view. The inProgress view
	// ensures that ongoing updates from the same batch see each other's changes.
	inProgress requesterState
}

// getMeta returns the SupporterMeta from the inProgress view; if not present,
// it falls back to the SupporterMeta from the checkedIn view.
func (rsfu *requesterStateForUpdate) getMeta() slpb.RequesterMeta {
	if rsfu.inProgress.meta != (slpb.RequesterMeta{}) {
		return rsfu.inProgress.meta
	}
	return rsfu.checkedIn.meta
}

// getSupportFrom returns the SupportState from the inProgress view; if not
// present, it falls back to the SupportState from the checkedIn view. The
// returned boolean indicates whether the store is present in the supportFrom
// map; it does NOT indicate whether support from that store is provided.
func (rsfu *requesterStateForUpdate) getSupportFrom(
	storeID slpb.StoreIdent,
) (slpb.SupportState, bool) {
	ss, ok := rsfu.inProgress.supportFrom[storeID]
	if !ok {
		ss, ok = rsfu.checkedIn.getSupportFrom(storeID)
	}
	return ss, ok
}

// checkOutUpdate creates a requesterStateForUpdate object with an empty
// inProgress view and a checkedIn view that points to the requesterState.
func (rs *requesterState) checkOutUpdate() *requesterStateForUpdate {
	return &requesterStateForUpdate{
		checkedIn: rs,
		inProgress: requesterState{
			meta:        slpb.RequesterMeta{},
			supportFrom: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
}

// checkInUpdate updates the checkedIn view with any updates from the inProgress
// view. It persists changes to disk before updating the in-memory objects.
func (rsfu *requesterStateForUpdate) checkInUpdate() {
	if rsfu.inProgress.meta == (slpb.RequesterMeta{}) && len(rsfu.inProgress.supportFrom) == 0 {
		return
	}
	// TODO(mira): persist to disk here, only meta.
	rsfu.checkedIn.mu.Lock()
	defer rsfu.checkedIn.mu.Unlock()
	if rsfu.inProgress.meta != (slpb.RequesterMeta{}) {
		if !rsfu.inProgress.meta.MaxRequested.IsEmpty() {
			rsfu.checkedIn.meta.MaxRequested = rsfu.inProgress.meta.MaxRequested
		}
		if rsfu.inProgress.meta.MaxEpoch != 0 {
			rsfu.checkedIn.meta.MaxEpoch = rsfu.inProgress.meta.MaxEpoch
		}
	}
	for storeID, ss := range rsfu.inProgress.supportFrom {
		rsfu.checkedIn.supportFrom[storeID] = ss
	}
}

// handleHeartbeatResponses processes a slice of heartbeat response messages in
// batch. It does so by checking out a requesterStateForUpdate, delegating the
// actual heartbeat response handling, and then checking the updates back in.
func (rs *requesterState) handleHeartbeatResponses(msgs []slpb.Message) {
	rsfu := rs.checkOutUpdate()
	for _, msg := range msgs {
		// TODO(mira): This assumes that the messages may be of any type (since in
		// the prototype they come from a single receive queue). But we can simplify
		// the logic here and assume only heartbeat responses are sent to this function.
		// This will require separate receive queues for heartbeats and responses.
		switch msg.Type {
		case slpb.MsgHeartbeat:
			continue
		case slpb.MsgHeartbeatResp:
			rsfu.handleHeartbeatResponse(msg)
		default:
			log.Errorf(context.Background(), "unexpected message type: %v", msg.Type)
		}
	}
	rsfu.checkInUpdate()
}

// handleHeartbeatResponse handles a single heartbeat response message. It
// updates the inProgress view of requesterStateForUpdate only if there are any
// changes.
func (rsfu *requesterStateForUpdate) handleHeartbeatResponse(msg slpb.Message) {
	from := msg.From
	meta := rsfu.getMeta()
	ss, ok := rsfu.getSupportFrom(from)
	if !ok {
		ss = slpb.SupportState{Target: from}
	}
	metaNew, ssNew := handleHeartbeatResponse(meta, ss, msg)
	if meta != metaNew {
		rsfu.inProgress.meta = metaNew
	}
	if ss != ssNew {
		rsfu.inProgress.supportFrom[from] = ssNew
	}
}

// handleHeartbeatResponse contains the core logic for updating the epoch and
// expiration for a support provider upon receiving a heartbeat response.
func handleHeartbeatResponse(
	rm slpb.RequesterMeta, ss slpb.SupportState, msg slpb.Message,
) (slpb.RequesterMeta, slpb.SupportState) {
	if rm.MaxEpoch < msg.Epoch {
		rm.MaxEpoch = msg.Epoch
	}
	if ss.Epoch == msg.Epoch {
		ss.Expiration.Forward(msg.Expiration)
	} else if ss.Epoch < msg.Epoch {
		// assert ss.Epoch == msg.Epoch - 1
		ss.Epoch = msg.Epoch
		// assert msg.Expiration = hlc.Timestamp{}
		ss.Expiration = msg.Expiration
	}
	return rm, ss
}

// getHeartbeatsToSend generates a slice of heartbeat messages, one for each
// store in requesterState.supportFrom. Before returning the messages, it also
// updates the max requested timestamp in requesterState.meta.
func (rs *requesterState) getHeartbeatsToSend(
	now hlc.Timestamp, interval time.Duration,
) (heartbeats []slpb.Message) {
	// Updating MaxRequested is not a batch update, so it technically doesn't need
	// to use the checkIn/checkOut mechanism, but makes the logic simpler.
	rsfu := rs.checkOutUpdate()
	rsfu.updateMaxRequested(now, interval)
	rsfu.checkInUpdate()
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	for _, ss := range rs.supportFrom {
		heartbeat := slpb.Message{
			Type:       slpb.MsgHeartbeat,
			From:       rs.storeID,
			To:         ss.Target,
			Epoch:      ss.Epoch,
			Expiration: rs.meta.MaxRequested,
		}
		heartbeats = append(heartbeats, heartbeat)
	}
	return heartbeats
}

// updateMaxRequested forwards the current MaxRequested timestamp to now + interval,
// where now is the node's clock timestamp and interval is the liveness interval.
func (rsfu *requesterStateForUpdate) updateMaxRequested(now hlc.Timestamp, interval time.Duration) {
	newMaxRequested := now.Add(interval.Nanoseconds(), 0)
	if rsfu.getMeta().MaxRequested.Less(newMaxRequested) {
		rsfu.inProgress.meta.MaxRequested.Forward(newMaxRequested)
	}
}

// incrementMaxEpoch increments the MaxEpoch in requesterState.meta.
func (rs *requesterState) incrementMaxEpoch() {
	// Updating MaxEpoch is not a batch update, so it technically doesn't need
	// to use the checkIn/checkOut mechanism, but makes the logic simpler.
	rsfu := rs.checkOutUpdate()
	currentEpoch := rsfu.getMeta().MaxEpoch
	rsfu.inProgress.meta.MaxEpoch = currentEpoch + 1
	rsfu.checkInUpdate()
}
