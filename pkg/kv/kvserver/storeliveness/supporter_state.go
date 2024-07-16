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

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// supporterState stores all in-memory supporter-related data.
// The typical interactions with supporterState are:
// - supporterState.handleHeartbeats(msgs []slpb.Message)
// - supporterState.withdrawSupport(now hlc.ClockTimestamp)
// - supporterState.getSupportFor(id slpb.StoreIdent)
// NB: handleHeartbeats and withdrawSupport are not safe for concurrent use.

// TODO(mira): Same comment about concurrent use safety as in requesterState.
type supporterState struct {
	// meta stores the SupporterMeta, including the max timestamp at which this
	// store has withdrawn support.
	meta slpb.SupporterMeta
	// supportFor stores the SupportState for each remote store for which this
	// store has provided support.
	supportFor map[slpb.StoreIdent]slpb.SupportState
	// mu controls access to supportFor.
	mu syncutil.RWMutex
}

// getSupportFor returns the SupportState corresponding to the given store in
// supporterState.supportFor. The returned boolean indicates whether the given
// store is present in the supportFor map; it does NOT indicate whether support
// for that store is provided.
func (ss *supporterState) getSupportFor(id slpb.StoreIdent) (slpb.SupportState, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	s, ok := ss.supportFor[id]
	return s, ok
}

// supporterStateForUpdate is a helper struct that is instantiated whenever the
// supporterState is being updated. It is necessary only for batch updates where
// the individual updates need to see each other's changes, but concurrent calls
// to SupportFor should see the persisted-to-disk view until the entire batch is
// also successfully persisted.
type supporterStateForUpdate struct {
	// checkedIn is a reference to the original supporterState struct before the
	// update started. It is used to respond to calls to SupportFor while an
	// update is in progress to provide a response consistent with the state
	// persisted on disk.
	checkedIn *supporterState
	// inProgress holds all the updates to supporterState that are in progress and
	// have not yet been reflected in the checkedIn view. The inProgress view
	// ensures that ongoing updates from the same batch see each other's changes.
	inProgress supporterState
}

// getMeta returns the SupporterMeta from the inProgress view; if not present,
// it falls back to the SupporterMeta from the checkedIn view.
func (ssfu *supporterStateForUpdate) getMeta() slpb.SupporterMeta {
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		return ssfu.inProgress.meta
	}
	return ssfu.checkedIn.meta
}

// getSupportFor returns the SupportState from the inProgress view; if not
// present, it falls back to the SupportState from the checkedIn view.
// The returned boolean indicates whether the store is present in the supportFor
// map; it does NOT indicate whether support is provided.
func (ssfu *supporterStateForUpdate) getSupportFor(
	storeID slpb.StoreIdent,
) (slpb.SupportState, bool) {
	ss, ok := ssfu.inProgress.supportFor[storeID]
	if !ok {
		ss, ok = ssfu.checkedIn.getSupportFor(storeID)
	}
	return ss, ok
}

// checkOutUpdate creates a supporterStateForUpdate object with an empty
// inProgress view and a checkedIn view that points to the supporterState.
func (ss *supporterState) checkOutUpdate() *supporterStateForUpdate {
	return &supporterStateForUpdate{
		checkedIn: ss,
		inProgress: supporterState{
			meta:       slpb.SupporterMeta{},
			supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
}

// checkInUpdate updates the checkedIn view with any updates from the inProgress
// view. It persists changes to disk before updating the in-memory objects.
func (ssfu *supporterStateForUpdate) checkInUpdate() {
	if ssfu.inProgress.meta == (slpb.SupporterMeta{}) && len(ssfu.inProgress.supportFor) == 0 {
		return
	}
	// TODO(mira): persist to disk here, meta and supportFor.
	ssfu.checkedIn.mu.Lock()
	defer ssfu.checkedIn.mu.Unlock()
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		if !ssfu.inProgress.meta.MaxWithdrawn.IsEmpty() {
			ssfu.checkedIn.meta.MaxWithdrawn = ssfu.inProgress.meta.MaxWithdrawn
		}
	}
	for storeID, ss := range ssfu.inProgress.supportFor {
		ssfu.checkedIn.supportFor[storeID] = ss
	}
}

// handleHeartbeats processes a slice of heartbeat messages in batch. It does so
// by checking out a supporterStateForUpdate, delegating the actual heartbeat
// handling, and then checking the updates back in.
func (ss *supporterState) handleHeartbeats(msgs []slpb.Message) []slpb.Message {
	ssfu := ss.checkOutUpdate()
	var responses []slpb.Message
	for _, msg := range msgs {
		switch msg.Type {
		case slpb.MsgHeartbeat:
			resp := ssfu.handleHeartbeat(msg)
			responses = append(responses, resp)
		case slpb.MsgHeartbeatResp:
			continue
		default:
			log.Errorf(context.Background(), "unexpected message type: %v", msg.Type)
		}
	}
	ssfu.checkInUpdate()
	return responses
}

// handleHeartbeat handles a single heartbeat message. It updates the inProgress
// view of supporterStateForUpdate only if there are any changes, and returns
// a heartbeat response message.
func (ssfu *supporterStateForUpdate) handleHeartbeat(msg slpb.Message) slpb.Message {
	from := msg.From
	ss, ok := ssfu.getSupportFor(from)
	if !ok {
		ss = slpb.SupportState{Target: from}
	}
	ssNew := handleHeartbeat(ss, msg)
	if ss != ssNew {
		ssfu.inProgress.supportFor[from] = ssNew
	}
	return slpb.Message{
		Type:       slpb.MsgHeartbeatResp,
		From:       msg.To,
		To:         msg.From,
		Epoch:      ssNew.Epoch,
		Expiration: ssNew.Expiration,
	}
}

// handleHeartbeat contains the core logic for updating the epoch and expiration
// of a support requester upon receiving a heartbeat.
func handleHeartbeat(ss slpb.SupportState, msg slpb.Message) slpb.SupportState {
	if ss.Epoch == msg.Epoch {
		ss.Expiration.Forward(msg.Expiration)
	} else if ss.Epoch < msg.Epoch {
		ss.Epoch = msg.Epoch
		ss.Expiration = msg.Expiration
	}
	return ss
}

// withdrawSupport checks if support needs to be withdrawn from any stores in
// batch. It does so by checking out supporterStateForUpdate, delegating the
// actual support withdrawal, and checking the updates back in.
func (ss *supporterState) withdrawSupport(now hlc.ClockTimestamp) {
	ssfu := ss.checkOutUpdate()
	func() {
		// TODO(mira): do we need to hold the RLock() while iterating over
		// ss.supportFor? I think so.
		ss.mu.RLock()
		defer ss.mu.RUnlock()
		for id := range ss.supportFor {
			ssfu.withdrawSupport(id, now)
		}
	}()
	ssfu.checkInUpdate()
}

// withdrawSupport handles a single support withdrawal. It updates the
// inProgress view of supporterStateForUpdate only if there are any changes.
func (ssfu *supporterStateForUpdate) withdrawSupport(id slpb.StoreIdent, now hlc.ClockTimestamp) {
	ss, ok := ssfu.getSupportFor(id)
	if !ok {
		return
	}
	ssNew := withdrawSupport(ss, now)
	if ss != ssNew {
		ssfu.inProgress.supportFor[id] = ssNew
		sm := ssfu.getMeta()
		if sm.MaxWithdrawn.Forward(now) {
			ssfu.inProgress.meta = sm
		}
	}
}

// withdrawSupport contains the core logic for updating the epoch and expiration
// of a support requester when withdrawing support.
func withdrawSupport(ss slpb.SupportState, now hlc.ClockTimestamp) slpb.SupportState {
	if !ss.Expiration.IsEmpty() && ss.Expiration.Less(now.ToTimestamp()) {
		ss.Epoch++
		ss.Expiration = hlc.Timestamp{}
	}
	return ss
}
