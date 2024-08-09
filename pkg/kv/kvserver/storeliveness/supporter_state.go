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
	"sync/atomic"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// supporterStateHandler is the main interface for handling support for other
// stores. The typical interactions with supporterStateHandler are:
//   - getSupportFor(id slpb.StoreIdent)
//   - update := checkOutUpdate()
//     update.handleHeartbeat(msg slpb.Message)
//     checkInUpdate(update)
//   - update := checkOutUpdate()
//     update.withdrawSupport(now hlc.ClockTimestamp)
//     checkInUpdate(update)
//
// Only one update can be in progress to ensure that multiple mutation methods
// are not run concurrently.
//
// Adding a store to support is done automatically when a heartbeat from that
// store is first received. Currently, a store is never removed.
type supporterStateHandler struct {
	// supporterState is the source of truth for provided support.
	supporterState *supporterState
	// mu controls access to supporterState. The access pattern to supporterState
	// is single writer, multi reader. Concurrent reads come from API calls to
	// SupportFor; these require RLocking mu. Updates to supporterState are done
	// from a single goroutine; these require Locking mu when writing the updates.
	// These updates also read from supporterState but there is no need to RLock
	// mu during these reads (since there are no concurrent writes).
	mu syncutil.RWMutex
	// update is a reference to an in-progress change in supporterStateForUpdate.
	// A non-nil update implies there is no ongoing update; i.e. the referenced
	// requesterStateForUpdate is available to be checked out.
	update atomic.Pointer[supporterStateForUpdate]
}

func newSupporterStateHandler() *supporterStateHandler {
	ssh := supporterStateHandler{
		supporterState: &supporterState{
			meta:       slpb.SupporterMeta{},
			supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
	ssh.update.Store(
		&supporterStateForUpdate{
			checkedIn: ssh.supporterState,
			inProgress: supporterState{
				meta:       slpb.SupporterMeta{},
				supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
			},
		},
	)
	return &ssh
}

// supporterStateForUpdate is a helper struct that facilitates updates to
// supporterState. It is necessary only for batch updates where the individual
// updates need to see each other's changes, while concurrent calls to
// SupportFor see the persisted-to-disk view until the in-progress batch is
// successfully persisted.
type supporterStateForUpdate struct {
	// checkedIn is a reference to the original supporterState struct stored in
	// supporterStateHandler. It is used to respond to calls to SupportFor (while
	// an update is in progress) to provide a response consistent with the state
	// persisted on disk.
	checkedIn *supporterState
	// inProgress holds all the updates to supporterState that are in progress and
	// have not yet been reflected in the checkedIn view. The inProgress view
	// ensures that ongoing updates from the same batch see each other's changes.
	inProgress supporterState
}

// supporterState stores the core data structures for providing support.
type supporterState struct {
	// meta stores the SupporterMeta, including the max timestamp at which this
	// store has withdrawn support.
	meta slpb.SupporterMeta
	// supportFor stores the SupportState for each remote store for which this
	// store has provided support.
	supportFor map[slpb.StoreIdent]slpb.SupportState
}

// getSupportFor returns the SupportState corresponding to the given store in
// supporterState.supportFor.
func (ssh *supporterStateHandler) getSupportFor(id slpb.StoreIdent) slpb.SupportState {
	ssh.mu.RLock()
	defer ssh.mu.RUnlock()
	ss, ok := ssh.supporterState.supportFor[id]
	if !ok {
		return slpb.SupportState{}
	}
	return ss
}

// Functions for handling supporterState updates.

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
		ss, ok = ssfu.checkedIn.supportFor[storeID]
	}
	return ss, ok
}

// reset clears the inProgress view of supporterStateForUpdate.
func (ssfu *supporterStateForUpdate) reset() {
	ssfu.inProgress.meta = slpb.SupporterMeta{}
	ssfu.inProgress.supportFor = make(map[slpb.StoreIdent]slpb.SupportState)
}

// updateInProgress determines if there is an update in progress.
func (ssh *supporterStateHandler) updateInProgress() bool {
	return ssh.update.Load() == nil
}

// checkOutUpdate returns the supporterStateForUpdate referenced in
// supporterStateHandler.update and replaces it with a nil pointer to ensure it
// cannot be checked out concurrently as part of another mutation.
func (ssh *supporterStateHandler) checkOutUpdate() *supporterStateForUpdate {
	if ssh.updateInProgress() {
		panic("unsupported concurrent update")
	}
	return ssh.update.Swap(nil)
}

// checkInUpdate updates the checkedIn view of supporterStateForUpdate with any
// updates from the inProgress view. It clears the inProgress view, and swaps it
// back in supporterStateHandler.update to be checked out by future updates.
func (ssh *supporterStateHandler) checkInUpdate(forUpdate *supporterStateForUpdate) {
	defer func() {
		forUpdate.reset()
		ssh.update.Swap(forUpdate)
	}()
	if forUpdate.inProgress.meta == (slpb.SupporterMeta{}) &&
		len(forUpdate.inProgress.supportFor) == 0 {
		return
	}
	ssh.mu.Lock()
	defer ssh.mu.Unlock()
	if forUpdate.inProgress.meta != (slpb.SupporterMeta{}) {
		if !forUpdate.inProgress.meta.MaxWithdrawn.IsEmpty() {
			forUpdate.checkedIn.meta.MaxWithdrawn = forUpdate.inProgress.meta.MaxWithdrawn
		}
	}
	for storeID, ss := range forUpdate.inProgress.supportFor {
		forUpdate.checkedIn.supportFor[storeID] = ss
	}
}

// Functions for handling heartbeats.

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

// Functions for withdrawing support.

// withdrawSupport handles a single support withdrawal. It updates the
// inProgress view of supporterStateForUpdate only if there are any changes.
func (ssfu *supporterStateForUpdate) withdrawSupport(now hlc.ClockTimestamp) {
	// It's ok to read store IDs directly from ssfu.checkedIn.supportFor even
	// though it's possible that within the same update a handleHeartbeat call
	// added a new store to ssfu.inProgress.supportFor. The loop below will not
	// see that store but that's ok; support will be withdrawn in the next
	// iteration if needed.
	for id := range ssfu.checkedIn.supportFor {
		ss, ok := ssfu.getSupportFor(id)
		if !ok {
			return
		}
		ssNew := withdrawSupport(ss, now)
		if ss != ssNew {
			ssfu.inProgress.supportFor[id] = ssNew
			if ssfu.getMeta().MaxWithdrawn.Less(now) {
				ssfu.inProgress.meta.MaxWithdrawn.Forward(now)
			}
		}
	}
}

// withdrawSupport contains the core logic for updating the epoch and expiration
// of a support requester when withdrawing support.
func withdrawSupport(ss slpb.SupportState, now hlc.ClockTimestamp) slpb.SupportState {
	if !ss.Expiration.IsEmpty() && ss.Expiration.LessEq(now.ToTimestamp()) {
		ss.Epoch++
		ss.Expiration = hlc.Timestamp{}
	}
	return ss
}
