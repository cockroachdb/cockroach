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
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// supporterStateHandler is the main interface for handling support for other
// stores. The typical interactions with supporterStateHandler are:
//   - getSupportFor(id slpb.StoreIdent)
//   - checkOutUpdate()
//     handleHeartbeat(msg slpb.Message)
//     checkInUpdate()
//   - checkOutUpdate()
//     withdrawSupport(now hlc.ClockTimestamp)
//     checkInUpdate()
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
	// update stores any in-progress updates.
	update *supporterStateForUpdate
	// updateMu ensures there is only one update in progress.
	updateMu syncutil.Mutex
}

// supporterStateForUpdate is a helper struct that facilitates updates to
// supporterState. It is necessary only for batch updates where the individual
// updates need to see each other's changes, while concurrent calls to
// SupportFor see the persisted-to-disk view until the in-progress batch is
// successfully persisted.
type supporterStateForUpdate struct {
	// checkedIn is a reference to the original supporterState struct before the
	// update started. It is used to respond to calls to SupportFor (while an
	// update is in progress) to provide a response consistent with the state
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

// checkOutUpdate creates a supporterStateForUpdate object with an empty
// inProgress view and a checkedIn view that points to the supporterState.
// (if one doesn't exist already).
func (ssh *supporterStateHandler) checkOutUpdate() {
	if !ssh.updateMu.TryLock() {
		panic("unsupported concurrent update")
	}
	if ssh.update == nil {
		ssh.update = &supporterStateForUpdate{
			checkedIn: ssh.supporterState,
			inProgress: supporterState{
				meta:       slpb.SupporterMeta{},
				supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
			},
		}
	}
}

// checkInUpdate updates checkedIn with any updates from inProgress.
func (ssh *supporterStateHandler) checkInUpdate() {
	defer ssh.updateMu.Unlock()
	if ssh.update.inProgress.meta == (slpb.SupporterMeta{}) &&
		len(ssh.update.inProgress.supportFor) == 0 {
		return
	}
	ssh.mu.Lock()
	defer ssh.mu.Unlock()
	if ssh.update.inProgress.meta != (slpb.SupporterMeta{}) {
		if !ssh.update.inProgress.meta.MaxWithdrawn.IsEmpty() {
			ssh.update.checkedIn.meta.MaxWithdrawn = ssh.update.inProgress.meta.MaxWithdrawn
		}
	}
	for storeID, ss := range ssh.update.inProgress.supportFor {
		ssh.update.checkedIn.supportFor[storeID] = ss
	}
	ssh.update.reset()
}

// Functions for handling heartbeats.

// handleHeartbeat delegates heartbeat handling to supporterStateForUpdate, if
// there is one; otherwise, it panics.
func (ssh *supporterStateHandler) handleHeartbeat(msg slpb.Message) slpb.Message {
	ssh.updateMu.AssertHeld()
	return ssh.update.handleHeartbeat(msg)
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

// Functions for withdrawing support.

// withdrawSupport delegates withdrawing support to supporterStateForUpdate, if
// there is one; otherwise, it panics.
func (ssh *supporterStateHandler) withdrawSupport(now hlc.ClockTimestamp) {
	ssh.updateMu.AssertHeld()
	ssh.update.withdrawSupport(now)
}

// withdrawSupport handles a single support withdrawal. It updates the
// inProgress view of supporterStateForUpdate only if there are any changes.
func (ssfu *supporterStateForUpdate) withdrawSupport(now hlc.ClockTimestamp) {
	for id := range ssfu.checkedIn.supportFor {
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
