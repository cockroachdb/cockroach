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
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// requesterStateHandler is the main interface for handling support from other
// stores. The typical interactions with requesterStateHandler are:
//   - getSupportFrom(id slpb.StoreIdent)
//   - addStore(id slpb.StoreIdent)
//   - removeStore(id slpb.StoreIdent)
//   - update := checkOutUpdate()
//     update.getHeartbeatsToSend(now hlc.Timestamp,	interval time.Duration)
//     checkInUpdate(update)
//   - update := checkOutUpdate()
//     update.handleHeartbeatResponse(msg slpb.Message)
//     checkInUpdate(update)
//
// Only one update can be in progress to ensure that multiple mutation methods
// are not run concurrently. Adding or removing a store while an update is in
// progress is not allowed.
type requesterStateHandler struct {
	// requesterState is the source of truth for requested support.
	requesterState *requesterState
	// mu controls access to requesterState. The access pattern to requesterState
	// is single writer, multi reader. Concurrent reads come from API calls to
	// SupportFrom; these require RLocking mu. Updates to requesterState are done
	// from a single goroutine; this requires Locking mu when writing the updates.
	// These updates also read from requesterState but there is no need to RLock
	// mu during these reads (since there are no concurrent writes).
	mu syncutil.RWMutex
	// update is a reference to an in-progress change in requesterStateForUpdate.
	// A non-nil update implies there is no ongoing update; i.e. the referenced
	// requesterStateForUpdate is available to be checked out.
	update atomic.Pointer[requesterStateForUpdate]
}

func newRequesterStateHandler() *requesterStateHandler {
	rsh := requesterStateHandler{
		requesterState: &requesterState{
			meta:        slpb.RequesterMeta{MaxEpoch: 1},
			supportFrom: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
	rsh.update.Store(
		&requesterStateForUpdate{
			checkedIn: rsh.requesterState,
			inProgress: requesterState{
				meta:        slpb.RequesterMeta{},
				supportFrom: make(map[slpb.StoreIdent]slpb.SupportState),
			},
		},
	)
	return &rsh
}

// requesterStateForUpdate is a helper struct that facilitates updates to
// requesterState. It is necessary only for batch updates where the individual
// updates need to see each other's changes, while concurrent calls to
// SupportFrom see the persisted-to-disk view until the in-progress batch is
// successfully persisted.
type requesterStateForUpdate struct {
	// checkedIn is a reference to the original requesterState struct stored in
	// requesterStateHandler. It is used to respond to calls to SupportFrom (while
	// an update is in progress) to provide a response consistent with the state
	// persisted on disk.
	checkedIn *requesterState
	// inProgress holds all the updates to requesterState that are in progress and
	// have not yet been reflected in the checkedIn view. The inProgress view
	// ensures that ongoing updates from the same batch see each other's changes.
	inProgress requesterState
}

// requesterState stores the core data structures for requesting support.
type requesterState struct {
	// meta stores the RequesterMeta, including the max timestamp and max epoch at
	// which this store has requested support.
	meta slpb.RequesterMeta
	// supportFrom stores the SupportState for each remote store from which this
	// store has received support.
	supportFrom map[slpb.StoreIdent]slpb.SupportState
}

// getSupportFrom returns the SupportState corresponding to the given store in
// requesterState.supportFrom. The returned boolean indicates whether the given
// store is present in the supportFrom map; it does NOT indicate whether support
// from that store is provided.
func (rsh *requesterStateHandler) getSupportFrom(id slpb.StoreIdent) (slpb.SupportState, bool) {
	rsh.mu.RLock()
	defer rsh.mu.RUnlock()
	ss, ok := rsh.requesterState.supportFrom[id]
	return ss, ok
}

// addStore adds a store to the requesterState.supportFrom map, if not present.
// Adding a store doesn't require persisting anything to disk, so it doesn't
// need to go through the checkOut/checkIn process.
func (rsh *requesterStateHandler) addStore(id slpb.StoreIdent) {
	// Adding a store while there's an ongoing update is not allowed.
	if rsh.updateInProgress() {
		panic("unsupported addStore with a concurrent update")
	}
	rsh.mu.Lock()
	defer rsh.mu.Unlock()
	if _, ok := rsh.requesterState.supportFrom[id]; !ok {
		ss := slpb.SupportState{Target: id, Epoch: rsh.requesterState.meta.MaxEpoch}
		rsh.requesterState.supportFrom[id] = ss
	}
}

// removeStore removes a store from the requesterState.supportFrom map.
// Removing a store doesn't require persisting to disk, so it doesn't need to go
// through the checkOut/checkIn process.
func (rsh *requesterStateHandler) removeStore(id slpb.StoreIdent) {
	// Removing a store while there's an ongoing update is not allowed.
	if rsh.updateInProgress() {
		panic("unsupported removeStore with a concurrent update")
	}
	rsh.mu.Lock()
	defer rsh.mu.Unlock()
	delete(rsh.requesterState.supportFrom, id)
}

// Functions for handling requesterState updates.

// getMeta returns the RequesterMeta from the inProgress view; if not present,
// it falls back to the RequesterMeta from the checkedIn view.
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
		ss, ok = rsfu.checkedIn.supportFrom[storeID]
	}
	return ss, ok
}

// reset clears the inProgress view of requesterStateForUpdate.
func (rsfu *requesterStateForUpdate) reset() {
	rsfu.inProgress.meta = slpb.RequesterMeta{}
	rsfu.inProgress.supportFrom = make(map[slpb.StoreIdent]slpb.SupportState)
}

// updateInProgress determines if there is an update in progress.
func (rsh *requesterStateHandler) updateInProgress() bool {
	return rsh.update.Load() == nil
}

// checkOutUpdate returns the requesterStateForUpdate referenced in
// requesterStateHandler.update and replaces it with a nil pointer to ensure it
// cannot be checked out concurrently as part of another mutation.
func (rsh *requesterStateHandler) checkOutUpdate() *requesterStateForUpdate {
	if rsh.updateInProgress() {
		panic("unsupported concurrent update")
	}
	return rsh.update.Swap(nil)
}

// checkInUpdate updates the checkedIn view of requesterStateForUpdate with any
// updates from the inProgress view. It clears the inProgress view, and swaps it
// back in requesterStateHandler.update to be checked out by future updates.
func (rsh *requesterStateHandler) checkInUpdate(forUpdate *requesterStateForUpdate) {
	defer func() {
		forUpdate.reset()
		rsh.update.Swap(forUpdate)
	}()
	if forUpdate.inProgress.meta == (slpb.RequesterMeta{}) &&
		len(forUpdate.inProgress.supportFrom) == 0 {
		return
	}
	rsh.mu.Lock()
	defer rsh.mu.Unlock()
	if forUpdate.inProgress.meta != (slpb.RequesterMeta{}) {
		if !forUpdate.inProgress.meta.MaxRequested.IsEmpty() {
			forUpdate.checkedIn.meta.MaxRequested = forUpdate.inProgress.meta.MaxRequested
		}
		if forUpdate.inProgress.meta.MaxEpoch != 0 {
			forUpdate.checkedIn.meta.MaxEpoch = forUpdate.inProgress.meta.MaxEpoch
		}
	}
	for storeID, ss := range forUpdate.inProgress.supportFrom {
		forUpdate.checkedIn.supportFrom[storeID] = ss
	}
}

// Functions for generating heartbeats.

// getHeartbeatsToSend updates MaxRequested and generates heartbeats.
func (rsfu *requesterStateForUpdate) getHeartbeatsToSend(
	from slpb.StoreIdent, now hlc.Timestamp, interval time.Duration,
) []slpb.Message {
	rsfu.updateMaxRequested(now, interval)
	return rsfu.generateHeartbeats(from)
}

// updateMaxRequested forwards the current MaxRequested timestamp to now + interval,
// where now is the node's clock timestamp and interval is the liveness interval.
func (rsfu *requesterStateForUpdate) updateMaxRequested(now hlc.Timestamp, interval time.Duration) {
	newMaxRequested := now.Add(interval.Nanoseconds(), 0)
	if rsfu.getMeta().MaxRequested.Less(newMaxRequested) {
		rsfu.inProgress.meta.MaxRequested.Forward(newMaxRequested)
	}
}

func (rsfu *requesterStateForUpdate) generateHeartbeats(from slpb.StoreIdent) []slpb.Message {
	heartbeats := make([]slpb.Message, 0, len(rsfu.checkedIn.supportFrom))
	// It's ok to read store IDs directly from rsfu.checkedIn.supportFrom since
	// adding and removing stores is not allowed while there's an update in progress.
	for id := range rsfu.checkedIn.supportFrom {
		ss, ok := rsfu.getSupportFrom(id)
		if !ok {
			continue
		}
		heartbeat := slpb.Message{
			Type:       slpb.MsgHeartbeat,
			From:       from,
			To:         ss.Target,
			Epoch:      ss.Epoch,
			Expiration: rsfu.getMeta().MaxRequested,
		}
		heartbeats = append(heartbeats, heartbeat)
	}
	return heartbeats
}

// Functions for handling heartbeat responses.

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
		assert(
			ss.Epoch == msg.Epoch-1,
			"the supporter epoch leads the requester epoch by more than 1",
		)
		ss.Epoch = msg.Epoch
		assert(
			msg.Expiration == hlc.Timestamp{},
			"the supporter responded with an incremented epoch but non-zero timestamp",
		)
		ss.Expiration = msg.Expiration
	}
	return rm, ss
}

// Functions for incrementing MaxEpoch.

// incrementMaxEpoch increments the inProgress view of MaxEpoch.
func (rsfu *requesterStateForUpdate) incrementMaxEpoch() {
	currentEpoch := rsfu.getMeta().MaxEpoch
	rsfu.inProgress.meta.MaxEpoch = currentEpoch + 1
}

func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
