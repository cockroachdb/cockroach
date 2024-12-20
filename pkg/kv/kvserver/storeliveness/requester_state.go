// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"sync/atomic"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// requesterState stores the core data structures for requesting support.
type requesterState struct {
	// meta stores the RequesterMeta, including the max timestamp and max epoch at
	// which this store has requested support.
	meta slpb.RequesterMeta
	// supportFrom stores a pointer to requestedSupport for each remote store
	// from which this store has received support.
	supportFrom map[slpb.StoreIdent]*requestedSupport
}

// requestedSupport is a wrapper around SupportState that also indicates whether
// support from the store has been queried via SupportFrom.
type requestedSupport struct {
	// state is the SupportState corresponding to a single store from which
	// support was requested.
	state slpb.SupportState
	// recentlyQueried indicates if support from the store has been queried
	// (by calling SupportFrom) recently. Unlike all other fields in
	// requesterState, updating this field does not require locking
	// requesterStateHandler.mu for writing. This is because recentlyQueried is
	// not updated as part of the support state and meta process; it updated in
	// SupportFrom, so it's important that updating recentlyQueried doesn't lock
	// requesterStateHandler.mu for writing. However, updating recentlyQueried
	// needs to lock requesterStateHandler.mu for reading to ensure that no new
	// stores are added to the supportFrom map.
	recentlyQueried atomic.Int32
}

// recentlyQueried transitions between three possible values to make sure stores
// are not marked as idle prematurely. A store transitions from active to
// inactive every IdleSupportFromInterval and back to active upon being queried
// in a SupportFrom call. If another IdleSupportFromInterval expires after a
// store was marked as inactive, it will be marked as idle and will not be sent
// heartbeats until it transitions to active again.
const (
	// active indicates that the store has been queried in a SupportFrom call
	// recently (within IdleSupportFromInterval).
	active int32 = iota
	// inactive indicates that the store has NOT been queried in a SupportFrom
	// call recently (within IdleSupportFromInterval).
	inactive
	// idle indicates that it has been even longer since the store was queried
	// in a SupportFrom (more than IdleSupportFromInterval).
	idle
)

// requesterStateHandler is the main interface for handling support from other
// stores. The typical interactions with requesterStateHandler are:
//   - getSupportFrom(id slpb.StoreIdent)
//   - addStore(id slpb.StoreIdent)
//   - markIdleStores()
//   - rsfu := checkOutUpdate()
//     rsfu.getHeartbeatsToSend(now hlc.Timestamp, interval time.Duration)
//     checkInUpdate(rsfu)
//     finishUpdate(rsfu)
//   - rsfu := checkOutUpdate()
//     rsfu.handleHeartbeatResponse(msg slpb.Message)
//     checkInUpdate(rsfu)
//     finishUpdate(rsfu)
//
// Only one update can be in progress to ensure that multiple mutation methods
// are not run concurrently. Adding or removing a store while an update is in
// progress is not allowed.
type requesterStateHandler struct {
	// requesterState is the source of truth for requested support.
	requesterState requesterState
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
	rsh := &requesterStateHandler{
		requesterState: requesterState{
			meta:        slpb.RequesterMeta{},
			supportFrom: make(map[slpb.StoreIdent]*requestedSupport),
		},
	}
	rsh.update.Store(
		&requesterStateForUpdate{
			checkedIn: &rsh.requesterState,
			inProgress: requesterState{
				meta:        slpb.RequesterMeta{},
				supportFrom: make(map[slpb.StoreIdent]*requestedSupport),
			},
		},
	)
	return rsh
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

// getSupportFrom returns the SupportState corresponding to the given store in
// requesterState.supportFrom. The returned boolean indicates whether the given
// store is present in the supportFrom map; it does NOT indicate whether support
// from that store is provided.
func (rsh *requesterStateHandler) getSupportFrom(
	id slpb.StoreIdent,
) (supportState slpb.SupportState, exists bool, wasIdle bool) {
	rsh.mu.RLock()
	defer rsh.mu.RUnlock()
	rs, exists := rsh.requesterState.supportFrom[id]
	if exists {
		// If a store is present, set recentlyQueried to true. Otherwise, if
		// this is a new store, recentlyQueried will be set to true in addStore.
		wasIdle = rs.recentlyQueried.Swap(active) == idle
		supportState = rs.state
	}
	return supportState, exists, wasIdle
}

// exportAllSupportFrom exports a copy of all SupportStates from the
// requesterState.supportFrom map.
func (rsh *requesterStateHandler) exportAllSupportFrom() []slpb.SupportState {
	rsh.mu.RLock()
	defer rsh.mu.RUnlock()
	supportStates := make([]slpb.SupportState, len(rsh.requesterState.supportFrom))
	for _, ss := range rsh.requesterState.supportFrom {
		supportStates = append(supportStates, ss.state)
	}
	return supportStates
}

// addStore adds a store to the requesterState.supportFrom map, if not present.
// The function returns a boolean indicating whether the store was added.
func (rsh *requesterStateHandler) addStore(id slpb.StoreIdent) bool {
	// Adding a store doesn't require persisting anything to disk, so it doesn't
	// need to go through the full checkOut/checkIn process. However, we still
	// check out the update to ensure that there are no concurrent updates.
	defer rsh.finishUpdate(rsh.checkOutUpdate())
	rsh.mu.Lock()
	defer rsh.mu.Unlock()
	if _, ok := rsh.requesterState.supportFrom[id]; !ok {
		rs := requestedSupport{
			state: slpb.SupportState{Target: id, Epoch: rsh.requesterState.meta.MaxEpoch},
		}
		// Adding a store is done in response to SupportFrom, so it's ok to set
		// recentlyQueried to active here. This also ensures the store will not
		// be marked as idle immediately after adding.
		rs.recentlyQueried.Store(active)
		rsh.requesterState.supportFrom[id] = &rs
		return true
	}
	return false
}

// markIdleStores marks all stores in the requesterState.supportFrom map as
// idle if they have not appeared in a getSupportFrom call since the last time
// markIdleStores was called.
func (rsh *requesterStateHandler) markIdleStores(ctx context.Context) {
	// Marking stores doesn't require persisting anything to disk, so it doesn't
	// need to go through the full checkOut/checkIn process. However, we still
	// check out the update to ensure that there are no concurrent updates.
	defer rsh.finishUpdate(rsh.checkOutUpdate())

	rsh.mu.RLock()
	defer rsh.mu.RUnlock()
	for _, rs := range rsh.requesterState.supportFrom {
		if !rs.recentlyQueried.CompareAndSwap(active, inactive) {
			if rs.recentlyQueried.CompareAndSwap(inactive, idle) {
				log.Infof(ctx, "stopping heartbeats to idle store %+v", rs.state.Target)
			}
		}
	}
}

// Functions for handling requesterState updates.

// assertMeta ensures the meta in the inProgress view does not regress any of
// the meta fields in the checkedIn view.
func (rsfu *requesterStateForUpdate) assertMeta() {
	assert(
		rsfu.checkedIn.meta.MaxEpoch <= rsfu.inProgress.meta.MaxEpoch,
		"max epoch regressed during update",
	)
	assert(
		rsfu.checkedIn.meta.MaxRequested.LessEq(rsfu.inProgress.meta.MaxRequested),
		"max requested regressed during update",
	)
}

// getMeta returns the RequesterMeta from the inProgress view; if not present,
// it falls back to the RequesterMeta from the checkedIn view. If there are
// fields in inProgress.meta that were not modified in this update, they will
// contain the values from checkedIn.meta.
func (rsfu *requesterStateForUpdate) getMeta() slpb.RequesterMeta {
	if rsfu.inProgress.meta != (slpb.RequesterMeta{}) {
		rsfu.assertMeta()
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
	rs, ok := rsfu.inProgress.supportFrom[storeID]
	if !ok {
		rs, ok = rsfu.checkedIn.supportFrom[storeID]
	}
	var supportState slpb.SupportState
	if ok {
		supportState = rs.state
	}
	return supportState, ok
}

// reset clears the inProgress view of requesterStateForUpdate.
func (rsfu *requesterStateForUpdate) reset() {
	rsfu.inProgress.meta = slpb.RequesterMeta{}
	clear(rsfu.inProgress.supportFrom)
}

// write writes the requester meta to disk if it changed in this update.
func (rsfu *requesterStateForUpdate) write(ctx context.Context, rw storage.ReadWriter) error {
	if rsfu.inProgress.meta == (slpb.RequesterMeta{}) {
		return nil
	}
	rsfu.assertMeta()
	if err := writeRequesterMeta(ctx, rw, rsfu.inProgress.meta); err != nil {
		return err
	}
	return nil
}

// read reads the requester meta from disk and populates it in
// requesterStateHandler.requesterState.
func (rsh *requesterStateHandler) read(ctx context.Context, r storage.Reader) error {
	meta, err := readRequesterMeta(ctx, r)
	if err != nil {
		return err
	}
	rsh.mu.Lock()
	defer rsh.mu.Unlock()
	rsh.requesterState.meta = meta
	return nil
}

// checkOutUpdate returns the requesterStateForUpdate referenced in
// requesterStateHandler.update and replaces it with a nil pointer to ensure it
// cannot be checked out concurrently as part of another mutation.
func (rsh *requesterStateHandler) checkOutUpdate() *requesterStateForUpdate {
	rsfu := rsh.update.Swap(nil)
	if rsfu == nil {
		panic("unsupported concurrent update")
	}
	return rsfu
}

// checkInUpdate updates the checkedIn view of requesterStateForUpdate with any
// updates from the inProgress view. It clears the inProgress view, and swaps it
// back in requesterStateHandler.update to be checked out by future updates.
func (rsh *requesterStateHandler) checkInUpdate(rsfu *requesterStateForUpdate) {
	if rsfu.inProgress.meta == (slpb.RequesterMeta{}) && len(rsfu.inProgress.supportFrom) == 0 {
		return
	}
	rsh.mu.Lock()
	defer rsh.mu.Unlock()
	if rsfu.inProgress.meta != (slpb.RequesterMeta{}) {
		rsfu.assertMeta()
		rsfu.checkedIn.meta = rsfu.inProgress.meta
	}
	for storeID, rs := range rsfu.inProgress.supportFrom {
		rsfu.checkedIn.supportFrom[storeID].state = rs.state
	}
}

// finishUpdate performs cleanup after a successful or unsuccessful
// checkInUpdate. It resets the requesterStateForUpdate in-progress state and
// makes it available for future check out.
func (rsh *requesterStateHandler) finishUpdate(rsfu *requesterStateForUpdate) {
	rsfu.reset()
	rsh.update.Swap(rsfu)
}

// Functions for generating heartbeats.

// getHeartbeatsToSend updates MaxRequested and generates heartbeats. These
// heartbeats must not be sent before the MaxRequested update is persisted to
// disk.
func (rsfu *requesterStateForUpdate) getHeartbeatsToSend(
	from slpb.StoreIdent, now hlc.Timestamp, interval time.Duration,
) []slpb.Message {
	rsfu.updateMaxRequested(now, interval)
	return rsfu.generateHeartbeats(from)
}

// updateMaxRequested forwards the current MaxRequested timestamp to now +
// support duration, where now is the node's clock timestamp.
func (rsfu *requesterStateForUpdate) updateMaxRequested(
	now hlc.Timestamp, supportDuration time.Duration,
) {
	newMaxRequested := now.Add(supportDuration.Nanoseconds(), 0)
	meta := rsfu.getMeta()
	if meta.MaxRequested.Forward(newMaxRequested) {
		// Update the entire meta struct to ensure MaxEpoch is not overwritten.
		rsfu.inProgress.meta = meta
	}
}

func (rsfu *requesterStateForUpdate) generateHeartbeats(from slpb.StoreIdent) []slpb.Message {
	heartbeats := make([]slpb.Message, 0, len(rsfu.checkedIn.supportFrom))
	maxRequested := rsfu.getMeta().MaxRequested
	// Assert that there are no updates in rsfu.inProgress.supportFrom to make
	// sure we can iterate over rsfu.checkedIn.supportFrom in the loop below.
	assert(
		len(rsfu.inProgress.supportFrom) == 0, "reading from requesterStateForUpdate."+
			"checkedIn.supportFrom while requesterStateForUpdate.inProgress.supportFrom is not empty",
	)
	for _, rs := range rsfu.checkedIn.supportFrom {
		// Skip idle stores.
		if rs.recentlyQueried.Load() == idle {
			continue
		}
		heartbeat := slpb.Message{
			Type:       slpb.MsgHeartbeat,
			From:       from,
			To:         rs.state.Target,
			Epoch:      rs.state.Epoch,
			Expiration: maxRequested,
		}
		heartbeats = append(heartbeats, heartbeat)
	}
	return heartbeats
}

// Functions for handling heartbeat responses.

// handleHeartbeatResponse handles a single heartbeat response message. It
// updates the inProgress view of requesterStateForUpdate only if there are any
// changes.
func (rsfu *requesterStateForUpdate) handleHeartbeatResponse(
	ctx context.Context, msg *slpb.Message,
) {
	from := msg.From
	meta := rsfu.getMeta()
	ss, ok := rsfu.getSupportFrom(from)
	// If the store is not present in the map, ignore the heartbeat response;
	// it is likely an old heartbeat response before the local store restarted.
	if !ok {
		return
	}
	metaNew, ssNew := handleHeartbeatResponse(meta, ss, msg)
	if meta != metaNew {
		rsfu.inProgress.meta = metaNew
	}
	if ss != ssNew {
		rsfu.inProgress.supportFrom[from] = &requestedSupport{state: ssNew}
		logSupportFromChange(ctx, ss, ssNew)
	}
}

// handleHeartbeatResponse contains the core logic for updating the epoch and
// expiration for a support provider upon receiving a heartbeat response.
func handleHeartbeatResponse(
	rm slpb.RequesterMeta, ss slpb.SupportState, msg *slpb.Message,
) (slpb.RequesterMeta, slpb.SupportState) {
	if rm.MaxEpoch < msg.Epoch {
		rm.MaxEpoch = msg.Epoch
	}
	if ss.Epoch == msg.Epoch {
		ss.Expiration.Forward(msg.Expiration)
	} else if ss.Epoch < msg.Epoch {
		assert(ss.Epoch == msg.Epoch-1, "epoch incremented by more than 1")
		ss.Epoch = msg.Epoch
		assert(msg.Expiration == hlc.Timestamp{}, "incremented epoch but non-zero timestamp")
		ss.Expiration = msg.Expiration
	}
	return rm, ss
}

// logSupportFromChange logs the old and new support state after handling a
// heartbeat response. The logic mirrors that in handleHeartbeatResponse and
// uses the same assertions.
func logSupportFromChange(ctx context.Context, ss slpb.SupportState, ssNew slpb.SupportState) {
	if ss.Epoch == ssNew.Epoch {
		if ss.Expiration.IsEmpty() {
			log.Infof(ctx, "received support from %s", supportChangeStr(ss, ssNew))
		} else if log.ExpensiveLogEnabled(ctx, 3) {
			log.VInfof(ctx, 3, "extended support from %s", supportChangeStr(ss, ssNew))
		}
	} else {
		assert(ss.Epoch < ssNew.Epoch, "epoch regressed")
		log.Infof(ctx, "lost support from %s", supportChangeStr(ss, ssNew))
	}
}

// Functions for incrementing MaxEpoch.

// incrementMaxEpoch increments the inProgress view of MaxEpoch.
func (rsfu *requesterStateForUpdate) incrementMaxEpoch() {
	meta := rsfu.getMeta()
	meta.MaxEpoch++
	// Update the entire meta struct to ensure MaxRequested is not overwritten.
	rsfu.inProgress.meta = meta
}

func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}

func supportChangeStr(ssOld slpb.SupportState, ssNew slpb.SupportState) redact.RedactableString {
	return redact.Sprintf("store %+v; old = %+v, new = %+v", ssNew.Target, ssOld, ssNew)
}
