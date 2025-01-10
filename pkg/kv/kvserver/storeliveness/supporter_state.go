// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"sync/atomic"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// supporterState stores the core data structures for providing support.
type supporterState struct {
	// meta stores the SupporterMeta, including the max timestamp at which this
	// store has withdrawn support.
	meta slpb.SupporterMeta
	// supportFor stores the SupportState for each remote store for which this
	// store has provided support.
	supportFor map[slpb.StoreIdent]slpb.SupportState
}

// supporterStateHandler is the main interface for handling support for other
// stores. The typical interactions with supporterStateHandler are:
//   - getSupportFor(id slpb.StoreIdent)
//   - ssfu := checkOutUpdate()
//     ssfu.handleHeartbeat(msg slpb.Message)
//     checkInUpdate(ssfu)
//     finishUpdate(ssfu)
//   - ssfu := checkOutUpdate()
//     ssfu.withdrawSupport(now hlc.ClockTimestamp)
//     checkInUpdate(ssfu)
//     finishUpdate(ssfu)
//
// Only one update can be in progress to ensure that multiple mutation methods
// are not run concurrently.
//
// Adding a store to support is done automatically when a heartbeat from that
// store is first received. Currently, a store is never removed.
type supporterStateHandler struct {
	// supporterState is the source of truth for provided support.
	supporterState supporterState
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
	ssh := &supporterStateHandler{
		supporterState: supporterState{
			meta:       slpb.SupporterMeta{},
			supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
	ssh.update.Store(
		&supporterStateForUpdate{
			checkedIn: &ssh.supporterState,
			inProgress: supporterState{
				meta:       slpb.SupporterMeta{},
				supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
			},
		},
	)
	return ssh
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

// getSupportFor returns the SupportState corresponding to the given store in
// supporterState.supportFor.
func (ssh *supporterStateHandler) getSupportFor(id slpb.StoreIdent) slpb.SupportState {
	ssh.mu.RLock()
	defer ssh.mu.RUnlock()
	return ssh.supporterState.supportFor[id]
}

// getNumSupportFor returns the size of the supporterState.supportFor map.
func (ssh *supporterStateHandler) getNumSupportFor() int {
	ssh.mu.RLock()
	defer ssh.mu.RUnlock()
	return len(ssh.supporterState.supportFor)
}

// exportAllSupportFor exports a copy of all SupportStates from the
// supporterState.supportFor map.
func (ssh *supporterStateHandler) exportAllSupportFor() []slpb.SupportState {
	ssh.mu.RLock()
	defer ssh.mu.RUnlock()
	supportStates := make([]slpb.SupportState, len(ssh.supporterState.supportFor))
	for _, ss := range ssh.supporterState.supportFor {
		supportStates = append(supportStates, ss)
	}
	return supportStates
}

// Functions for handling supporterState updates.

// assertMeta ensures the meta in the inProgress view does not regress any of
// the meta fields in the checkedIn view.
func (ssfu *supporterStateForUpdate) assertMeta() {
	assert(
		ssfu.checkedIn.meta.MaxWithdrawn.LessEq(ssfu.inProgress.meta.MaxWithdrawn),
		"max withdrawn regressed during update",
	)
}

// getMeta returns the SupporterMeta from the inProgress view; if not present,
// it falls back to the SupporterMeta from the checkedIn view. If there are
// fields in inProgress.meta that were not modified in this update, they will
// contain the values from checkedIn.meta.
func (ssfu *supporterStateForUpdate) getMeta() slpb.SupporterMeta {
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		ssfu.assertMeta()
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
	clear(ssfu.inProgress.supportFor)
}

// write writes the supporter meta and supportFor to disk if they changed in
// this update. Accepts a batch to avoid potentially writing multiple support
// states separately.
func (ssfu *supporterStateForUpdate) write(ctx context.Context, b storage.Batch) error {
	if ssfu.inProgress.meta == (slpb.SupporterMeta{}) && len(ssfu.inProgress.supportFor) == 0 {
		return nil
	}
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		ssfu.assertMeta()
		if err := writeSupporterMeta(ctx, b, ssfu.inProgress.meta); err != nil {
			return err
		}
	}
	for _, ss := range ssfu.inProgress.supportFor {
		if err := writeSupportForState(ctx, b, ss); err != nil {
			return err
		}
	}
	return nil
}

// read reads the supporter meta and supportFor from disk and populates them in
// supporterStateHandler.supporterState.
func (ssh *supporterStateHandler) read(ctx context.Context, r storage.Reader) error {
	meta, err := readSupporterMeta(ctx, r)
	if err != nil {
		return err
	}
	supportFor, err := readSupportForState(ctx, r)
	if err != nil {
		return err
	}
	ssh.mu.Lock()
	defer ssh.mu.Unlock()
	ssh.supporterState.meta = meta
	ssh.supporterState.supportFor = make(map[slpb.StoreIdent]slpb.SupportState, len(supportFor))
	for _, s := range supportFor {
		ssh.supporterState.supportFor[s.Target] = s
	}
	return nil
}

// checkOutUpdate returns the supporterStateForUpdate referenced in
// supporterStateHandler.update and replaces it with a nil pointer to ensure it
// cannot be checked out concurrently as part of another mutation.
func (ssh *supporterStateHandler) checkOutUpdate() *supporterStateForUpdate {
	ssfu := ssh.update.Swap(nil)
	if ssfu == nil {
		panic("unsupported concurrent update")
	}
	return ssfu
}

// checkInUpdate updates the checkedIn view of supporterStateForUpdate with any
// updates from the inProgress view. It clears the inProgress view, and swaps it
// back in supporterStateHandler.update to be checked out by future updates.
func (ssh *supporterStateHandler) checkInUpdate(ssfu *supporterStateForUpdate) {
	if ssfu.inProgress.meta == (slpb.SupporterMeta{}) && len(ssfu.inProgress.supportFor) == 0 {
		return
	}
	ssh.mu.Lock()
	defer ssh.mu.Unlock()
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		ssfu.assertMeta()
		ssfu.checkedIn.meta = ssfu.inProgress.meta
	}
	for storeID, ss := range ssfu.inProgress.supportFor {
		ssfu.checkedIn.supportFor[storeID] = ss
	}
}

// finishUpdate performs cleanup after a successful or unsuccessful
// checkInUpdate. It resets the supporterStateForUpdate in-progress state and
// makes it available for future check out.
func (ssh *supporterStateHandler) finishUpdate(ssfu *supporterStateForUpdate) {
	ssfu.reset()
	ssh.update.Swap(ssfu)
}

// Functions for handling heartbeats.

// handleHeartbeat handles a single heartbeat message. It updates the inProgress
// view of supporterStateForUpdate only if there are any changes, and returns
// a heartbeat response message.
func (ssfu *supporterStateForUpdate) handleHeartbeat(
	ctx context.Context, msg *slpb.Message,
) slpb.Message {
	from := msg.From
	ss, ok := ssfu.getSupportFor(from)
	if !ok {
		ss = slpb.SupportState{Target: from}
	}
	ssNew := handleHeartbeat(ss, msg)
	if ss != ssNew {
		ssfu.inProgress.supportFor[from] = ssNew
		logSupportForChange(ctx, ss, ssNew)
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
func handleHeartbeat(ss slpb.SupportState, msg *slpb.Message) slpb.SupportState {
	assert(!msg.Expiration.IsEmpty(), "requested support with zero expiration")
	if ss.Epoch == msg.Epoch {
		ss.Expiration.Forward(msg.Expiration)
	} else if ss.Epoch < msg.Epoch {
		assert(ss.Expiration.Less(msg.Expiration), "support expiration regression across epochs")
		ss.Epoch = msg.Epoch
		ss.Expiration = msg.Expiration
	}
	return ss
}

// logSupportForChange logs the old and new support state after handling a
// heartbeat.
func logSupportForChange(ctx context.Context, ss slpb.SupportState, ssNew slpb.SupportState) {
	assert(!ssNew.Expiration.IsEmpty(), "requested support with zero expiration")
	if ss.Epoch == ssNew.Epoch && !ss.Expiration.IsEmpty() {
		if log.ExpensiveLogEnabled(ctx, 3) {
			log.VInfof(ctx, 3, "extended support for %s", supportChangeStr(ss, ssNew))
		}
	} else {
		log.Infof(ctx, "provided support for %s", supportChangeStr(ss, ssNew))
	}
}

// Functions for withdrawing support.

// withdrawSupport handles a single support withdrawal. It updates the
// inProgress view of supporterStateForUpdate only if there are any changes.
// The function returns the store IDs for which support was withdrawn.
func (ssfu *supporterStateForUpdate) withdrawSupport(
	ctx context.Context, now hlc.ClockTimestamp,
) (supportWithdrawnForStoreIDs map[roachpb.StoreID]struct{}) {
	// Assert that there are no updates in ssfu.inProgress.supportFor to make
	// sure we can iterate over ssfu.checkedIn.supportFor in the loop below.
	assert(
		len(ssfu.inProgress.supportFor) == 0, "reading from supporterStateForUpdate."+
			"checkedIn.supportFor while supporterStateForUpdate.inProgress.supportFor is not empty",
	)
	supportWithdrawnForStoreIDs = make(map[roachpb.StoreID]struct{})
	for id, ss := range ssfu.checkedIn.supportFor {
		ssNew := maybeWithdrawSupport(ss, now)
		if ss != ssNew {
			ssfu.inProgress.supportFor[id] = ssNew
			log.Infof(ctx, "withdrew support for %s", supportChangeStr(ss, ssNew))
			meta := ssfu.getMeta()
			if meta.MaxWithdrawn.Forward(now) {
				ssfu.inProgress.meta = meta
			}
			supportWithdrawnForStoreIDs[id.StoreID] = struct{}{}
		}
	}
	return supportWithdrawnForStoreIDs
}

// maybeWithdrawSupport contains the core logic for updating the epoch and
// expiration of a support requester when withdrawing support.
func maybeWithdrawSupport(ss slpb.SupportState, now hlc.ClockTimestamp) slpb.SupportState {
	if !ss.Expiration.IsEmpty() && ss.Expiration.LessEq(now.ToTimestamp()) {
		ss.Epoch++
		ss.Expiration = hlc.Timestamp{}
	}
	return ss
}
