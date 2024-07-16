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
	"time"
)

// The SupportManager will interact with the requesterState in these ways:
//
//  1. Handle heartbeat responses in batch:
//     update := requesterState.checkOutBatchUpdate()
//     update.handleHeartbeatResp(msg1)
//     update.handleHeartbeatResp(msg2)
//     update.handleHeartbeatResp(msg3)
//     update.checkInBatchUpdate(engine)
//
//  2. Increment MaxEpoch upon restart:
//     requesterState.incrementMaxEpoch()
//
//  3. Update MaxRequested upon sending heartbeats:
//     requesterState.updateMaxRequested(now, LivenessInterval)
//
//  4. Respond to SupportFrom() calls:
//     requesterState.getSupportFrom(storeID)
//
//  5. Add a new store, triggered by SupportFrom calls.
//
//  6. Remove a store, triggered by no calls to SupportFrom in a while.
type requesterState struct {
	mu          syncutil.RWMutex
	meta        slpb.RequesterMeta
	supportFrom map[slpb.StoreIdent]slpb.SupportState
}

func (rs *requesterState) getSupportFrom(id slpb.StoreIdent) (slpb.SupportState, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	ss, ok := rs.supportFrom[id]
	return ss, ok
}

func (rs *requesterState) addStore(id slpb.StoreIdent) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.supportFrom[id]; !ok {
		rs.supportFrom[id] = slpb.SupportState{Target: id, Epoch: rs.meta.MaxEpoch}
	}
}

func (rs *requesterState) removeStore(id slpb.StoreIdent) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.supportFrom, id)
}

type requesterStateForUpdate struct {
	persisted  *requesterState
	inProgress requesterState
}

func (rsfu *requesterStateForUpdate) getSupportFrom(
	storeID slpb.StoreIdent,
) (slpb.SupportState, bool) {
	ss, ok := rsfu.inProgress.supportFrom[storeID]
	if !ok {
		ss, ok = rsfu.persisted.getSupportFrom(storeID)
	}
	return ss, ok
}

func (rsfu *requesterStateForUpdate) getMeta() slpb.RequesterMeta {
	if rsfu.inProgress.meta != (slpb.RequesterMeta{}) {
		return rsfu.inProgress.meta
	}
	return rsfu.persisted.meta
}

func (rs *requesterState) checkOutBatchUpdate() *requesterStateForUpdate {
	return &requesterStateForUpdate{
		persisted:  rs,
		inProgress: requesterState{meta: slpb.RequesterMeta{}},
	}
}

func (rsfu *requesterStateForUpdate) checkInBatchUpdate() {
	if rsfu.inProgress.meta == (slpb.RequesterMeta{}) {
		return
	}
	// TODO(mira): persist to disk here.
	rsfu.persisted.mu.Lock()
	defer rsfu.persisted.mu.Unlock()
	rsfu.persisted.meta = rsfu.inProgress.meta
}

func (rsfu *requesterStateForUpdate) handleHeartbeatResp(msg slpb.Message) {
	from := msg.From
	rm := rsfu.getMeta()
	ss, ok := rsfu.getSupportFrom(from)
	if !ok {
		ss = slpb.SupportState{Target: from}
	}
	rmNew, ssNew := handleHeartbeatResp(rm, ss, msg)
	if rm != rmNew {
		rsfu.inProgress.meta = rmNew
	}
	if ss != ssNew {
		rsfu.inProgress.supportFrom[from] = ssNew
	}
}

func handleHeartbeatResp(
	rm slpb.RequesterMeta, ss slpb.SupportState, msg slpb.Message,
) (slpb.RequesterMeta, slpb.SupportState) {
	if ss.Epoch == msg.Epoch {
		// Forward end time.
		ss.Expiration.Forward(msg.Expiration)
	}
	if ss.Epoch < msg.Epoch {
		// Update the support state's epoch.
		ss.Epoch = msg.Epoch
		ss.Expiration = hlc.Timestamp{}
	}

	if rm.MaxEpoch < msg.Epoch {
		// Current epoch needs to be updated.
		rm.MaxEpoch = msg.Epoch
	}
	return rm, ss
}

func (rs *requesterState) updateMaxRequested(now hlc.Timestamp, interval time.Duration) {
	newMaxRequested := now.Add(interval.Nanoseconds(), 0)
	newMaxRequested.Forward(rs.meta.MaxRequested)
	if rs.meta.MaxRequested.Less(newMaxRequested) {
		// TODO(mira): persist to disk here.
		rs.mu.Lock()
		defer rs.mu.Unlock()
		rs.meta.MaxRequested.Forward(newMaxRequested)
	}
}

func (rs *requesterState) incrementMaxEpoch() {
	// TODO(mira): persist to disk here.
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.meta.MaxEpoch++
}
