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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type lockedSupportInfo struct {
	mu         syncutil.RWMutex
	epoch      Epoch
	expiration Expiration
}

func (lsi *lockedSupportInfo) getEpochAndExpiration() (Epoch, Expiration) {
	lsi.mu.RLock()
	defer lsi.mu.RUnlock()
	return lsi.epoch, lsi.expiration
}

type supportState struct {
	// A local or remote store.
	storeID   storelivenesspb.StoreIdent
	forSelfBy lockedSupportInfo
	bySelfFor lockedSupportInfo
}

func newSupportState(storeID storelivenesspb.StoreIdent, currentEpoch Epoch) *supportState {
	return &supportState{
		storeID: storeID,
		forSelfBy: lockedSupportInfo{
			epoch: currentEpoch,
		},
	}
}

func (ss *supportState) supportFor() (Epoch, Expiration) {
	return ss.bySelfFor.getEpochAndExpiration()
}

func (ss *supportState) supportFrom() (Epoch, Expiration) {
	return ss.forSelfBy.getEpochAndExpiration()
}

func (ss *supportState) handleHeartbeat(epoch Epoch, expiration Expiration) bool {
	// TODO(mira): persist epoch and expiration if they changed:
	// - before updating the data structures, and
	// - without holding the mutex.
	ss.bySelfFor.mu.Lock()
	defer ss.bySelfFor.mu.Unlock()
	if ss.bySelfFor.epoch <= epoch {
		ss.bySelfFor.epoch = epoch
		if hlc.Timestamp(ss.bySelfFor.expiration).Less(hlc.Timestamp(expiration)) {
			ss.bySelfFor.expiration = expiration
		}
		return true
	}
	return false
}

func (ss *supportState) handleHeartbeatResponse(
	currentEpoch *lockedEpoch,
	ack bool,
	expiration Expiration,
) {
	ss.forSelfBy.mu.Lock()
	defer ss.forSelfBy.mu.Unlock()
	if ack {
		ss.forSelfBy.expiration = expiration
	} else {
		currentEpoch.incrementEpoch()
		ss.forSelfBy.epoch = currentEpoch.getEpoch()
		ss.forSelfBy.expiration = Expiration(hlc.Timestamp{WallTime: 0, Logical: 0})
	}
}

func (ss *supportState) maybeWithdrawSupport(now hlc.Timestamp) {
	// TODO(mira): persist epoch and expiration if they changed:
	// - before updating the data structures, and
	// - without holding the mutex.
	ss.bySelfFor.mu.Lock()
	defer ss.bySelfFor.mu.Unlock()
	if hlc.Timestamp(ss.bySelfFor.expiration).Less(now) {
		ss.bySelfFor.epoch++
		ss.bySelfFor.expiration = Expiration(hlc.Timestamp{WallTime: 0, Logical: 0})
	}
}
