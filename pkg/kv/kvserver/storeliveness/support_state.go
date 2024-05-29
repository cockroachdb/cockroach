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

type lockedSupportInfo struct {
	mu         syncutil.RWMutex
	epoch      slpb.Epoch
	expiration slpb.Expiration
}

func (lsi *lockedSupportInfo) getEpochAndExpiration() (slpb.Epoch, slpb.Expiration) {
	lsi.mu.RLock()
	defer lsi.mu.RUnlock()
	return lsi.epoch, lsi.expiration
}

type supportState struct {
	// A local or remote store.
	storeID   slpb.StoreIdent
	forSelfBy lockedSupportInfo
	bySelfFor lockedSupportInfo
}

func newSupportState(storeID slpb.StoreIdent, curEpoch slpb.Epoch) *supportState {
	return &supportState{
		storeID: storeID,
		forSelfBy: lockedSupportInfo{
			epoch: curEpoch,
		},
	}
}

func (ss *supportState) supportFor() (slpb.Epoch, slpb.Expiration) {
	return ss.bySelfFor.getEpochAndExpiration()
}

func (ss *supportState) supportFrom() (slpb.Epoch, slpb.Expiration) {
	return ss.forSelfBy.getEpochAndExpiration()
}

func (ss *supportState) handleHeartbeat(epoch slpb.Epoch, expiration slpb.Expiration) bool {
	// TODO(mira): persist epoch and expiration if they changed:
	// - before updating the data structures, and
	// - without holding the mutex.
	ss.bySelfFor.mu.Lock()
	defer ss.bySelfFor.mu.Unlock()
	if ss.bySelfFor.epoch > epoch {
		return false
	}
	ss.bySelfFor.epoch = epoch
	ss.bySelfFor.expiration.Forward(expiration)
	return true
}

func (ss *supportState) handleHeartbeatResp(
	currentEpoch *lockedEpoch, epoch slpb.Epoch, expiration slpb.Expiration, ack bool,
) {
	ss.forSelfBy.mu.Lock()
	defer ss.forSelfBy.mu.Unlock()
	if ack {
		ss.forSelfBy.expiration = expiration
	} else {
		currentEpoch.incrementEpoch()
		ss.forSelfBy.epoch = currentEpoch.getEpoch()
		ss.forSelfBy.expiration = slpb.Expiration{}
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
		ss.bySelfFor.expiration = slpb.Expiration{}
	}
}
