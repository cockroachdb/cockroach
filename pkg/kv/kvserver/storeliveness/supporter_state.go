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

// The SupportManager will interact with the supporterState in these ways:
//
//  1. Handle heartbeat messages in batch:
//     update := supporterState.checkOutForBatchUpdate()
//     resp1 := update.handleHeartbeat(msg1)
//     resp2 := update.handleHeartbeat(msg2)
//     resp3 := update.handleHeartbeat(msg3)
//     update.checkInBatchUpdate(engine)
//
//  2. Withdraw support in batch:
//     update := supporterState.checkOutBatchUpdate()
//     update.withdrawSupport(store1)
//     update.withdrawSupport(store2)
//     update.withdrawSupport(store3)
//     update.checkInBatchUpdate(engine)
//
//  3. Respond to SupportFrom() calls:
//     supporterState.getSupportFor(storeID)
//
//  4. Adding a store is handled automatically by handleHeartbeat.
//
//  5. We never remove stores from supportFor currently, but maybe we should
//     clean those up eventually, after support has expired a long time ago.
type supporterState struct {
	mu         syncutil.RWMutex
	meta       slpb.SupporterMeta
	supportFor map[slpb.StoreIdent]slpb.SupportState
}

func (ss *supporterState) getSupportFor(id slpb.StoreIdent) (slpb.SupportState, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	s, ok := ss.supportFor[id]
	return s, ok
}

type supporterStateForUpdate struct {
	persisted  *supporterState
	inProgress supporterState
}

func (ssfu *supporterStateForUpdate) getMeta() slpb.SupporterMeta {
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		return ssfu.inProgress.meta
	}
	return ssfu.persisted.meta
}

func (ssfu *supporterStateForUpdate) getSupportFor(
	storeID slpb.StoreIdent,
) (slpb.SupportState, bool) {
	ss, ok := ssfu.inProgress.supportFor[storeID]
	if !ok {
		ss, ok = ssfu.persisted.getSupportFor(storeID)
	}
	return ss, ok
}

func (ss *supporterState) checkOutBatchUpdate() *supporterStateForUpdate {
	return &supporterStateForUpdate{
		persisted: ss,
		inProgress: supporterState{
			supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
}

func (ssfu *supporterStateForUpdate) checkInBatchUpdate() {
	if ssfu.inProgress.meta == (slpb.SupporterMeta{}) && len(ssfu.inProgress.supportFor) == 0 {
		return
	}
	// TODO(mira): persist to disk here.
	ssfu.persisted.mu.Lock()
	defer ssfu.persisted.mu.Unlock()
	if ssfu.inProgress.meta != (slpb.SupporterMeta{}) {
		ssfu.persisted.meta = ssfu.inProgress.meta
	}
	for storeID, ss := range ssfu.inProgress.supportFor {
		ssfu.persisted.supportFor[storeID] = ss
	}
}

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

func handleHeartbeat(ss slpb.SupportState, msg slpb.Message) slpb.SupportState {
	if ss.Epoch <= msg.Epoch {
		ss.Epoch = max(ss.Epoch, msg.Epoch)
		ss.Expiration.Forward(msg.Expiration)
	}
	return ss
}

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

func withdrawSupport(ss slpb.SupportState, now hlc.ClockTimestamp) slpb.SupportState {
	if !ss.Expiration.IsEmpty() && ss.Expiration.Less(now.ToTimestamp()) {
		ss.Epoch++
		ss.Expiration = hlc.Timestamp{}
	}
	return ss
}
