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
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func handleHeartbeat(ss slpb.SupportState, msg slpb.Message) (_ slpb.SupportState, ack bool) {
	if ss.Epoch > msg.Epoch {
		// Old epoch, nack.
		return ss, false
	}
	// Forward epoch.
	ss.Epoch = max(ss.Epoch, msg.Epoch)
	// Forward end time.
	ss.EndTime.Forward(msg.EndTime)
	return ss, true
}

func handleHeartbeatResp(
	rm slpb.RequesterMeta, ss slpb.SupportState, msg slpb.Message,
) (slpb.RequesterMeta, slpb.SupportState) {
	if msg.Ack {
		if ss.Epoch < msg.Epoch {
			panic("unexpected ack for newer epoch")
		}
		if ss.Epoch == msg.Epoch {
			// Forward end time.
			ss.EndTime.Forward(msg.EndTime)
		}
	} else /* if !ack */ {
		if rm.CurrentEpoch < msg.Epoch {
			// Current epoch needs to be updated.
			rm.CurrentEpoch = msg.Epoch
		}
		if ss.Epoch < msg.Epoch {
			// Update the support state's epoch.
			ss.Epoch = rm.CurrentEpoch
			ss.EndTime = hlc.Timestamp{}
		}
	}
	return rm, ss
}

func maybeWithdrawSupport(ss slpb.SupportState, now hlc.ClockTimestamp) slpb.SupportState {
	if !ss.EndTime.IsEmpty() && ss.EndTime.Less(now.ToTimestamp()) {
		ss.Epoch++
		ss.EndTime = hlc.Timestamp{}
	}
	return ss
}

type supporterState struct {
	mu         syncutil.RWMutex
	meta       slpb.SupporterMeta
	supportFor map[slpb.StoreIdent]slpb.SupportState
}

func (ss *supporterState) load(ctx context.Context, r storage.Reader) error {
	var err error
	ss.meta, err = readSupporterMeta(ctx, r)
	if err != nil {
		return err
	}
	supportFor, err := readSupportForState(ctx, r)
	if err != nil {
		return err
	}
	ss.supportFor = make(map[slpb.StoreIdent]slpb.SupportState, len(supportFor))
	for _, s := range supportFor {
		ss.supportFor[s.Target] = s
	}
	return nil
}

func (ss *supporterState) getSupportFor(id slpb.StoreIdent) (slpb.SupportState, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	s, ok := ss.supportFor[id]
	return s, ok
}

type supporterStateVolatile struct {
	durable  *supporterState
	volatile supporterState
}

func makeSupporterStateVolatile(durable *supporterState) *supporterStateVolatile {
	return &supporterStateVolatile{
		durable: durable,
		volatile: supporterState{
			supportFor: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
}

func (ssv *supporterStateVolatile) getMeta() slpb.SupporterMeta {
	if ssv.volatile.meta != (slpb.SupporterMeta{}) {
		return ssv.volatile.meta
	}
	return ssv.durable.meta
}

func (ssv *supporterStateVolatile) getSupportFor(
	storeID slpb.StoreIdent,
) (slpb.SupportState, bool) {
	ss, ok := ssv.volatile.supportFor[storeID]
	if !ok {
		ss, ok = ssv.durable.supportFor[storeID]
	}
	return ss, ok
}

func (ssv *supporterStateVolatile) handleHeartbeat(
	msg slpb.Message,
) (_ slpb.SupportState, ack bool) {
	from := msg.From
	ss, ok := ssv.getSupportFor(from)
	if !ok {
		ss = slpb.SupportState{Target: from}
	}
	ssNew, ack := handleHeartbeat(ss, msg)
	if ss != ssNew {
		ssv.volatile.supportFor[from] = ssNew
	}
	return ssNew, ack
}

func (ssv *supporterStateVolatile) maybeWithdrawSupport(
	id slpb.StoreIdent, now hlc.ClockTimestamp,
) {
	ss, ok := ssv.getSupportFor(id)
	if !ok {
		return
	}
	ssNew := maybeWithdrawSupport(ss, now)
	if ss != ssNew {
		// Update the volatile support state.
		ssv.volatile.supportFor[id] = ssNew
		// Also, update the supporter meta.
		sm := ssv.getMeta()
		if sm.MaxWithdrawal.Forward(now) {
			ssv.volatile.meta = sm
		}
	}
}

func (ssv *supporterStateVolatile) needsWrite() bool {
	return ssv.volatile.meta != (slpb.SupporterMeta{}) || len(ssv.volatile.supportFor) > 0
}

func (ssv *supporterStateVolatile) write(ctx context.Context, rw storage.ReadWriter) error {
	if ssv.volatile.meta != (slpb.SupporterMeta{}) {
		if err := writeSupporterMeta(ctx, rw, ssv.volatile.meta); err != nil {
			return err
		}
	}
	for _, ss := range ssv.volatile.supportFor {
		if err := writeSupportForState(ctx, rw, ss); err != nil {
			return err
		}
	}
	return nil
}

func (ssv *supporterStateVolatile) updateDurable() {
	ssv.durable.mu.Lock()
	defer ssv.durable.mu.Unlock()
	if ssv.volatile.meta != (slpb.SupporterMeta{}) {
		ssv.durable.meta = ssv.volatile.meta
	}
	for storeID, ss := range ssv.volatile.supportFor {
		ssv.durable.supportFor[storeID] = ss
	}
}

type requesterState struct {
	mu          syncutil.RWMutex
	meta        slpb.RequesterMeta
	supportFrom map[slpb.StoreIdent]slpb.SupportState
}

func (rs *requesterState) load(ctx context.Context, r storage.Reader) error {
	var err error
	rs.meta, err = readRequesterMeta(ctx, r)
	if err != nil {
		return err
	}
	rs.supportFrom = make(map[slpb.StoreIdent]slpb.SupportState)
	return nil
}

func (rs *requesterState) getSupportFrom(id slpb.StoreIdent) (slpb.SupportState, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	ss, ok := rs.supportFrom[id]
	return ss, ok
}

func (rs *requesterState) addStoreIfNotExists(id slpb.StoreIdent) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.supportFrom[id]; !ok {
		rs.supportFrom[id] = slpb.SupportState{Target: id, Epoch: rs.meta.CurrentEpoch}
	}
}

type requesterStateVolatile struct {
	durable  *requesterState
	volatile requesterState
}

func makeRequesterStateVolatile(durable *requesterState) *requesterStateVolatile {
	return &requesterStateVolatile{
		durable: durable,
		volatile: requesterState{
			supportFrom: make(map[slpb.StoreIdent]slpb.SupportState),
		},
	}
}

func (rsv *requesterStateVolatile) getMeta() slpb.RequesterMeta {
	if rsv.volatile.meta != (slpb.RequesterMeta{}) {
		return rsv.volatile.meta
	}
	return rsv.durable.meta
}

func (rsv *requesterStateVolatile) getSupportFrom(
	storeID slpb.StoreIdent,
) (slpb.SupportState, bool) {
	ss, ok := rsv.volatile.supportFrom[storeID]
	if !ok {
		ss, ok = rsv.durable.supportFrom[storeID]
	}
	return ss, ok
}

func (rsv *requesterStateVolatile) handleHeartbeatResp(msg slpb.Message) {
	from := msg.From
	rm := rsv.getMeta()
	ss, ok := rsv.getSupportFrom(from)
	if !ok {
		ss = slpb.SupportState{Target: from}
	}
	rmNew, ssNew := handleHeartbeatResp(rm, ss, msg)
	if rm != rmNew {
		rsv.volatile.meta = rmNew
	}
	if ss != ssNew {
		rsv.volatile.supportFrom[from] = ssNew
	}
}

func (rsv *requesterStateVolatile) incrementEpoch() {
	rm := rsv.getMeta()
	rm.CurrentEpoch++
	rsv.volatile.meta = rm
}

func (rsv *requesterStateVolatile) updateMaxRequested(now hlc.Timestamp, interval time.Duration) {
	rm := rsv.getMeta()
	endTime := now.Add(interval.Nanoseconds(), 0)
	endTime.Forward(rm.MaxRequested)
	if rm.MaxRequested.Forward(endTime) {
		rsv.volatile.meta = rm
	}
}

func (rsv *requesterStateVolatile) needsWrite() bool {
	return rsv.volatile.meta != (slpb.RequesterMeta{})
}

func (rsv *requesterStateVolatile) write(ctx context.Context, rw storage.ReadWriter) error {
	if rsv.volatile.meta != (slpb.RequesterMeta{}) {
		if err := writeRequesterMeta(ctx, rw, rsv.volatile.meta); err != nil {
			return err
		}
	}
	return nil
}

func (rsv *requesterStateVolatile) updateDurable() {
	rsv.durable.mu.Lock()
	defer rsv.durable.mu.Unlock()
	if rsv.volatile.meta != (slpb.RequesterMeta{}) {
		rsv.durable.meta = rsv.volatile.meta
	}
	for storeID, ss := range rsv.volatile.supportFrom {
		rsv.durable.supportFrom[storeID] = ss
	}
}
