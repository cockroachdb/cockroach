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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type lockedEpoch struct {
	mu    syncutil.Mutex
	epoch slpb.Epoch
}

func (le *lockedEpoch) getEpoch() slpb.Epoch {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.epoch
}

func (le *lockedEpoch) incrementEpoch() {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.epoch++
}

type receiveQueue struct {
	mu struct { // not to be locked directly
		syncutil.Mutex
		msgs []slpb.Message
	}
	// maxLen
}

func (q *receiveQueue) Append(msg slpb.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.msgs = append(q.mu.msgs, msg)
}

func (q *receiveQueue) Drain() []slpb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	msgs := q.mu.msgs
	q.mu.msgs = nil
	return msgs
}

type Options struct {
	*hlc.Clock
	// How often heartbeats are sent out to all remote stores.
	HeartbeatInterval time.Duration
	// How much ahead of the current time we ask for support.
	LivenessInterval time.Duration
	// How often we check for support expiry.
	SupportExpiryInterval time.Duration
	// How often we drain the receive queue and handle the responses in it.
	ResponseHandlingInterval time.Duration
}

type SupportManager struct {
	storeID      slpb.StoreIdent
	options      Options
	stopper      *stop.Stopper
	transport    *Transport
	receiveQueue receiveQueue
	// TODO(mira): For production, or even before, when persistence is added to
	// the algorithm, double-check all locking and ensure mutexes are not held
	// during local disk IO.
	// Protects supportMap.
	mu           syncutil.RWMutex
	supportMap   map[slpb.StoreIdent]*supportState
	currentEpoch lockedEpoch
	// Metrics
}

func (sm *SupportManager) getSupportState(remoteStore slpb.StoreIdent) (*supportState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ss, ok := sm.supportMap[remoteStore]
	return ss, ok
}

func NewSupportManager(
	storeID slpb.StoreIdent, options Options, stopper *stop.Stopper, transport *Transport,
) *SupportManager {
	sm := &SupportManager{
		storeID:    storeID,
		options:    options,
		stopper:    stopper,
		transport:  transport,
		supportMap: map[slpb.StoreIdent]*supportState{},
	}
	sm.currentEpoch.incrementEpoch()
	sm.supportMap[storeID] = newSupportState(storeID, sm.currentEpoch.getEpoch())
	// TODO(mira): load and populate persisted state (bySelfFor).
	// For each remote store in supportMap, set:
	// 1. forSelfBy.epoch = currentEpoch,
	// 2. forSelfBy.expiration = empty.
	// TODO(mira): use a stopper here.
	go sm.startLoop(context.Background())
	return sm
}

func (sm *SupportManager) HandleMessage(ctx context.Context, msg slpb.Message) {
	sm.receiveQueue.Append(msg)
}

func (sm *SupportManager) startLoop(ctx context.Context) {
	var heartbeatTimer timeutil.Timer
	var supportExpiryInterval timeutil.Timer
	var responseInterval timeutil.Timer
	defer heartbeatTimer.Stop()
	defer supportExpiryInterval.Stop()
	defer responseInterval.Stop()

	heartbeatTimer.Reset(sm.options.HeartbeatInterval)
	supportExpiryInterval.Reset(sm.options.SupportExpiryInterval)
	responseInterval.Reset(sm.options.ResponseHandlingInterval)

	for {
		select {
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
			// TODO(mira): persist max expiration of the heartbeats about to send.
			for remoteStore, ss := range sm.supportMap {
				epoch, _ := ss.forSelfBy.getEpochAndExpiration()
				endTime := sm.options.Clock.Now().Add(sm.options.LivenessInterval.Nanoseconds(), 0)
				sm.sendHeartbeat(ctx, remoteStore, epoch, slpb.Expiration(endTime))
			}
			log.Infof(ctx, "sent heartbeats to all remote stores")
			heartbeatTimer.Reset(sm.options.HeartbeatInterval)
		case <-supportExpiryInterval.C:
			supportExpiryInterval.Read = true
			for _, ss := range sm.supportMap {
				ss.maybeWithdrawSupport(sm.options.Clock.Now())
			}
			log.Infof(ctx, "checked for support withdrawal")
			supportExpiryInterval.Reset(sm.options.SupportExpiryInterval)
		case <-responseInterval.C:
			responseInterval.Read = true
			msgs := sm.receiveQueue.Drain()
			var resps []slpb.Message
			for _, msg := range msgs {
				switch msg.Type {
				case slpb.MsgHeartbeat:
					resp := sm.handleHeartbeat(ctx, msg)
					resps = append(resps, resp)
				case slpb.MsgHeartbeatResp:
					sm.handleHeartbeatResp(ctx, msg)
				default:
					log.Errorf(ctx, "unexpected message type: %v", msg.Type)
				}
			}
			log.Infof(ctx, "drained receive queue of size %d", len(msgs))
			// TODO(nvanbenschoten): sync to disk.
			for _, resp := range resps {
				_ = sm.transport.SendAsync(resp)
			}
			log.Infof(ctx, "sent %d responses", len(resps))
			responseInterval.Reset(sm.options.ResponseHandlingInterval)
		}
	}
}

func (sm *SupportManager) sendHeartbeat(
	ctx context.Context, to slpb.StoreIdent, epoch slpb.Epoch, endTime slpb.Expiration,
) {
	msg := slpb.Message{
		Type:    slpb.MsgHeartbeat,
		From:    sm.storeID,
		To:      to,
		Epoch:   epoch,
		EndTime: endTime,
	}
	sent := sm.transport.SendAsync(msg)
	if sent {
		log.Infof(ctx, "sent heartbeat to store %+v, with epoch %+v and expiration %+v",
			to, epoch, endTime)
	} else {
		log.Warningf(ctx, "sending heartbeat to store %+v failed", to)
	}
}

func (sm *SupportManager) handleHeartbeat(
	ctx context.Context, msg slpb.Message,
) (resp slpb.Message) {
	fromStore := msg.From
	ss, ok := sm.getSupportState(fromStore)
	if !ok {
		log.Infof(ctx, "received a heartbeat from an unknown remote store %+v", fromStore)
		return
	}
	ack := ss.handleHeartbeat(msg.Epoch, msg.EndTime)
	log.Infof(ctx, "handled heartbeat from %+v with epoch %+v and expiration %+v, ack: %t",
		fromStore, msg.Epoch, msg.EndTime, ack)
	resp = slpb.Message{
		Type: slpb.MsgHeartbeatResp,
		From: sm.storeID,
		To:   fromStore,
		// TODO(mira): are these values correct?
		Epoch:   msg.Epoch,
		EndTime: msg.EndTime,
		Ack:     ack,
	}
	return resp
}

func (sm *SupportManager) handleHeartbeatResp(ctx context.Context, msg slpb.Message) {
	fromStore := msg.From
	ss, ok := sm.getSupportState(fromStore)
	if !ok {
		log.Infof(ctx, "received a heartbeat response from an unknown remote store %+v", fromStore)
		return
	}
	ss.handleHeartbeatResp(&sm.currentEpoch, msg.Epoch, msg.EndTime, msg.Ack)
	log.Infof(ctx, "handled heartbeat response from %s with expiration %s and ack %t",
		fromStore, msg.EndTime, msg.Ack)
}

func (sm *SupportManager) addStore(ctx context.Context, storeID slpb.StoreIdent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.supportMap[storeID]
	if ok {
		log.Infof(ctx, "remote store already exists %+v", storeID)
		return
	}
	sm.supportMap[storeID] = newSupportState(storeID, sm.currentEpoch.getEpoch())
}

func (sm *SupportManager) removeStore(ctx context.Context, storeID slpb.StoreIdent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.supportMap[storeID]
	if !ok {
		log.Infof(ctx, "attempting to remove a missing local store %+v", storeID)
		return
	}
	delete(sm.supportMap, storeID)
}

func (sm *SupportManager) SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool) {
	ss, ok := sm.getSupportState(id)
	if !ok {
		log.Infof(context.Background(), "attempting to evaluate support for an unknown remote store %+v", id)
		return 0, false
	}
	epoch, exp := ss.supportFor()
	if exp.IsEmpty() {
		return 0, false
	}
	return epoch, true
}

func (sm *SupportManager) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, slpb.Expiration, bool) {
	ss, ok := sm.getSupportState(id)
	if !ok {
		log.Infof(context.Background(), "attempting to evaluate support from an unknown remote store %+v; adding remote store", id)
		// TODO(mira): remove a remote store if SupportFrom hasn't been called in a while.
		sm.addStore(context.Background(), id)
		return 0, slpb.Expiration{}, false
	}
	epoch, exp := ss.supportFrom()
	return epoch, exp, !exp.IsEmpty()
}
