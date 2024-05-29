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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type lockedEpoch struct {
	mu    syncutil.Mutex
	epoch Epoch
}

func (le *lockedEpoch) getEpoch() Epoch {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.epoch
}

func (le *lockedEpoch) incrementEpoch() {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.epoch++
}

type heartbeatInfo struct {
	msg        *storelivenesspb.HeartbeatUnion
	respStream HeartbeatResponseStream
}

type receiveQueue struct {
	mu struct { // not to be locked directly
		syncutil.Mutex
		infos []heartbeatInfo
	}
	// maxLen
}

func (q *receiveQueue) Append(
	msg *storelivenesspb.HeartbeatUnion, s HeartbeatResponseStream,
) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.infos = append(q.mu.infos, heartbeatInfo{
		msg:        msg,
		respStream: s,
	})
}

func (q *receiveQueue) Drain() []heartbeatInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	infos := q.mu.infos
	q.mu.infos = nil
	return infos
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
	storeID      storelivenesspb.StoreIdent
	options      Options
	stopper      *stop.Stopper
	transport    *SLTransport
	receiveQueue receiveQueue
	// TODO(mira): For production, or even before, when persistence is added to
	// the algorithm, double-check all locking and ensure mutexes are not held
	// during local disk IO.
	// Protects supportMap.
	mu           syncutil.RWMutex
	supportMap   map[storelivenesspb.StoreIdent]*supportState
	currentEpoch lockedEpoch
	// Metrics
}

func (sm *SupportManager) getSupportState(remoteStore storelivenesspb.StoreIdent) (*supportState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ss, ok := sm.supportMap[remoteStore]
	return ss, ok
}

func NewSupportManager(
	storeID storelivenesspb.StoreIdent,
	options Options,
	stopper *stop.Stopper,
	transport *SLTransport,
) *SupportManager {
	sm := &SupportManager{
		storeID:    storeID,
		options:    options,
		stopper:    stopper,
		transport:  transport,
		supportMap: map[storelivenesspb.StoreIdent]*supportState{},
	}
	sm.currentEpoch.incrementEpoch()
	sm.supportMap[storeID] = newSupportState(storeID, sm.currentEpoch.getEpoch())
	// TODO(mira): load and populate persisted state (bySelfFor).
	// For each remote store in supportMap, set:
	// 1. forSelfBy.epoch = currentEpoch,
	// 2. forSelfBy.expiration = empty.
	go sm.startLoop()
	return sm
}

func (sm *SupportManager) HandleHeartbeatRequest(
	ctx context.Context,
	req *storelivenesspb.HeartbeatRequest,
	respStream HeartbeatResponseStream,
) *kvpb.Error {
	msg := storelivenesspb.HeartbeatUnion{Request: req}
	sm.receiveQueue.Append(&msg, respStream)
	return nil
}

func (sm *SupportManager) HandleHeartbeatResponse(
	ctx context.Context,
	res *storelivenesspb.HeartbeatResponse,
) error {
	msg := storelivenesspb.HeartbeatUnion{Response: res}
	sm.receiveQueue.Append(&msg, nil)
	return nil
}

func (sm *SupportManager) startLoop() {
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
				sm.sendHeartbeat(remoteStore, epoch)
			}
			log.Infof(context.Background(), "sent heartbeats to all remote stores")
			heartbeatTimer.Reset(sm.options.HeartbeatInterval)
		case <-supportExpiryInterval.C:
			supportExpiryInterval.Read = true
			for _, ss := range sm.supportMap {
				ss.maybeWithdrawSupport(sm.options.Clock.Now())
			}
			log.Infof(context.Background(), "checked for support withdrawal")
			supportExpiryInterval.Reset(sm.options.SupportExpiryInterval)
		case <-responseInterval.C:
			responseInterval.Read = true
			infos := sm.receiveQueue.Drain()
			for _, info := range infos {
				if info.msg.Request != nil {
					sm.handleHeartbeatRequest(info.msg.Request, info.respStream)
				}
				if info.msg.Response != nil {
					sm.handleHeartbeatResponse(info.msg.Response)
				}
			}
			log.Infof(context.Background(), "drained receive queue of size %d", len(infos))
			responseInterval.Reset(sm.options.ResponseHandlingInterval)
		}
	}
}

func (sm *SupportManager) sendHeartbeat(
	remoteStore storelivenesspb.StoreIdent,
	epoch Epoch,
) {
	req := newHeartbeatRequest()
	req.Header = storelivenesspb.Header{
		From: sm.storeID,
		To:   remoteStore,
	}
	endTime := sm.options.Clock.Now().Add(sm.options.LivenessInterval.Nanoseconds(), 0)
	req.Heartbeat = storelivenesspb.Heartbeat{
		Epoch:   int64(epoch),
		EndTime: endTime,
	}
	if !sm.transport.SendAsync(req, rpc.SystemClass) {
		log.Infof(context.Background(), "sending heartbeat to store %+v failed", remoteStore)
		return
	}
	log.Infof(context.Background(), "sent heartbeat to store %+v, with epoch %+v and expiration %+v",
		remoteStore, epoch, endTime)
}

func (sm *SupportManager) handleHeartbeatRequest(req *storelivenesspb.HeartbeatRequest, s HeartbeatResponseStream) {
	fromStore := req.Header.From
	ss, ok := sm.getSupportState(fromStore)
	if !ok {
		log.Infof(context.Background(), "received a heartbeat from an unknown remote store %+v", fromStore)
		return
	}
	res := newHeartbeatResponse(req, nil)
	res.HeartbeatAck = ss.handleHeartbeat(Epoch(req.Heartbeat.Epoch), Expiration(req.Heartbeat.EndTime))
	if err := s.Send(res); err != nil {
		log.Infof(context.Background(), "error sending heartbeat response: %+v", err)
	}
	log.Infof(context.Background(), "handled heartbeat request from %+v with epoch %+v and expiration %+v",
		fromStore, req.Heartbeat.Epoch, req.Heartbeat.EndTime)
}

func (sm *SupportManager) handleHeartbeatResponse(res *storelivenesspb.HeartbeatResponse) {
	fromStore := res.Header.From
	ss, ok := sm.getSupportState(fromStore)
	if !ok {
		log.Infof(context.Background(), "received a heartbeat response from an unknown remote store %+v", fromStore)
		return
	}
	ss.handleHeartbeatResponse(&sm.currentEpoch, res.HeartbeatAck, Expiration(res.Heartbeat.EndTime))
	log.Infof(context.Background(), "handled heartbeat response from %+v with ack %+v and expiration %+v",
		fromStore, res.HeartbeatAck, res.Heartbeat.EndTime)
}

func (sm *SupportManager) addStore(storeID storelivenesspb.StoreIdent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.supportMap[storeID]
	if ok {
		log.Infof(context.Background(), "remote store already exists %+v", storeID)
		return
	}
	sm.supportMap[storeID] = newSupportState(storeID, sm.currentEpoch.getEpoch())
}

func (sm *SupportManager) removeStore(storeID storelivenesspb.StoreIdent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.supportMap[storeID]
	if !ok {
		log.Infof(context.Background(), "attempting to remove a missing local store %+v", storeID)
		return
	}
	delete(sm.supportMap, storeID)
}

func (sm *SupportManager) SupportFor(id storelivenesspb.StoreIdent) (Epoch, bool) {
	ss, ok := sm.getSupportState(id)
	if !ok {
		log.Infof(context.Background(), "attempting to evaluate support for an unknown remote store %+v", id)
		return 0, false
	}
	epoch, expiration := ss.supportFor()
	if hlc.Timestamp(expiration).IsEmpty() || hlc.Timestamp(expiration).Less(sm.options.Clock.Now()) {
		return 0, false
	}
	return epoch, true
}

func (sm *SupportManager) SupportFrom(id storelivenesspb.StoreIdent) (Epoch, Expiration, bool) {
	ss, ok := sm.getSupportState(id)
	if !ok {
		log.Infof(context.Background(), "attempting to evaluate support from an unknown remote store %+v; adding remote store", id)
		// TODO(mira): remove a remote store if SupportFrom hasn't been called in a while.
		sm.addStore(id)
		return 0, Expiration{WallTime: 0, Logical: 0}, false
	}
	epoch, expiration := ss.supportFrom()
	if hlc.Timestamp(expiration).IsEmpty() || hlc.Timestamp(expiration).Less(sm.options.Clock.Now()) {
		return 0, Expiration{WallTime: 0, Logical: 0}, false
	}
	return epoch, expiration, true
}
