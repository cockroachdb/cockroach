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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type Options struct {
	*hlc.Clock
	HeartbeatInterval time.Duration
	// Some hacky injections for testing.
	callbackScheduler callbackScheduler
}

type SupportManager struct {
	storeID storelivenesspb.StoreIdent
	options Options
	stopper *stop.Stopper
	// TODO(mira): For production, replace this with a scheduler that allows us to
	// avoid a goroutine per remote connection.
	callbackScheduler callbackScheduler
	heartbeatSender   *heartbeatSender
	// TODO(mira): For production, or even before, when persistence is added to
	// the algorithm, double-check all locking and ensure mutexes are not held
	// during local disk IO.
	// Protects heartbeatAndSupportMap.
	mu                     syncutil.RWMutex
	heartbeatAndSupportMap map[storelivenesspb.StoreIdent]*supportState
	// Metrics
}

func NewSupportManager(
	storeID storelivenesspb.StoreIdent,
	options Options,
	stopper *stop.Stopper,
	heartbeatSender heartbeatSender,
) *SupportManager {
	var cs callbackScheduler = &timerCallbackScheduler{stopper}
	if options.callbackScheduler != nil {
		cs = options.callbackScheduler
	}
	sm := &SupportManager{
		storeID:                storeID,
		options:                options,
		stopper:                stopper,
		callbackScheduler:      cs,
		heartbeatSender:        &heartbeatSender,
		heartbeatAndSupportMap: map[storelivenesspb.StoreIdent]*supportState{},
	}
	sm.heartbeatAndSupportMap[storeID] = &supportState{storeID: storeID}
	sm.startHeartbeatLoop(context.Background(), options.HeartbeatInterval, storeID)
	return sm
}

func (sm *SupportManager) HandleHeartbeat(
	req *storelivenesspb.HeartbeatRequest,
) (*storelivenesspb.HeartbeatResponse, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	remoteStore := req.Header.From
	hbs, ok := sm.heartbeatAndSupportMap[remoteStore]
	header := storelivenesspb.Header{
		From:       req.Header.To,
		To:         req.Header.From,
		SenderTime: sm.options.Clock.NowAsClockTimestamp(),
	}
	if !ok {
		log.Infof(context.Background(), "handling a heartbeat request from a missing remote store %+v", remoteStore)
		return &storelivenesspb.HeartbeatResponse{
			Header: header, HeartbeatAck: false,
		}, nil
	}
	ack, err := hbs.handleHeartbeat(Epoch(req.Heartbeat.Epoch), Expiration(req.Heartbeat.EndTime))
	if err != nil {
		return &storelivenesspb.HeartbeatResponse{
			Header: header, HeartbeatAck: false,
		}, err
	}
	return &storelivenesspb.HeartbeatResponse{
		Header: header, HeartbeatAck: ack,
	}, nil
}

// Assumes sm.mu is held.
func (sm *SupportManager) startHeartbeatLoop(
	ctx context.Context,
	heartbeatInterval time.Duration,
	toStore storelivenesspb.StoreIdent) {
	hbs, ok := sm.heartbeatAndSupportMap[toStore]
	if !ok {
		log.Infof(context.Background(), "attempting to start heartbeating a missing remote store %+v", toStore)
		return
	}
	hbs.heartbeatTimerHandle = sm.callbackScheduler.registerCallback(
		fmt.Sprintf("storeliveness.heartbeat %+v", hbs.storeID), func() {
			sm.sendHeartbeat(hbs, toStore, heartbeatInterval)
		})
	sm.sendHeartbeat(hbs, toStore, heartbeatInterval)
}

func (sm *SupportManager) sendHeartbeat(
	hbs *supportState,
	toStore storelivenesspb.StoreIdent,
	heartbeatInterval time.Duration) {
	hbs.heartbeatTimerHandle.runCallbackAfterDuration(heartbeatInterval)
	req := storelivenesspb.HeartbeatRequest{
		Header: storelivenesspb.Header{
			From:       sm.storeID,
			To:         toStore,
			SenderTime: sm.options.Clock.NowAsClockTimestamp(),
		},
	}
	res, err := (*sm.heartbeatSender).SendHeartbeat(&req)
	if err != nil {
		log.Infof(context.Background(), "sending heartbeat to store %+v failed %s", toStore, err)
		return
	}
	log.Infof(context.Background(), "storeliveness: sent heartbeat from %+v, to %+v, interval %s",
		sm.storeID, hbs.storeID, heartbeatInterval.String())
	err = sm.handleHeartbeatResponse(res)
	if err != nil {
		log.Infof(context.Background(), "handling heartbeat response %+v failed %s", res, err)
		return
	}
	log.Infof(context.Background(), "storeliveness: received heartbeat response from %+v, to %+v, interval %s",
		hbs.storeID, sm.storeID, heartbeatInterval.String())
}

// Assumes sm.mu is held.
func (sm *SupportManager) handleHeartbeatResponse(res *storelivenesspb.HeartbeatResponse) error {
	remoteStore := res.Header.From
	hbs, ok := sm.heartbeatAndSupportMap[remoteStore]
	if !ok {
		log.Infof(context.Background(), "handling a heartbeat response from a missing remote store %+v", remoteStore)
		return nil
	}
	return hbs.handleHeartbeatResponse(res.HeartbeatAck)
}

// Assumes sm.mu is held.
func (sm *SupportManager) stopHeartbeatLoop(
	toStore storelivenesspb.StoreIdent) {
	hbs, ok := sm.heartbeatAndSupportMap[toStore]
	if !ok {
		log.Infof(context.Background(), "attempting to stop heartbeating a missing remote store %+v", toStore)
		return
	}
	hbs.heartbeatTimerHandle.unregister()
}

// Assumes sm.mu is held.
func (sm *SupportManager) addStore(storeID storelivenesspb.StoreIdent) {
	sm.heartbeatAndSupportMap[storeID] = &supportState{storeID: storeID}
	sm.startHeartbeatLoop(context.Background(), sm.options.HeartbeatInterval, storeID)
}

// Assumes sm.mu is held.
func (sm *SupportManager) removeStore(storeID storelivenesspb.StoreIdent) {
	_, ok := sm.heartbeatAndSupportMap[storeID]
	if !ok {
		log.Infof(context.Background(), "attempting to remove a missing local store %+v", storeID)
		return
	}
	sm.stopHeartbeatLoop(storeID)
	delete(sm.heartbeatAndSupportMap, storeID)
}

func (sm *SupportManager) SupportFor(id storelivenesspb.StoreIdent) (Epoch, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	hbs, ok := sm.heartbeatAndSupportMap[id]
	if !ok {
		log.Infof(context.Background(), "attempting to evaluate support for an unknown remote store %+v", id)
		return 0, false
	}
	return hbs.supportFor()
}

func (sm *SupportManager) SupportFrom(id storelivenesspb.StoreIdent) (Epoch, Expiration, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	hbs, ok := sm.heartbeatAndSupportMap[id]
	if !ok {
		log.Infof(context.Background(), "attempting to evaluate support from an unknown remote store %+v; adding remote store", id)
		// TODO(mira): remove a remote store if SupportFrom hasn't been called ina while.
		sm.addStore(id)
		return 0, Expiration{WallTime: 0, Logical: 0}, false
	}
	return hbs.supportFrom()
}
