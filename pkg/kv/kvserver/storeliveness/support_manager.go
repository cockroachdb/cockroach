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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var StoreLivenessEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.storeliveness.enabled",
	"if enabled, store liveness will heartbeat periodically",
	true,
)

type receiveQueue struct {
	mu struct { // not to be locked directly
		syncutil.Mutex
		msgs []slpb.Message
	}
	sig chan struct{}
	// maxLen
}

func makeReceiveQueue() receiveQueue {
	return receiveQueue{
		sig: make(chan struct{}, 1),
	}
}

func (q *receiveQueue) Append(msg slpb.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.msgs = append(q.mu.msgs, msg)
	select {
	case q.sig <- struct{}{}:
	default:
	}
}

func (q *receiveQueue) Sig() <-chan struct{} {
	return q.sig
}

func (q *receiveQueue) Drain() []slpb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	msgs := q.mu.msgs
	q.mu.msgs = nil
	return msgs
}

type Options struct {
	// How often heartbeats are sent out to all remote stores.
	HeartbeatInterval time.Duration
	// How much ahead of the current time we ask for support.
	LivenessInterval time.Duration
	// How often we check for support expiry.
	SupportExpiryInterval time.Duration
}

type SupportManager struct {
	storeID      slpb.StoreIdent
	engine       storage.Engine
	options      Options
	stopper      *stop.Stopper
	clock        *hlc.Clock
	transport    *Transport
	receiveQueue receiveQueue
	// TODO(mira): For production, or even before, when persistence is added to
	// the algorithm, double-check all locking and ensure mutexes are not held
	// during local disk IO.
	// Protects supportMap.
	ss supporterState
	rs requesterState
	// Metrics
	loopStopper *stop.Stopper
}

func NewSupportManager(
	ctx context.Context,
	storeID slpb.StoreIdent,
	engine storage.Engine,
	options Options,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	transport *Transport,
) (*SupportManager, error) {
	sm := &SupportManager{
		storeID:      storeID,
		engine:       engine,
		options:      options,
		stopper:      stopper,
		clock:        clock,
		transport:    transport,
		receiveQueue: makeReceiveQueue(),
	}

	if err := sm.onRestart(ctx); err != nil {
		return nil, err
	}
	if err := sm.Start(ctx); err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *SupportManager) onRestart(ctx context.Context) error {
	// Load the supporter and requester state from disk.
	if err := sm.ss.load(ctx, sm.engine); err != nil {
		return err
	}
	if err := sm.rs.load(ctx, sm.engine); err != nil {
		return err
	}
	// Advance our clock to the maximum withdrawal time.
	if err := sm.clock.UpdateAndCheckMaxOffset(ctx, sm.ss.meta.MaxWithdrawal); err != nil {
		return err
	}
	// Wait out the previous max requested time.
	if err := sm.clock.SleepUntil(ctx, sm.rs.meta.MaxRequested); err != nil {
		return err
	}
	// Increment the current epoch.
	rsv := makeRequesterStateVolatile(&sm.rs)
	rsv.incrementEpoch()
	if err := rsv.write(ctx, sm.engine); err != nil {
		return err
	}
	rsv.updateDurable()
	return nil
}

func (sm *SupportManager) HandleMessage(ctx context.Context, msg slpb.Message) {
	sm.receiveQueue.Append(msg)
}

func (sm *SupportManager) Start(ctx context.Context) error {
	sm.loopStopper = stop.NewStopper()
	return sm.loopStopper.RunAsyncTaskEx(
		ctx, stop.TaskOpts{TaskName: "storeliveness-loop"}, func(context.Context) {
			sm.startLoop(ctx)
		})
}

func (sm *SupportManager) Stop(ctx context.Context) {
	sm.loopStopper.Stop(ctx)
}

func (sm *SupportManager) startLoop(ctx context.Context) {
	var heartbeatTimer timeutil.Timer
	var supportExpiryInterval timeutil.Timer
	defer heartbeatTimer.Stop()
	defer supportExpiryInterval.Stop()
	// TODO(mira): make these intervals cluster settings.
	heartbeatTimer.Reset(sm.options.HeartbeatInterval)
	supportExpiryInterval.Reset(sm.options.SupportExpiryInterval)

	for {
		// NOTE: only listen to the receive queue's signal if we don't already have
		// heartbeats to send or support to check. This prevents a constant flow of
		// inbound messages from starving out the other work.
		var receiveQueueSig <-chan struct{}
		if len(heartbeatTimer.C) == 0 && len(supportExpiryInterval.C) == 0 {
			receiveQueueSig = sm.receiveQueue.Sig()
		}

		select {
		case <-sm.stopper.ShouldQuiesce():
			return

		case <-sm.loopStopper.ShouldQuiesce():
			return

		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
			heartbeatTimer.Reset(sm.options.HeartbeatInterval)
			sm.sendHeartbeats(ctx)

		case <-supportExpiryInterval.C:
			supportExpiryInterval.Read = true
			supportExpiryInterval.Reset(sm.options.SupportExpiryInterval)
			sm.withdrawSupport(ctx)

		case <-receiveQueueSig:
			msgs := sm.receiveQueue.Drain()
			sm.handleMessages(ctx, msgs)
		}
	}
}

func (sm *SupportManager) sendHeartbeats(ctx context.Context) {
	// Persist a new requester state with an updated MaxRequested timestamp.
	rsv := makeRequesterStateVolatile(&sm.rs)
	rsv.updateMaxRequested(sm.clock.Now(), sm.options.LivenessInterval)
	if err := rsv.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write requester state: %v", err)
		return
	}
	rsv.updateDurable()

	// Send heartbeats to each remote store.
	for _, ss := range sm.rs.supportFrom {
		sm.sendHeartbeat(ctx, ss.Target, ss.Epoch, sm.rs.meta.MaxRequested)
	}
	log.Infof(ctx, "store %d sent heartbeats to %d remote stores", sm.storeID, len(sm.rs.supportFrom))
}

func (sm *SupportManager) sendHeartbeat(
	ctx context.Context, to slpb.StoreIdent, epoch slpb.Epoch, endTime hlc.Timestamp,
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
		log.VInfof(ctx, 2, "sent heartbeat to store %+v, with epoch %+v and expiration %+v",
			to, epoch, endTime)
	} else {
		log.Warningf(ctx, "sending heartbeat to store %+v failed", to)
	}
}

func (sm *SupportManager) withdrawSupport(ctx context.Context) {
	now := sm.clock.NowAsClockTimestamp()
	ssv := makeSupporterStateVolatile(&sm.ss)
	for _, ss := range sm.ss.supportFor {
		ssv.maybeWithdrawSupport(ss.Target, now)
	}

	if !ssv.needsWrite() {
		log.VInfof(ctx, 2, "checked for support withdrawal and found none")
		return
	}

	batch := sm.engine.NewBatch()
	defer batch.Close()
	if err := ssv.write(ctx, batch); err != nil {
		log.Warningf(ctx, "failed to write supporter state: %v", err)
		return
	}
	if err := batch.Commit(true /* sync */); err != nil {
		log.Warningf(ctx, "failed to sync supporter state: %v", err)
		return
	}
	ssv.updateDurable()
	log.VInfof(ctx, 2, "withdrew some store liveness support")
}

func (sm *SupportManager) handleMessages(ctx context.Context, msgs []slpb.Message) {
	log.VInfof(ctx, 2, "drained receive queue of size %d", len(msgs))
	ssv := makeSupporterStateVolatile(&sm.ss)
	rsv := makeRequesterStateVolatile(&sm.rs)
	var resps []slpb.Message
	for _, msg := range msgs {
		switch msg.Type {
		case slpb.MsgHeartbeat:
			resp := sm.handleHeartbeat(ctx, ssv, msg)
			resps = append(resps, resp)
		case slpb.MsgHeartbeatResp:
			sm.handleHeartbeatResp(ctx, rsv, msg)
		default:
			log.Errorf(ctx, "unexpected message type: %v", msg.Type)
		}
	}

	if rsv.needsWrite() || ssv.needsWrite() {
		batch := sm.engine.NewBatch()
		defer batch.Close()
		if err := ssv.write(ctx, batch); err != nil {
			log.Warningf(ctx, "failed to write supporter state: %v", err)
			return
		}
		if err := rsv.write(ctx, batch); err != nil {
			log.Warningf(ctx, "failed to write requester state: %v", err)
			return
		}
		if err := batch.Commit(true /* sync */); err != nil {
			log.Warningf(ctx, "failed to sync supporter and requester state: %v", err)
			return
		}
	}
	ssv.updateDurable()
	rsv.updateDurable()

	for _, resp := range resps {
		_ = sm.transport.SendAsync(resp)
	}
	log.VInfof(ctx, 2, "sent %d responses", len(resps))
}

func (sm *SupportManager) handleHeartbeat(
	ctx context.Context, ssv *supporterStateVolatile, msg slpb.Message,
) (resp slpb.Message) {
	ss, ack := ssv.handleHeartbeat(msg)
	log.VInfof(ctx, 2, "handled heartbeat from %+v with epoch %+v and expiration %+v, ack: %t",
		msg.From, msg.Epoch, msg.EndTime, ack)
	resp = slpb.Message{
		Type: slpb.MsgHeartbeatResp,
		From: sm.storeID,
		To:   ss.Target,
		// TODO(mira): are these values correct?
		Epoch:   ss.Epoch,
		EndTime: ss.EndTime,
		Ack:     ack,
	}
	return resp
}

func (sm *SupportManager) handleHeartbeatResp(
	ctx context.Context, rsv *requesterStateVolatile, msg slpb.Message,
) {
	rsv.handleHeartbeatResp(msg)
	log.VInfof(ctx, 2, "handled heartbeat response from %s with expiration %s and ack %t",
		msg.From, msg.EndTime, msg.Ack)
}

func (sm *SupportManager) SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool) {
	ss, ok := sm.ss.getSupportFor(id)
	if !ok {
		log.VInfof(context.Background(), 2, "attempting to evaluate support for an unknown remote store %+v", id)
		return 0, false
	}
	return ss.Epoch, true
}

func (sm *SupportManager) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, slpb.Expiration, bool) {
	ss, ok := sm.rs.getSupportFrom(id)
	if !ok {
		// TODO(mira): remove a remote store if SupportFrom hasn't been called in a while.
		sm.rs.addStoreIfNotExists(id)
		return 0, slpb.Expiration{}, false
	}
	if ss.EndTime.IsEmpty() {
		return 0, slpb.Expiration{}, false
	}
	return ss.Epoch, slpb.Expiration(ss.EndTime), true
}
