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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var Enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.store_liveness.enabled",
	"if enabled, store liveness will heartbeat periodically; if disabled, "+
		"store liveness will still respond to heartbeats and calls to SupportFor.",
	true,
)

// TODO(mira): These should go into a config instead.

var LivenessInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.liveness_interval.",
	"determines the store liveness support expiration time",
	6 * time.Second, // store.cfg.RangeLeaseDuration
)

var HeartbeatInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.heartbeat_interval.",
	"determines how often store liveness broadcasts heartbeats",
	3 * time.Second, // store.cfg.RangeLeaseRenewalDuration()
)

var SupportExpiryInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.store_liveness.support_expiry_interval.",
	"determines how often store liveness checks if support should be withdrawn",
	1 * time.Second,
)

type SupportManager struct {
	storeID      slpb.StoreIdent
	engine       storage.Engine
	settings     *clustersettings.Settings
	stopper      *stop.Stopper
	clock        *hlc.Clock
	transport    *Transport
	receiveQueue receiveQueue
	ss           supporterStateHandler
	rs           requesterStateHandler
	loopStopper  chan struct{}
}

func (sm *SupportManager) Enabled(ctx context.Context) bool {
	clusterSettingEnabled := Enabled.Get(&sm.settings.SV)
	versionGateEnabled := sm.settings.Version.IsActive(ctx, clusterversion.V24_3_StoreLivenessEnabled)
	return clusterSettingEnabled && versionGateEnabled
}

func NewSupportManager(
	ctx context.Context,
	storeID slpb.StoreIdent,
	engine storage.Engine,
	settings *clustersettings.Settings,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	transport *Transport,
) (*SupportManager, error) {
	sm := &SupportManager{
		storeID:      storeID,
		engine:       engine,
		settings:     settings,
		stopper:      stopper,
		clock:        clock,
		transport:    transport,
		receiveQueue: makeReceiveQueue(),
	}
	sm.rs = *newRequesterStateHandler()
	sm.ss = *newSupporterStateHandler()
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
	if err := sm.ss.read(ctx, sm.engine); err != nil {
		return err
	}
	if err := sm.rs.read(ctx, sm.engine); err != nil {
		return err
	}
	// Advance the clock to the maximum withdrawal time.
	if err := sm.clock.UpdateAndCheckMaxOffset(
		ctx,
		sm.ss.supporterState.meta.MaxWithdrawn,
	); err != nil {
		return err
	}
	// Wait out the previous max requested time.
	if err := sm.clock.SleepUntil(ctx, sm.rs.requesterState.meta.MaxRequested); err != nil {
		return err
	}
	// Increment the current epoch.
	rsfu := sm.rs.checkOutUpdate()
	rsfu.incrementMaxEpoch()
	if err := rsfu.write(ctx, sm.engine); err != nil {
		return err
	}
	sm.rs.checkInUpdate(rsfu)
	return nil
}

func (sm *SupportManager) HandleMessage(msg slpb.Message) {
	sm.receiveQueue.Append(msg)
}

func (sm *SupportManager) SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool) {
	ss := sm.ss.getSupportFor(id)
	if ss.Expiration.IsEmpty() {
		return ss.Epoch, false
	}
	return ss.Epoch, true
}

func (sm *SupportManager) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp, bool) {
	ss, ok := sm.rs.getSupportFrom(id)
	if !ok {
		// TODO(mira): remove a remote store if SupportFrom hasn't been called in a while.
		sm.rs.addStore(id)
		return 0, hlc.Timestamp{}, false
	}
	if ss.Expiration.IsEmpty() {
		return 0, hlc.Timestamp{}, false
	}
	return ss.Epoch, ss.Expiration, true
}

func (sm *SupportManager) Start(ctx context.Context) error {
	if sm.loopStopper != nil {
		return nil
	}
	sm.loopStopper = make(chan struct{})
	go sm.startLoop(ctx)
	return nil
}

func (sm *SupportManager) Stop() {
	if sm.loopStopper == nil {
		return
	}
	close(sm.loopStopper)
	sm.loopStopper = nil
}

func (sm *SupportManager) startLoop(ctx context.Context) {
	loopStopper := sm.loopStopper
	var heartbeatTimer timeutil.Timer
	var supportExpiryTimer timeutil.Timer
	defer heartbeatTimer.Stop()
	defer supportExpiryTimer.Stop()
	heartbeatTimer.Reset(HeartbeatInterval.Get(&sm.settings.SV))
	supportExpiryTimer.Reset(SupportExpiryInterval.Get(&sm.settings.SV))

	for {
		// NOTE: only listen to the receive queue's signal if we don't already have
		// heartbeats to send or support to check. This prevents a constant flow of
		// inbound messages from starving out the other work.
		var receiveQueueSig <-chan struct{}
		if len(heartbeatTimer.C) == 0 && len(supportExpiryTimer.C) == 0 {
			receiveQueueSig = sm.receiveQueue.Sig()
		}

		select {
		case <-sm.stopper.ShouldQuiesce():
			return

		case <-loopStopper:
			return

		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
			heartbeatTimer.Reset(HeartbeatInterval.Get(&sm.settings.SV))
			if sm.Enabled(ctx) {
				sm.sendHeartbeats(ctx)
			}

		case <-supportExpiryTimer.C:
			supportExpiryTimer.Read = true
			supportExpiryTimer.Reset(SupportExpiryInterval.Get(&sm.settings.SV))
			sm.withdrawSupport(ctx)

		case <-receiveQueueSig:
			msgs := sm.receiveQueue.Drain()
			sm.handleMessages(ctx, msgs)
		}
	}
}

func (sm *SupportManager) sendHeartbeats(ctx context.Context) {
	rsfu := sm.rs.checkOutUpdate()
	livenessInterval := LivenessInterval.Get(&sm.settings.SV)
	heartbeats := rsfu.getHeartbeatsToSend(sm.storeID, sm.clock.Now(), livenessInterval)
	if err := rsfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write requester meta: %v", err)
		return
	}
	sm.rs.checkInUpdate(rsfu)

	// Send heartbeats to each remote store.
	for _, msg := range heartbeats {
		sent := sm.transport.SendAsync(msg)
		if sent {
			log.VInfof(
				ctx, 2, "sent heartbeat to store %+v, with epoch %+v and expiration %+v",
				msg.To, msg.Epoch, msg.Expiration,
			)
		} else {
			log.Warningf(ctx, "sending heartbeat to store %+v failed", msg.To)
		}
	}
	log.Infof(
		ctx, "store %d sent heartbeats to %d remote stores", sm.storeID,
		len(sm.rs.requesterState.supportFrom),
	)
}

func (sm *SupportManager) withdrawSupport(ctx context.Context) {
	ssfu := sm.ss.checkOutUpdate()
	ssfu.withdrawSupport(sm.clock.NowAsClockTimestamp())
	if err := ssfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write supporter meta: %v", err)
	}
	sm.ss.checkInUpdate(ssfu)
	log.VInfof(ctx, 2, "withdrew some store liveness support")
}

func (sm *SupportManager) handleMessages(ctx context.Context, msgs []slpb.Message) {
	log.VInfof(ctx, 2, "drained receive queue of size %d", len(msgs))
	rsfu := sm.rs.checkOutUpdate()
	ssfu := sm.ss.checkOutUpdate()
	var responses []slpb.Message
	for _, msg := range msgs {
		switch msg.Type {
		case slpb.MsgHeartbeat:
			responses = append(responses, ssfu.handleHeartbeat(msg))
		case slpb.MsgHeartbeatResp:
			rsfu.handleHeartbeatResponse(msg)
		default:
			log.Errorf(context.Background(), "unexpected message type: %v", msg.Type)
		}
	}

	batch := sm.engine.NewBatch()
	defer batch.Close()
	if err := rsfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write requester meta: %v", err)
	}
	if err := ssfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write supporter meta: %v", err)
	}
	if err := batch.Commit(true /* sync */); err != nil {
		log.Warningf(ctx, "failed to sync supporter and requester state: %v", err)
		return
	}
	sm.rs.checkInUpdate(rsfu)
	sm.ss.checkInUpdate(ssfu)

	for _, r := range responses {
		_ = sm.transport.SendAsync(r)
	}
	log.VInfof(ctx, 2, "sent %d responses", len(responses))
}

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
