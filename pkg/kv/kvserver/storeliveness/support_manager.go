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
	"golang.org/x/exp/maps"
)

var Enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.store_liveness.enabled",
	"if enabled, store liveness will heartbeat periodically; if disabled, "+
		"store liveness will still respond to heartbeats and calls to SupportFor",
	true,
)

type Options struct {
	// HeartbeatInterval determines how often Store Liveness sends heartbeats.
	HeartbeatInterval time.Duration
	// LivenessInterval determines the Store Liveness support expiration time.
	LivenessInterval time.Duration
	// SupportExpiryInterval determines how often Store Liveness checks if support
	// should be withdrawn.
	SupportExpiryInterval time.Duration
	// IdleSupportFromInterval determines how ofter Store Liveness checks if any
	// stores have not appeared in a SupportFrom call recently.
	IdleSupportFromInterval time.Duration
	// SupportWithdrawalGracePeriod determines how long Store Liveness should
	// wait after restart before withdrawing support. It helps prevent support
	// churn until the first heartbeats are delivered.
	SupportWithdrawalGracePeriod time.Duration
}

// MessageSender is the interface that defines how Store Liveness messages are
// sent. Transport is the production implementation of MessageSender.
type MessageSender interface {
	SendAsync(msg slpb.Message) (sent bool)
}

// SupportManager orchestrates requesting and providing Store Liveness support.
type SupportManager struct {
	storeID               slpb.StoreIdent
	engine                storage.Engine
	options               Options
	settings              *clustersettings.Settings
	stopper               *stop.Stopper
	clock                 *hlc.Clock
	sender                MessageSender
	receiveQueue          receiveQueue
	storesToAdd           storesToAdd
	minWithdrawalTS       hlc.Timestamp
	supporterStateHandler *supporterStateHandler
	requesterStateHandler *requesterStateHandler
}

// NewSupportManager creates a new Store Liveness SupportManager. The main
// goroutine that processes Store Liveness messages is initialized
// separately, via Start.
func NewSupportManager(
	storeID slpb.StoreIdent,
	engine storage.Engine,
	options Options,
	settings *clustersettings.Settings,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	sender MessageSender,
) *SupportManager {
	return &SupportManager{
		storeID:               storeID,
		engine:                engine,
		options:               options,
		settings:              settings,
		stopper:               stopper,
		clock:                 clock,
		sender:                sender,
		receiveQueue:          newReceiveQueue(),
		storesToAdd:           newStoresToAdd(),
		requesterStateHandler: newRequesterStateHandler(),
		supporterStateHandler: newSupporterStateHandler(),
	}
}

// HandleMessage implements the MessageHandler interface. It appends incoming
// messages to a queue and does not block on processing the messages.
func (sm *SupportManager) HandleMessage(msg *slpb.Message) {
	sm.receiveQueue.Append(msg)
}

var _ MessageHandler = (*SupportManager)(nil)

// SupportFor implements the Fabric interface. It delegates the response to the
// SupportManager's supporterStateHandler.
func (sm *SupportManager) SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool) {
	ss := sm.supporterStateHandler.getSupportFor(id)
	// An empty expiration implies support has expired.
	if ss.Expiration.IsEmpty() {
		return 0, false
	}
	return ss.Epoch, true
}

// SupportFrom implements the Fabric interface. It delegates the response to the
// SupportManager's supporterStateHandler.
func (sm *SupportManager) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp, bool) {
	ss, ok := sm.requesterStateHandler.getSupportFrom(id)
	if !ok {
		// If this is the first time SupportFrom has been called for this store,
		// the store will be added to requesterStateHandler before the next
		// round of heartbeats. Multiple SupportFrom calls can race and call
		// storesToAdd.addStore concurrently but that's ok because storesToAdd
		// uses a map to avoid duplicates, and the requesterStateHandler's
		// addStore checks if the store exists before adding it.
		sm.storesToAdd.addStore(id)
		return 0, hlc.Timestamp{}, false
	}
	// An empty expiration implies support has expired.
	if ss.Expiration.IsEmpty() {
		return 0, hlc.Timestamp{}, false
	}
	return ss.Epoch, ss.Expiration, true
}

// SupportFromEnabled implements the Fabric interface and determines if Store
// Liveness sends heartbeats. It returns true if both the cluster setting and
// version gate are on.
func (sm *SupportManager) SupportFromEnabled(ctx context.Context) bool {
	clusterSettingEnabled := Enabled.Get(&sm.settings.SV)
	versionGateEnabled := sm.settings.Version.IsActive(
		ctx, clusterversion.V24_3_StoreLivenessEnabled,
	)
	return clusterSettingEnabled && versionGateEnabled
}

// Start starts the main processing goroutine in startLoop as an async task.
func (sm *SupportManager) Start(ctx context.Context) error {
	// onRestart is called synchronously before the start of the main loop in
	// order to ensure the SupportManager has loaded all persisted state into
	// memory and adjusted its clock accordingly before answering SupportFrom
	// and SupportFor requests.
	if err := sm.onRestart(ctx); err != nil {
		return err
	}

	return sm.stopper.RunAsyncTask(
		ctx, "storeliveness.SupportManager: loop", sm.startLoop,
	)
}

// onRestart initializes the SupportManager with state persisted on disk.
func (sm *SupportManager) onRestart(ctx context.Context) error {
	// Load the supporter and requester state from disk.
	if err := sm.supporterStateHandler.read(ctx, sm.engine); err != nil {
		return err
	}
	if err := sm.requesterStateHandler.read(ctx, sm.engine); err != nil {
		return err
	}
	// Advance the clock to the maximum withdrawal time.
	if err := sm.clock.UpdateAndCheckMaxOffset(
		ctx, sm.supporterStateHandler.supporterState.meta.MaxWithdrawn,
	); err != nil {
		return err
	}
	// Wait out the previous maximum requested time.
	if err := sm.clock.SleepUntil(
		ctx, sm.requesterStateHandler.requesterState.meta.MaxRequested,
	); err != nil {
		return err
	}
	// Set the minimum withdrawal time to give other stores a grace period
	// before losing support.
	sm.minWithdrawalTS = sm.clock.Now().AddDuration(sm.options.SupportWithdrawalGracePeriod)
	// Increment the current epoch.
	rsfu := sm.requesterStateHandler.checkOutUpdate()
	rsfu.incrementMaxEpoch()
	if err := rsfu.write(ctx, sm.engine); err != nil {
		return err
	}
	sm.requesterStateHandler.checkInUpdate(rsfu)
	return nil
}

// startLoop contains the main processing goroutine which orchestrates sending
// heartbeats, responding to messages, withdrawing support, adding and removing
// stores. Doing so in a single goroutine serializes these actions and
// simplifies the concurrency model.
func (sm *SupportManager) startLoop(ctx context.Context) {
	heartbeatTicker := time.NewTicker(sm.options.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	supportExpiryTicker := time.NewTicker(sm.options.SupportExpiryInterval)
	defer supportExpiryTicker.Stop()

	idleSupportFromTicker := time.NewTicker(sm.options.IdleSupportFromInterval)
	defer idleSupportFromTicker.Stop()

	for {
		// NOTE: only listen to the receive queue's signal if we don't already have
		// heartbeats to send or support to check. This prevents a constant flow of
		// inbound messages from delaying the other work due to the random selection
		// between multiple enabled channels.
		var receiveQueueSig <-chan struct{}
		if len(heartbeatTicker.C) == 0 && len(supportExpiryTicker.C) == 0 {
			receiveQueueSig = sm.receiveQueue.Sig()
		}

		select {
		case <-heartbeatTicker.C:
			// First check if any stores need to be added to ensure they are included
			// in the round of heartbeats below.
			sm.maybeAddStores()
			if sm.SupportFromEnabled(ctx) {
				sm.sendHeartbeats(ctx)
			}

		case <-supportExpiryTicker.C:
			sm.withdrawSupport(ctx)

		case <-idleSupportFromTicker.C:
			sm.requesterStateHandler.markIdleStores()

		case <-receiveQueueSig:
			msgs := sm.receiveQueue.Drain()
			sm.handleMessages(ctx, msgs)

		case <-sm.stopper.ShouldQuiesce():
			return
		}
	}
}

// maybeAddStores drains storesToAdd and delegates adding any new stores to the
// SupportManager's requesterStateHandler.
func (sm *SupportManager) maybeAddStores() {
	sta := sm.storesToAdd.drainStoresToAdd()
	for _, store := range sta {
		sm.requesterStateHandler.addStore(store)
	}
}

// sendHeartbeats delegates heartbeat generation to the requesterStateHandler
// and sends the resulting messages via Transport.
func (sm *SupportManager) sendHeartbeats(ctx context.Context) {
	rsfu := sm.requesterStateHandler.checkOutUpdate()
	livenessInterval := sm.options.LivenessInterval
	heartbeats := rsfu.getHeartbeatsToSend(sm.storeID, sm.clock.Now(), livenessInterval)
	if err := rsfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write requester meta: %v", err)
		return
	}
	sm.requesterStateHandler.checkInUpdate(rsfu)

	// Send heartbeats to each remote store.
	for _, msg := range heartbeats {
		if sent := sm.sender.SendAsync(msg); !sent {
			log.Warningf(ctx, "sending heartbeat to store %+v failed", msg.To)
		}
	}
	log.VInfof(
		ctx, 2, "store %d sent heartbeats to %d stores", sm.storeID, len(heartbeats),
	)
}

// withdrawSupport delegates support withdrawal to supporterStateHandler.
func (sm *SupportManager) withdrawSupport(ctx context.Context) {
	now := sm.clock.NowAsClockTimestamp()
	// Do not withdraw support if the grace period hasn't elapsed yet.
	if now.ToTimestamp().Less(sm.minWithdrawalTS) {
		return
	}
	ssfu := sm.supporterStateHandler.checkOutUpdate()
	ssfu.withdrawSupport(now)
	if err := ssfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write supporter meta: %v", err)
	}
	log.VInfof(
		ctx, 2, "store %d withdrew support from %d stores",
		sm.storeID, len(ssfu.inProgress.supportFor),
	)
	sm.supporterStateHandler.checkInUpdate(ssfu)
}

// handleMessages iterates over the given messages and delegates their handling
// to either the requesterStateHandler or supporterStateHandler. It then writes
// all updates to disk in a single batch, and sends any responses via Transport.
func (sm *SupportManager) handleMessages(ctx context.Context, msgs []*slpb.Message) {
	log.VInfof(ctx, 2, "store %d drained receive queue of size %d", sm.storeID, len(msgs))
	rsfu := sm.requesterStateHandler.checkOutUpdate()
	ssfu := sm.supporterStateHandler.checkOutUpdate()
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

	// TODO(mira): handle the errors below (and elsewhere in SupportManager)
	// properly by resetting the update in progress.
	batch := sm.engine.NewBatch()
	defer batch.Close()
	if err := rsfu.write(ctx, batch); err != nil {
		log.Warningf(ctx, "failed to write requester meta: %v", err)
	}
	if err := ssfu.write(ctx, batch); err != nil {
		log.Warningf(ctx, "failed to write supporter meta: %v", err)
	}
	if err := batch.Commit(true /* sync */); err != nil {
		log.Warningf(ctx, "failed to sync supporter and requester state: %v", err)
		return
	}
	sm.requesterStateHandler.checkInUpdate(rsfu)
	sm.supporterStateHandler.checkInUpdate(ssfu)

	for _, response := range responses {
		_ = sm.sender.SendAsync(response)
	}
	log.VInfof(ctx, 2, "store %d sent %d responses", sm.storeID, len(responses))
}

// receiveQueue stores all received messages from the MessageHandler and allows
// them to be processed async and in batch.
type receiveQueue struct {
	mu struct {
		syncutil.Mutex
		msgs []*slpb.Message
	}
	sig chan struct{}
}

func newReceiveQueue() receiveQueue {
	return receiveQueue{
		sig: make(chan struct{}, 1),
	}
}

func (q *receiveQueue) Append(msg *slpb.Message) {
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

func (q *receiveQueue) Drain() []*slpb.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	msgs := q.mu.msgs
	q.mu.msgs = nil
	return msgs
}

// storesToAdd contains a set of stores that Store Liveness periodically adds to
// requesterState.supportFrom.
type storesToAdd struct {
	mu     syncutil.Mutex
	stores map[slpb.StoreIdent]struct{}
}

func newStoresToAdd() storesToAdd {
	return storesToAdd{
		stores: make(map[slpb.StoreIdent]struct{}),
	}
}

func (sta *storesToAdd) addStore(id slpb.StoreIdent) {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	sta.stores[id] = struct{}{}
}

func (sta *storesToAdd) drainStoresToAdd() []slpb.StoreIdent {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	s := maps.Keys(sta.stores)
	clear(sta.stores)
	return s
}
