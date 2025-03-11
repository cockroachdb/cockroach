// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

var Enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.store_liveness.enabled",
	"if enabled, store liveness will heartbeat periodically; if disabled, "+
		"store liveness will still respond to heartbeats and calls to SupportFor",
	true,
)

// MessageSender is the interface that defines how Store Liveness messages are
// sent. Transport is the production implementation of MessageSender.
type MessageSender interface {
	SendAsync(ctx context.Context, msg slpb.Message) (sent bool)
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
	withdrawalCallback    func(map[roachpb.StoreID]struct{})
	supporterStateHandler *supporterStateHandler
	requesterStateHandler *requesterStateHandler
	metrics               *SupportManagerMetrics
	knobs                 *SupportManagerKnobs
}

var _ Fabric = (*SupportManager)(nil)

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
	knobs *SupportManagerKnobs,
) *SupportManager {
	if knobs != nil && knobs.TestEngine != nil && knobs.TestEngine.storeID == storeID {
		engine = knobs.TestEngine
	}
	return &SupportManager{
		storeID:               storeID,
		engine:                engine,
		options:               options,
		settings:              settings,
		stopper:               stopper,
		clock:                 clock,
		sender:                sender,
		knobs:                 knobs,
		receiveQueue:          newReceiveQueue(),
		storesToAdd:           newStoresToAdd(),
		requesterStateHandler: newRequesterStateHandler(),
		supporterStateHandler: newSupporterStateHandler(),
		metrics:               newSupportManagerMetrics(),
	}
}

// Metrics returns metrics tracking this SupportManager.
func (sm *SupportManager) Metrics() *SupportManagerMetrics {
	return sm.metrics
}

// HandleMessage implements the MessageHandler interface. It appends incoming
// messages to a queue and does not block on processing the messages.
func (sm *SupportManager) HandleMessage(msg *slpb.Message) error {
	if err := sm.receiveQueue.Append(msg); err != nil {
		return err
	}
	sm.metrics.ReceiveQueueSize.Inc(1)
	sm.metrics.ReceiveQueueBytes.Inc(int64(msg.Size()))
	return nil
}

var _ MessageHandler = (*SupportManager)(nil)

// SupportFor implements the Fabric interface. It delegates the response to the
// SupportManager's supporterStateHandler.
func (sm *SupportManager) SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool) {
	ss := sm.supporterStateHandler.getSupportFor(id)
	// An empty expiration implies support has expired.
	return ss.Epoch, !ss.Expiration.IsEmpty()
}

// InspectSupportFrom implements the InspectFabric interface.
func (sm *SupportManager) InspectSupportFrom() slpb.SupportStatesPerStore {
	supportStates := sm.requesterStateHandler.exportAllSupportFrom()
	return slpb.SupportStatesPerStore{StoreID: sm.storeID, SupportStates: supportStates}
}

// InspectSupportFor implements the InspectFabric interface.
func (sm *SupportManager) InspectSupportFor() slpb.SupportStatesPerStore {
	supportStates := sm.supporterStateHandler.exportAllSupportFor()
	return slpb.SupportStatesPerStore{StoreID: sm.storeID, SupportStates: supportStates}
}

// SupportFrom implements the Fabric interface. It delegates the response to the
// SupportManager's supporterStateHandler.
func (sm *SupportManager) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp) {
	ss, ok, wasIdle := sm.requesterStateHandler.getSupportFrom(id)
	if !ok {
		// If this is the first time SupportFrom has been called for this store,
		// the store will be added to requesterStateHandler before the next
		// round of heartbeats. Multiple SupportFrom calls can race and call
		// storesToAdd.addStore concurrently but that's ok because storesToAdd
		// uses a map to avoid duplicates, and the requesterStateHandler's
		// addStore checks if the store exists before adding it.
		sm.storesToAdd.addStore(id)
		log.VInfof(context.TODO(), 2, "store %+v is not heartbeating store %+v yet", sm.storeID, id)
		return 0, hlc.Timestamp{}
	}
	if wasIdle {
		log.Infof(
			context.TODO(), "store %+v is starting to heartbeat store %+v (after being idle)",
			sm.storeID, id,
		)
	}
	return ss.Epoch, ss.Expiration
}

// RegisterSupportWithdrawalCallback implements the Fabric interface and
// registers a callback to be invoked on each support withdrawal.
func (sm *SupportManager) RegisterSupportWithdrawalCallback(cb func(map[roachpb.StoreID]struct{})) {
	sm.withdrawalCallback = cb
}

// SupportFromEnabled implements the Fabric interface and determines if Store
// Liveness sends heartbeats. It returns true if the cluster setting is on.
func (sm *SupportManager) SupportFromEnabled(ctx context.Context) bool {
	return Enabled.Get(&sm.settings.SV)
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
	// Update the support-for metric.
	sm.metrics.SupportForStores.Update(int64(sm.supporterStateHandler.getNumSupportFor()))
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
	defer sm.requesterStateHandler.finishUpdate(rsfu)
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
		// stores to add, heartbeats to send, or support to check. This prevents a
		// constant flow of inbound messages from delaying the other work due to the
		// random selection between multiple enabled channels.
		var receiveQueueSig <-chan struct{}
		if len(heartbeatTicker.C) == 0 &&
			len(supportExpiryTicker.C) == 0 &&
			len(sm.storesToAdd.sig) == 0 {
			receiveQueueSig = sm.receiveQueue.Sig()
		}

		select {
		case <-sm.storesToAdd.sig:
			sm.maybeAddStores(ctx)
			sm.sendHeartbeats(ctx)

		case <-heartbeatTicker.C:
			sm.sendHeartbeats(ctx)

		case <-supportExpiryTicker.C:
			sm.withdrawSupport(ctx)

		case <-idleSupportFromTicker.C:
			sm.requesterStateHandler.markIdleStores(ctx)

		case <-receiveQueueSig:
			// Decrementing the queue metrics is done in handleMessages.
			msgs := sm.receiveQueue.Drain()
			sm.handleMessages(ctx, msgs)

		case <-sm.stopper.ShouldQuiesce():
			return
		}
	}
}

// maybeAddStores drains storesToAdd and delegates adding any new stores to the
// SupportManager's requesterStateHandler.
func (sm *SupportManager) maybeAddStores(ctx context.Context) {
	sta := sm.storesToAdd.drainStoresToAdd()
	for _, store := range sta {
		if sm.requesterStateHandler.addStore(store) {
			log.Infof(ctx, "starting to heartbeat store %+v", store)
			sm.metrics.SupportFromStores.Inc(1)
		}
	}
}

// sendHeartbeats delegates heartbeat generation to the requesterStateHandler
// and sends the resulting messages via Transport.
func (sm *SupportManager) sendHeartbeats(ctx context.Context) {
	// If Store Liveness is not enabled, don't send heartbeats.
	if !sm.SupportFromEnabled(ctx) {
		return
	}
	if sm.knobs != nil && sm.knobs.DisableHeartbeats != nil && sm.knobs.DisableHeartbeats.Load() == sm.storeID {
		return
	}
	if sm.knobs != nil && sm.knobs.DisableAllHeartbeats != nil && sm.knobs.DisableAllHeartbeats.Load() {
		return
	}
	rsfu := sm.requesterStateHandler.checkOutUpdate()
	defer sm.requesterStateHandler.finishUpdate(rsfu)
	livenessInterval := sm.options.SupportDuration
	heartbeats := rsfu.getHeartbeatsToSend(sm.storeID, sm.clock.Now(), livenessInterval)
	if err := rsfu.write(ctx, sm.engine); err != nil {
		log.Warningf(ctx, "failed to write requester meta: %v", err)
		sm.metrics.HeartbeatFailures.Inc(int64(len(heartbeats)))
		return
	}
	sm.requesterStateHandler.checkInUpdate(rsfu)

	// Send heartbeats to each remote store.
	successes := 0
	for _, msg := range heartbeats {
		if sent := sm.sender.SendAsync(ctx, msg); sent {
			successes++
		} else {
			log.Warningf(ctx, "failed to send heartbeat to store %+v", msg.To)
		}
	}
	sm.metrics.HeartbeatSuccesses.Inc(int64(successes))
	sm.metrics.HeartbeatFailures.Inc(int64(len(heartbeats) - successes))
	log.VInfof(ctx, 2, "sent heartbeats to %d stores", successes)
}

// withdrawSupport delegates support withdrawal to supporterStateHandler.
func (sm *SupportManager) withdrawSupport(ctx context.Context) {
	now := sm.clock.NowAsClockTimestamp()
	// Do not withdraw support if the grace period hasn't elapsed yet.
	if now.ToTimestamp().Less(sm.minWithdrawalTS) {
		return
	}
	ssfu := sm.supporterStateHandler.checkOutUpdate()
	defer sm.supporterStateHandler.finishUpdate(ssfu)
	supportWithdrawnForStoreIDs := ssfu.withdrawSupport(ctx, now)
	numWithdrawn := len(supportWithdrawnForStoreIDs)
	if numWithdrawn == 0 {
		// No support to withdraw.
		return
	}

	batch := sm.engine.NewBatch()
	defer batch.Close()
	if err := ssfu.write(ctx, batch); err != nil {
		log.Warningf(ctx, "failed to write supporter meta and state: %v", err)
		sm.metrics.SupportWithdrawFailures.Inc(int64(numWithdrawn))
		return
	}
	if err := batch.Commit(true /* sync */); err != nil {
		log.Warningf(ctx, "failed to commit supporter meta and state: %v", err)
		sm.metrics.SupportWithdrawFailures.Inc(int64(numWithdrawn))
		return
	}
	sm.supporterStateHandler.checkInUpdate(ssfu)
	log.Infof(ctx, "withdrew support from %d stores", numWithdrawn)
	sm.metrics.SupportWithdrawSuccesses.Inc(int64(numWithdrawn))
	if sm.withdrawalCallback != nil {
		beforeProcess := timeutil.Now()
		sm.withdrawalCallback(supportWithdrawnForStoreIDs)
		afterProcess := timeutil.Now()
		processDur := afterProcess.Sub(beforeProcess)
		if processDur > minCallbackDurationToRecord {
			sm.metrics.CallbacksProcessingDuration.RecordValue(processDur.Nanoseconds())
		}
		log.Infof(ctx, "invoked callback for %d stores", numWithdrawn)
	}
}

// handleMessages iterates over the given messages and delegates their handling
// to either the requesterStateHandler or supporterStateHandler. It then writes
// all updates to disk in a single batch, and sends any responses via Transport.
func (sm *SupportManager) handleMessages(ctx context.Context, msgs []*slpb.Message) {
	log.VInfof(ctx, 2, "drained receive queue of size %d", len(msgs))
	rsfu := sm.requesterStateHandler.checkOutUpdate()
	defer sm.requesterStateHandler.finishUpdate(rsfu)
	ssfu := sm.supporterStateHandler.checkOutUpdate()
	defer sm.supporterStateHandler.finishUpdate(ssfu)
	var responses []slpb.Message
	for _, msg := range msgs {
		sm.metrics.ReceiveQueueSize.Dec(1)
		sm.metrics.ReceiveQueueBytes.Dec(int64(msg.Size()))
		switch msg.Type {
		case slpb.MsgHeartbeat:
			responses = append(responses, ssfu.handleHeartbeat(ctx, msg))
		case slpb.MsgHeartbeatResp:
			rsfu.handleHeartbeatResponse(ctx, msg)
		default:
			log.Errorf(ctx, "unexpected message type: %v", msg.Type)
		}
	}

	batch := sm.engine.NewBatch()
	defer batch.Close()
	if err := rsfu.write(ctx, batch); err != nil {
		log.Warningf(ctx, "failed to write requester meta: %v", err)
		sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
		return
	}
	if err := ssfu.write(ctx, batch); err != nil {
		log.Warningf(ctx, "failed to write supporter meta: %v", err)
		sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
		return
	}
	if err := batch.Commit(true /* sync */); err != nil {
		log.Warningf(ctx, "failed to sync supporter and requester state: %v", err)
		sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
		return
	}
	sm.requesterStateHandler.checkInUpdate(rsfu)
	sm.supporterStateHandler.checkInUpdate(ssfu)
	sm.metrics.MessageHandleSuccesses.Inc(int64(len(msgs)))
	// Handling heartbeats is the only way to add a store to the supportFor map.
	sm.metrics.SupportForStores.Update(int64(sm.supporterStateHandler.getNumSupportFor()))

	for _, response := range responses {
		_ = sm.sender.SendAsync(ctx, response)
	}
	log.VInfof(ctx, 2, "sent %d heartbeat responses", len(responses))
}

// maxReceiveQueueSize is the maximum number of messages the receive queue can
// store. If message consumption is slow (e.g. due to a disk stall) and the
// queue reaches maxReceiveQueueSize, incoming messages will be dropped.
const maxReceiveQueueSize = 10000

var receiveQueueSizeLimitReachedErr = errors.Errorf("store liveness receive queue is full")

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

func (q *receiveQueue) Append(msg *slpb.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Drop messages if maxReceiveQueueSize is reached.
	if len(q.mu.msgs) >= maxReceiveQueueSize {
		return receiveQueueSizeLimitReachedErr
	}
	q.mu.msgs = append(q.mu.msgs, msg)
	select {
	case q.sig <- struct{}{}:
	default:
	}
	return nil
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
	sig    chan struct{}
	stores map[slpb.StoreIdent]struct{}
}

func newStoresToAdd() storesToAdd {
	return storesToAdd{
		sig:    make(chan struct{}, 1),
		stores: make(map[slpb.StoreIdent]struct{}),
	}
}

func (sta *storesToAdd) addStore(id slpb.StoreIdent) {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	sta.stores[id] = struct{}{}
	select {
	case sta.sig <- struct{}{}:
	default:
	}
}

func (sta *storesToAdd) drainStoresToAdd() []slpb.StoreIdent {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	s := maps.Keys(sta.stores)
	clear(sta.stores)
	return s
}
