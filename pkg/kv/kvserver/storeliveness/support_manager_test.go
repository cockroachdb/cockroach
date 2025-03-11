// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"testing"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	store       = slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}
	remoteStore = slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)}
	options     = Options{
		SupportDuration:         6 * time.Millisecond,
		HeartbeatInterval:       3 * time.Millisecond,
		SupportExpiryInterval:   1 * time.Millisecond,
		IdleSupportFromInterval: 1 * time.Minute,
	}
)

// TestSupportManagerRequestsSupport tests that the SupportManager requests and
// establishes support on behalf of the local store.
func TestSupportManagerRequestsSupport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	require.NoError(t, sm.Start(ctx))

	// Start sending heartbeats to the remote store by calling SupportFrom.
	epoch, expiration := sm.SupportFrom(remoteStore)
	require.Equal(t, slpb.Epoch(0), epoch)
	require.Equal(t, hlc.Timestamp{}, expiration)

	testutils.SucceedsSoon(
		t, func() error {
			if sm.metrics.SupportFromStores.Value() != int64(1) {
				return errors.New("store not added yet")
			}
			return nil
		},
	)

	// Ensure heartbeats are sent.
	msgs := ensureHeartbeats(t, sender, 10)
	require.LessOrEqual(t, int64(10), sm.metrics.HeartbeatSuccesses.Count())
	require.Equal(t, int64(0), sm.metrics.HeartbeatFailures.Count())
	require.Equal(t, slpb.MsgHeartbeat, msgs[0].Type)
	require.Equal(t, sm.storeID, msgs[0].From)
	require.Equal(t, remoteStore, msgs[0].To)
	requestedExpiration := msgs[0].Expiration

	// Process a heartbeat response from the remote store.
	heartbeatResp := &slpb.Message{
		Type:       slpb.MsgHeartbeatResp,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: requestedExpiration,
	}
	require.NoError(t, sm.HandleMessage(heartbeatResp))

	// Ensure support is provided as seen by SupportFrom.
	testutils.SucceedsSoon(
		t, func() error {
			epoch, expiration = sm.SupportFrom(remoteStore)
			if expiration.IsEmpty() {
				return errors.New("support not provided yet")
			}
			require.Equal(t, slpb.Epoch(1), epoch)
			require.Equal(t, requestedExpiration, expiration)
			return nil
		},
	)
	testutils.SucceedsSoon(
		t, func() error {
			if sm.metrics.MessageHandleSuccesses.Count() != int64(1) {
				return errors.New("metric not incremented yet")
			}
			require.Equal(t, int64(0), sm.metrics.MessageHandleFailures.Count())
			return nil
		},
	)
}

// TestSupportManagerProvidesSupport tests that the SupportManager provides
// support for a remote store.
func TestSupportManagerProvidesSupport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	cb := func(supportWithdrawn map[roachpb.StoreID]struct{}) {
		require.Equal(t, 1, len(supportWithdrawn))
		_, ok := supportWithdrawn[roachpb.StoreID(2)]
		require.True(t, ok)
	}
	sm.RegisterSupportWithdrawalCallback(cb)
	require.NoError(t, sm.Start(ctx))

	// Pause the clock so support is not withdrawn before calling SupportFor.
	manual.Pause()

	// Process a heartbeat from the remote store.
	heartbeat := &slpb.Message{
		Type:       slpb.MsgHeartbeat,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: sm.clock.Now().AddDuration(options.SupportDuration),
	}
	require.NoError(t, sm.HandleMessage(heartbeat))

	// Ensure a response is sent.
	testutils.SucceedsSoon(
		t, func() error {
			if sender.getNumSentMessages() == 0 {
				return errors.New("more messages expected")
			}
			require.Equal(t, int64(1), sm.metrics.MessageHandleSuccesses.Count())
			require.Equal(t, int64(0), sm.metrics.MessageHandleFailures.Count())
			require.Equal(t, int64(1), sm.metrics.SupportForStores.Value())
			return nil
		},
	)
	msg := sender.drainSentMessages()[0]
	require.Equal(t, slpb.MsgHeartbeatResp, msg.Type)
	require.Equal(t, heartbeat.To, msg.From)
	require.Equal(t, heartbeat.From, msg.To)
	require.Equal(t, heartbeat.Epoch, msg.Epoch)
	require.Equal(t, heartbeat.Expiration, msg.Expiration)

	// Ensure support is provided as seen by SupportFor.
	epoch, supported := sm.SupportFor(remoteStore)
	require.Equal(t, slpb.Epoch(1), epoch)
	require.True(t, supported)

	// Resume the clock, so support can be withdrawn.
	manual.Resume()

	// Wait for support to be withdrawn.
	testutils.SucceedsSoon(
		t, func() error {
			epoch, supported = sm.SupportFor(remoteStore)
			if supported {
				return errors.New("support not withdrawn yet")
			}
			require.Equal(t, slpb.Epoch(2), epoch)
			require.False(t, supported)
			return nil
		},
	)
	testutils.SucceedsSoon(
		t, func() error {
			if sm.metrics.SupportWithdrawSuccesses.Count() != int64(1) {
				return errors.New("metric not incremented yet")
			}
			require.Equal(t, int64(0), sm.metrics.SupportWithdrawFailures.Count())
			return nil
		},
	)
}

// TestSupportManagerEnableDisable tests that the SupportManager respects
// enabling and disabling.
func TestSupportManagerEnableDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	require.NoError(t, sm.Start(ctx))

	// Start sending heartbeats by calling SupportFrom.
	sm.SupportFrom(remoteStore)
	ensureHeartbeats(t, sender, 10)

	// Disable Store Liveness and make sure heartbeats stop.
	Enabled.Override(ctx, &settings.SV, false)
	// One heartbeat may race in while heartbeats are being disabled.
	ensureNoHeartbeats(t, sender, sm.options.HeartbeatInterval, 1)

	// Enable Store Liveness again and make sure heartbeats are sent.
	Enabled.Override(ctx, &settings.SV, true)
	ensureHeartbeats(t, sender, 10)
}

// TestSupportManagerRestart tests that the SupportManager adjusts the clock
// correctly after restarting.
func TestSupportManagerRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	manualBehind := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	clockBehind := hlc.NewClockForTesting(manualBehind)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	// Initialize the SupportManager without starting the main goroutine.
	require.NoError(t, sm.onRestart(ctx))

	// Establish support for and from the remote store, and withdraw support.
	manual.Pause()
	manualBehind.Pause()
	sm.SupportFrom(remoteStore)
	sm.maybeAddStores(ctx)
	sm.sendHeartbeats(ctx)
	requestedTime := sm.requesterStateHandler.requesterState.meta.MaxRequested
	heartbeatResp := &slpb.Message{
		Type:       slpb.MsgHeartbeatResp,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: requestedTime,
	}
	heartbeat := &slpb.Message{
		Type:       slpb.MsgHeartbeat,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: clock.Now().AddDuration(sm.options.SupportDuration),
	}
	sm.handleMessages(ctx, []*slpb.Message{heartbeatResp, heartbeat})
	manual.Resume()
	manual.Increment(sm.options.SupportDuration.Nanoseconds())
	sm.withdrawSupport(ctx)
	withdrawalTime := sm.supporterStateHandler.supporterState.meta.MaxWithdrawn.ToTimestamp()

	// Simulate a restart by creating a new SupportManager with the same engine.
	// Use a regressed clock.
	sm = NewSupportManager(store, engine, options, settings, stopper, clockBehind, sender, nil)
	now := sm.clock.Now()
	require.False(t, requestedTime.Less(now))
	require.False(t, withdrawalTime.Less(now))

	manualBehind.Resume()
	require.NoError(t, sm.onRestart(ctx))
	manualBehind.Pause()

	// Ensure the clock is set past MaxWithdrawn and MaxRequested.
	now = sm.clock.Now()
	require.True(t, requestedTime.Less(now))
	require.True(t, withdrawalTime.Less(now))
}

// TestSupportManagerDiskStall tests that the SupportManager continues to
// respond to SupportFrom and SupportFor calls when its disk is stalled.
func TestSupportManagerDiskStall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := NewTestEngine(store)
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	// Initialize the SupportManager without starting the main goroutine.
	require.NoError(t, sm.onRestart(ctx))

	// Establish support for and from the remote store.
	sm.SupportFrom(remoteStore)
	sm.maybeAddStores(ctx)
	sm.sendHeartbeats(ctx)
	requestedTime := sm.requesterStateHandler.requesterState.meta.MaxRequested
	heartbeatResp := &slpb.Message{
		Type:       slpb.MsgHeartbeatResp,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: requestedTime,
	}
	heartbeat := &slpb.Message{
		Type:       slpb.MsgHeartbeat,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: clock.Now().AddDuration(sm.options.SupportDuration),
	}
	sm.handleMessages(ctx, []*slpb.Message{heartbeatResp, heartbeat})

	// Start blocking writes.
	engine.SetBlockOnWrite(true)
	sender.drainSentMessages()

	// Send heartbeats in a separate goroutine. It will block on writing the
	// requester meta.
	require.NoError(
		t, sm.stopper.RunAsyncTask(
			ctx, "heartbeat", sm.sendHeartbeats,
		),
	)
	ensureNoHeartbeats(t, sender, sm.options.HeartbeatInterval, 0)

	// SupportFrom and SupportFor calls are still being answered.
	epoch, _ := sm.SupportFrom(remoteStore)
	require.Equal(t, slpb.Epoch(1), epoch)

	epoch, supported := sm.SupportFor(remoteStore)
	require.Equal(t, slpb.Epoch(1), epoch)
	require.True(t, supported)

	// Stop blocking writes.
	engine.SetBlockOnWrite(false)

	// Ensure the heartbeat is unblocked and sent out.
	ensureHeartbeats(t, sender, 1)
}

// TestSupportManagerReceiveQueueLimit tests that the receive queue returns
// errors when the queue size limit is reached.
func TestSupportManagerReceiveQueueLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	// Initialize the SupportManager without starting the main goroutine.
	require.NoError(t, sm.onRestart(ctx))

	heartbeat := &slpb.Message{
		Type:       slpb.MsgHeartbeat,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: clock.Now().AddDuration(sm.options.SupportDuration),
	}

	for i := 0; i < maxReceiveQueueSize; i++ {
		require.NoError(t, sm.HandleMessage(heartbeat))
	}
	require.Equal(t, int64(maxReceiveQueueSize), sm.metrics.ReceiveQueueSize.Value())
	require.Equal(
		t, int64(maxReceiveQueueSize*heartbeat.Size()), sm.metrics.ReceiveQueueBytes.Value(),
	)

	// Nothing is consuming messages from the queue, so the next HandleMessage
	// should result in an error.
	require.Regexp(t, sm.HandleMessage(heartbeat), "store liveness receive queue is full")
}

// TestSupportManagerHeartbeatNewStore tests that when a store is added (in the
// first call of SupportFrom), heartbeats are sent to it immediately, before a
// heartbeat interval has expired.
func TestSupportManagerHeartbeatNewStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	// Set a very large heartbeat interval to ensure heartbeats for new stores are
	// sent out before the heartbeat ticker is signalled.
	options.HeartbeatInterval = time.Hour
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender, nil)
	require.NoError(t, sm.Start(ctx))

	// Start sending heartbeats to the remote store by calling SupportFrom.
	sm.SupportFrom(remoteStore)

	testutils.SucceedsSoon(
		t, func() error {
			if sm.metrics.HeartbeatSuccesses.Count() == int64(0) {
				return errors.New("heartbeat not sent yet")
			}
			return nil
		},
	)
}

func ensureHeartbeats(t *testing.T, sender *testMessageSender, expectedNum int) []slpb.Message {
	var msgs []slpb.Message
	testutils.SucceedsSoon(
		t, func() error {
			if sender.getNumSentMessages() < expectedNum {
				return errors.New("not enough heartbeats")
			}
			msgs = sender.drainSentMessages()
			require.Equal(t, slpb.MsgHeartbeat, msgs[0].Type)
			return nil
		},
	)
	return msgs
}

func ensureNoHeartbeats(
	t *testing.T, sender *testMessageSender, hbInterval time.Duration, slack int,
) {
	sender.drainSentMessages()
	err := testutils.SucceedsWithinError(
		func() error {
			if sender.getNumSentMessages() > slack {
				return errors.New("heartbeats are sent")
			} else {
				return errors.New("no heartbeats")
			}
		}, hbInterval*10,
	)
	require.Regexp(t, err, "no heartbeats")
}
