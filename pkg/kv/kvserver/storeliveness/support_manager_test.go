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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	store       = slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}
	remoteStore = slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)}
	options     = Options{
		HeartbeatInterval:       3 * time.Millisecond,
		LivenessInterval:        6 * time.Millisecond,
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
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender)
	require.NoError(t, sm.Start(ctx))

	// Start sending heartbeats to the remote store by calling SupportFrom.
	epoch, expiration, supported := sm.SupportFrom(remoteStore)
	require.Equal(t, slpb.Epoch(0), epoch)
	require.Equal(t, hlc.Timestamp{}, expiration)
	require.False(t, supported)

	// Ensure heartbeats are sent.
	msgs := ensureHeartbeats(t, sender, 10)
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
	sm.HandleMessage(heartbeatResp)

	// Ensure support is provided as seen by SupportFrom.
	testutils.SucceedsSoon(
		t, func() error {
			epoch, expiration, supported = sm.SupportFrom(remoteStore)
			if !supported {
				return errors.New("support not provided yet")
			}
			require.Equal(t, slpb.Epoch(1), epoch)
			require.Equal(t, requestedExpiration, expiration)
			require.True(t, supported)
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
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender)
	require.NoError(t, sm.Start(ctx))

	// Process a heartbeat from the remote store.
	heartbeat := &slpb.Message{
		Type:       slpb.MsgHeartbeat,
		From:       remoteStore,
		To:         sm.storeID,
		Epoch:      slpb.Epoch(1),
		Expiration: sm.clock.Now().AddDuration(time.Second),
	}
	sm.HandleMessage(heartbeat)

	// Ensure a response is sent.
	testutils.SucceedsSoon(
		t, func() error {
			if sender.getNumSentMessages() == 0 {
				return errors.New("more messages expected")
			}
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

	// Wait for support to be withdrawn.
	testutils.SucceedsSoon(
		t, func() error {
			epoch, supported = sm.SupportFor(remoteStore)
			if supported {
				return errors.New("support not withdrawn yet")
			}
			require.Equal(t, slpb.Epoch(0), epoch)
			require.False(t, supported)
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
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender)
	require.NoError(t, sm.Start(ctx))

	// Start sending heartbeats by calling SupportFrom.
	_, _, supported := sm.SupportFrom(remoteStore)
	require.False(t, supported)
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
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender)
	// Initialize the SupportManager without starting the main goroutine.
	require.NoError(t, sm.onRestart(ctx))

	// Establish support for and from the remote store, and withdraw support.
	manual.Pause()
	manualBehind.Pause()
	sm.SupportFrom(remoteStore)
	sm.maybeAddStores()
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
		Expiration: clock.Now().AddDuration(sm.options.LivenessInterval),
	}
	sm.handleMessages(ctx, []*slpb.Message{heartbeatResp, heartbeat})
	manual.Resume()
	manual.Increment(sm.options.LivenessInterval.Nanoseconds())
	sm.withdrawSupport(ctx)
	withdrawalTime := sm.supporterStateHandler.supporterState.meta.MaxWithdrawn.ToTimestamp()

	// Simulate a restart by creating a new SupportManager with the same engine.
	// Use a regressed clock.
	sm = NewSupportManager(store, engine, options, settings, stopper, clockBehind, sender)
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

func TestSupportManagerDiskStall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := &testEngine{
		Engine:     storage.NewDefaultInMemForTesting(),
		blockingCh: make(chan struct{}, 1),
	}
	defer engine.Close()
	settings := clustersettings.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	sender := &testMessageSender{}
	sm := NewSupportManager(store, engine, options, settings, stopper, clock, sender)
	// Initialize the SupportManager without starting the main goroutine.
	require.NoError(t, sm.onRestart(ctx))

	// Establish support for and from the remote store.
	sm.SupportFrom(remoteStore)
	sm.maybeAddStores()
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
		Expiration: clock.Now().AddDuration(sm.options.LivenessInterval),
	}
	sm.handleMessages(ctx, []*slpb.Message{heartbeatResp, heartbeat})

	// Start blocking writes.
	engine.setBlockOnWrite(true)
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
	epoch, _, supported := sm.SupportFrom(remoteStore)
	require.Equal(t, slpb.Epoch(1), epoch)
	require.True(t, supported)

	epoch, supported = sm.SupportFor(remoteStore)
	require.Equal(t, slpb.Epoch(1), epoch)
	require.True(t, supported)

	// Stop blocking writes.
	engine.blockingCh <- struct{}{}
	engine.setBlockOnWrite(false)

	// Ensure the heartbeat is unblocked and sent out.
	ensureHeartbeats(t, sender, 1)
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

// testMessageSender implements the MessageSender interface and stores all sent
// messages in a slice.
type testMessageSender struct {
	mu       syncutil.Mutex
	messages []slpb.Message
}

func (tms *testMessageSender) SendAsync(msg slpb.Message) (sent bool) {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	tms.messages = append(tms.messages, msg)
	return true
}

func (tms *testMessageSender) drainSentMessages() []slpb.Message {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	msgs := tms.messages
	tms.messages = nil
	return msgs
}

func (tms *testMessageSender) getNumSentMessages() int {
	tms.mu.Lock()
	defer tms.mu.Unlock()
	return len(tms.messages)
}

var _ MessageSender = (*testMessageSender)(nil)

// testEngine is a wrapper around storage.Engine that helps simulate failed and
// stalled writes.
type testEngine struct {
	storage.Engine
	mu           syncutil.Mutex
	blockingCh   chan struct{}
	blockOnWrite bool
	errorOnWrite bool
}

func (te *testEngine) NewBatch() storage.Batch {
	return testBatch{
		Batch:        te.Engine.NewBatch(),
		blockingCh:   te.blockingCh,
		blockOnWrite: te.blockOnWrite,
		errorOnWrite: te.errorOnWrite,
	}
}

func (te *testEngine) setBlockOnWrite(bow bool) {
	te.mu.Lock()
	defer te.mu.Unlock()
	te.blockOnWrite = bow
}

func (te *testEngine) PutUnversioned(key roachpb.Key, value []byte) error {
	te.mu.Lock()
	defer te.mu.Unlock()
	if te.blockOnWrite {
		<-te.blockingCh
	}
	if te.errorOnWrite {
		return errors.New("error writing")
	}
	return te.Engine.PutUnversioned(key, value)
}

type testBatch struct {
	storage.Batch
	blockingCh   chan struct{}
	blockOnWrite bool
	errorOnWrite bool
}

func (tb testBatch) Commit(sync bool) error {
	if tb.blockOnWrite {
		<-tb.blockingCh
	}
	if tb.errorOnWrite {
		return errors.New("error committing batch")
	}
	return tb.Batch.Commit(sync)
}
