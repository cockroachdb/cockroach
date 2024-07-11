package kvflowconnectedstream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// testingTokenAvailableNotification implements the TokenAvailableNotification
// interface.
type testingTokenAvailableNotification struct {
	t        *testing.T
	counter  atomic.Int32
	onNotify func()
}

func (t *testingTokenAvailableNotification) Notify(_ context.Context) {
	if t.onNotify != nil {
		t.onNotify()
	}
	t.counter.Add(1)
}

var _ TokenAvailableNotification = &testingTokenAvailableNotification{}

// TestStoreStreamSendTokenWatcher tests the basic functionality of
// StoreStreamSendTokensWatcher implemented by storeStreamSendTokensWatcher
// using two concurrent notifiers.
func TestStoreStreamSendTokenWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	watcher := NewStoreStreamSendTokensWatcher(stopper)
	tokensPerWorkClass := tokensPerWorkClass{
		regular: 100,
		elastic: 100,
	}

	settings := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClockForTesting(hlc.NewHybridManualClock())

	ElasticTokensPerStream.Override(ctx, &settings.SV, int64(tokensPerWorkClass.elastic))
	RegularTokensPerStream.Override(ctx, &settings.SV, int64(tokensPerWorkClass.regular))

	counter := newTokenCounter(settings, clock)

	var handleA, handleB StoreStreamSendTokenHandleID
	tokenIncrement := kvflowcontrol.Tokens(1)
	wantTokensA := tokensPerWorkClass.regular / 2
	wantTokensB := wantTokensA

	makeNotify := func(
		t *testing.T,
		wantTokens *kvflowcontrol.Tokens,
		handle *StoreStreamSendTokenHandleID,
		wc admissionpb.WorkClass,
	) *testingTokenAvailableNotification {
		return &testingTokenAvailableNotification{t: t, onNotify: func() {
			// Try deducting all the tokens we are waiting on when notified, we expect
			// there to be at most tokenIncrement tokens granted, since that is the
			// number of tokens returned.
			granted := counter.TryDeduct(ctx, wc, *wantTokens)
			require.Equal(t, tokenIncrement, granted)
			*wantTokens -= granted

			// Cancel the handle when no more tokens are wanted.
			if *wantTokens == 0 {
				watcher.CancelHandle(*handle)
			}
		}}
	}

	notifyA := makeNotify(t, &wantTokensA, &handleA, admissionpb.RegularWorkClass)
	notifyB := makeNotify(t, &wantTokensB, &handleB, admissionpb.RegularWorkClass)

	// Initially, deduct all the tokens so that there are none available.
	counter.Deduct(ctx, admissionpb.RegularWorkClass, tokensPerWorkClass.regular)
	handleA = watcher.NotifyWhenAvailable(counter, admissionpb.RegularWorkClass, notifyA)
	handleB = watcher.NotifyWhenAvailable(counter, admissionpb.RegularWorkClass, notifyB)

	// Return some tokens, so the number of tokens available is greater than 0.
	for i := 1; i <= int(tokensPerWorkClass.regular); i++ {
		counter.Return(ctx, admissionpb.RegularWorkClass, tokenIncrement)
		// The token counter doesn't synchronously signal the watcher to notify,
		// allow some delay.
		time.Sleep(1 * time.Millisecond)
		notifyCountA := notifyA.counter.Load()
		notifyCountB := notifyB.counter.Load()
		require.Equal(t, int32(i), notifyCountA+notifyCountB)
	}
	notifyCountA := notifyA.counter.Load()
	notifyCountB := notifyB.counter.Load()
	require.Equal(t, notifyCountA, notifyCountB)
	require.Equal(t, kvflowcontrol.Tokens(0), counter.tokens(admissionpb.RegularWorkClass))
	require.Equal(t, kvflowcontrol.Tokens(0), counter.tokens(admissionpb.ElasticWorkClass))

	// The handle is cancelled, the watcher should panic trying to find the
	// handle.
	require.Panics(t, func() { watcher.UpdateHandle(handleA, admissionpb.ElasticWorkClass) })

	// Create a new handle, then update it to a different work class. The watcher
	// should notify the handle when the work class is updated and not need to
	// wait on the original token counter to have available tokens before
	// swapping the work class.
	var handleC StoreStreamSendTokenHandleID
	wantTokensC := tokenIncrement
	notifyC := makeNotify(t, &wantTokensC, &handleC, admissionpb.ElasticWorkClass)
	handleC = watcher.NotifyWhenAvailable(counter, admissionpb.RegularWorkClass, notifyC)
	watcher.UpdateHandle(handleC, admissionpb.ElasticWorkClass)
	counter.Return(ctx, admissionpb.ElasticWorkClass, tokenIncrement)
	require.Equal(t, kvflowcontrol.Tokens(1), counter.tokens(admissionpb.ElasticWorkClass))

	time.Sleep(1 * time.Millisecond)
	require.Equal(t, int32(1), notifyC.counter.Load())
}
