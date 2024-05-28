package kvflowconnectedstream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
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

func (t *testingTokenAvailableNotification) Notify() {
	if t.onNotify != nil {
		t.onNotify()
	}
	t.counter.Add(1)
}

var _ TokenAvailableNotification = &testingTokenAvailableNotification{}

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
	clock := hlc.NewClockForTesting(hlc.NewHybridManualClock())
	counter := newTokenCounter(tokensPerWorkClass, clock)

	var handleA, handleB StoreStreamSendTokenHandleID
	tokenIncrement := kvflowcontrol.Tokens(1)
	wantTokensA := tokensPerWorkClass.regular / 2
	wantTokensB := wantTokensA

	makeNotify := func(
		t *testing.T,
		wantTokens *kvflowcontrol.Tokens,
		handle *StoreStreamSendTokenHandleID,
	) *testingTokenAvailableNotification {
		return &testingTokenAvailableNotification{t: t, onNotify: func() {
			// Try deducting all the tokens we are waiting on when notified, we expect
			// there to be at most tokenIncrement tokens granted, since that is the
			// number of tokens returned.
			granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, *wantTokens)
			require.Equal(t, tokenIncrement, granted)
			*wantTokens -= granted

			// Cancel the handle when no more tokens are wanted.
			if *wantTokens == 0 {
				watcher.CancelHandle(*handle)
			}
		}}
	}

	notifyA := makeNotify(t, &wantTokensA, &handleA)
	notifyB := makeNotify(t, &wantTokensB, &handleB)

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

	// TODO(kvoli): Test UpdateHandle.
}
