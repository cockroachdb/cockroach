package kvflowconnectedstream

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTokenCounter provides basic testing for the TokenCounter interface
// methods implemented by tokenCounter.
func TestTokenCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tokensPerWorkClass := tokensPerWorkClass{
		regular: 100,
		elastic: 100,
	}
	settings := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClockForTesting(hlc.NewHybridManualClock())

	elasticTokensPerStream.Override(ctx, &settings.SV, int64(tokensPerWorkClass.elastic))
	regularTokensPerStream.Override(ctx, &settings.SV, int64(tokensPerWorkClass.regular))

	counter := newTokenCounter(settings, clock)

	// Initially, expect there to be tokens available, so no handle is returned.
	available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
	require.True(t, available)
	require.Nil(t, handle)

	available, handle = counter.TokensAvailable(admissionpb.ElasticWorkClass)
	require.True(t, available)
	require.Nil(t, handle)

	// Use up all the tokens trying to deduct the maximum+1 (tokensPerWorkClass)
	// tokens. There should be exactly tokensPerWorkClass tokens granted.
	granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, tokensPerWorkClass.regular+1)
	require.Equal(t, tokensPerWorkClass.regular, granted)

	// There should now be no tokens available for regular work and a handle
	// returned.
	available, handle = counter.TokensAvailable(admissionpb.RegularWorkClass)
	require.False(t, available)
	require.NotNil(t, handle)

	granted, haveTokens := handle.TryDeductAndUnblockNextWaiter(1)
	require.Equal(t, kvflowcontrol.Tokens(0), granted)
	require.False(t, haveTokens)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		// Wait on the handle to be unblocked and expect that there are tokens
		// available when the wait channel is signaled.
		<-handle.WaitChannel()
		granted, haveTokens = handle.TryDeductAndUnblockNextWaiter(0)
		require.Equal(t, kvflowcontrol.Tokens(0), granted)
		require.True(t, haveTokens)
		// Wait on the handle to be unblocked again, this time try deducting
		// exactly the amount of tokens returned (all of them). There should be no
		// tokens available after this deduction.
		<-handle.WaitChannel()
		granted, haveTokens = handle.TryDeductAndUnblockNextWaiter(tokensPerWorkClass.regular)
		require.Equal(t, tokensPerWorkClass.regular, granted)
		require.False(t, haveTokens)
		wg.Done()
	}()
	counter.Return(ctx, admissionpb.RegularWorkClass, tokensPerWorkClass.regular)
	wg.Wait()
}

type mockTokenWaitingHandle struct {
	available        bool
	unavailableLater bool
}

func (m *mockTokenWaitingHandle) TryDeductAndUnblockNextWaiter(
	tokens kvflowcontrol.Tokens,
) (granted kvflowcontrol.Tokens, available bool) {
	if m.unavailableLater {
		m.available = false
	}
	return 0, m.available
}

func (m *mockTokenWaitingHandle) WaitChannel() <-chan struct{} {
	ch := make(chan struct{})
	if m.available {
		close(ch)
	}
	return ch
}

// TestWaitForEval provides basic testing for the WaitForEval function. The
// tests use a mocked token counter handle, which is either always available,
// or available exactly once and later unavailable.
func TestWaitForEval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// All tokens are available and the request is granted immediately.
	t.Run("all available", func(t *testing.T) {
		handles := []tokenWaitingHandleInfo{
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: true},
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: false},
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: false},
		}
		refreshWaitCh := make(chan struct{})
		state, _ := WaitForEval(ctx, refreshWaitCh, handles, 2, nil)
		require.Equal(t, WaitSuccess, state)
	})
	t.Run("all available and required", func(t *testing.T) {
		handles := []tokenWaitingHandleInfo{
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: true},
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: true},
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: true},
		}
		refreshWaitCh := make(chan struct{})
		state, _ := WaitForEval(ctx, refreshWaitCh, handles, 2, nil)
		require.Equal(t, WaitSuccess, state)
	})
	// Partial tokens are available and the refreshWaitCh is signaled.
	t.Run("refresh signaled", func(t *testing.T) {
		handles := []tokenWaitingHandleInfo{
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: true},
			{handle: &mockTokenWaitingHandle{available: false}, requiredWait: false},
			{handle: &mockTokenWaitingHandle{available: false}, requiredWait: false},
		}
		refreshWaitCh := make(chan struct{}, 1)
		refreshWaitCh <- struct{}{}
		state, _ := WaitForEval(ctx, refreshWaitCh, handles, 2, nil)
		require.Equal(t, RefreshWaitSignaled, state)
	})
	// Both a required quorum are signaled and the required waiters are signaled
	// but later a quorum is unavailable.
	t.Run("quorum available but later unavailable", func(t *testing.T) {
		handles := []tokenWaitingHandleInfo{
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: false},
			{handle: &mockTokenWaitingHandle{available: false}, requiredWait: false},
			{handle: &mockTokenWaitingHandle{available: true, unavailableLater: true}, requiredWait: false},
		}
		refreshWaitCh := make(chan struct{})
		state, _ := WaitForEval(ctx, refreshWaitCh, handles, 2, nil)
		require.Equal(t, RefreshWaitSignaled, state)
	})
	// A required quorum of tokens are available but the required waiter is later
	// unavailable.
	t.Run("quorum available but later missing required", func(t *testing.T) {
		handles := []tokenWaitingHandleInfo{
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: false},
			{handle: &mockTokenWaitingHandle{available: true}, requiredWait: false},
			{handle: &mockTokenWaitingHandle{available: true, unavailableLater: true}, requiredWait: true},
		}
		refreshWaitCh := make(chan struct{})
		state, _ := WaitForEval(ctx, refreshWaitCh, handles, 2, nil)
		require.Equal(t, RefreshWaitSignaled, state)
	})
}
