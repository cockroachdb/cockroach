package kvflowconnectedstream

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
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
	clock := hlc.NewClockForTesting(hlc.NewHybridManualClock())
	counter := newTokenCounter(tokensPerWorkClass, clock)

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
