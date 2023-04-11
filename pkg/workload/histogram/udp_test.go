package histogram

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUdpSendReceive(t *testing.T) {
	operations := []string{"op1", "op2"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := CreateUdpReceiver("localhost:12345", operations)
	go func() {
		r.Listen(ctx)
	}()

	s := CreateUdpPublisher("localhost:12345", operations)
	defer s.Close()

	s.Observe(time.Second, "op1")
	s.Observe(2*time.Second, "op1")
	s.Observe(3*time.Second, "op1")
	s.Observe(time.Millisecond, "op2")
	time.Sleep(100 * time.Millisecond)

	// Get the current values and reset the histograms
	histograms := r.Tick()
	require.EqualValues(t, 3, histograms["op1"].TotalCount())
	require.EqualValues(t, 1, histograms["op2"].TotalCount())
	// The mean of the histogram depends on the buckets, allow some variation.
	require.InEpsilon(t, 2*time.Second, histograms["op1"].Mean(), .02)
	require.InEpsilon(t, time.Millisecond, histograms["op2"].Mean(), .02)

	// Tick again and observe new values.
	s.Observe(time.Second, "op1")
	s.Observe(time.Millisecond, "op2")
	time.Sleep(100 * time.Millisecond)
	histograms = r.Tick()
	require.EqualValues(t, 1, histograms["op1"].TotalCount())
}
