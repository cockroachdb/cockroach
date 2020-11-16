// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanlatch

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignal(t *testing.T) {
	var s signal
	require.False(t, s.signaled())

	s.signal()
	require.True(t, s.signaled())
	require.Equal(t, struct{}{}, <-s.signalChan())
}

func TestSignalConcurrency(t *testing.T) {
	const trials = 100
	for i := 0; i < trials; i++ {
		var s signal
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-s.signalChan()
			require.True(t, s.signaled())
		}()
		go func() {
			defer wg.Done()
			require.False(t, s.signaled())
			s.signal()
			require.True(t, s.signaled())
		}()
		go func() {
			defer wg.Done()
			<-s.signalChan()
			require.True(t, s.signaled())
		}()
		wg.Wait()
		require.True(t, s.signaled())
	}
}

func BenchmarkSignaled(b *testing.B) {
	var s signal
	s.signal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.signaled()
	}
}

func BenchmarkSignalBeforeChan(b *testing.B) {
	var s signal
	for i := 0; i < b.N; i++ {
		s = signal{} // reset
		s.signal()
	}
}

func BenchmarkSignalAfterChan(b *testing.B) {
	var s signal
	chans := make([]chan struct{}, b.N)
	for i := range chans {
		chans[i] = make(chan struct{})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s = signal{} // reset
		s.c = chanToPtr(chans[i])
		s.signal()
	}
}

func BenchmarkInitialChanBeforeSignal(b *testing.B) {
	var s signal
	for i := 0; i < b.N; i++ {
		s = signal{} // reset
		_ = s.signalChan()
	}
}

func BenchmarkSecondChanBeforeSignal(b *testing.B) {
	var s signal
	_ = s.signalChan()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.signalChan()
	}
}

func BenchmarkInitialChanAfterSignal(b *testing.B) {
	var s signal
	s.signal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.c = nil
		_ = s.signalChan()
	}
}

func BenchmarkSecondChanAfterSignal(b *testing.B) {
	var s signal
	s.signal()
	_ = s.signalChan()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.signalChan()
	}
}

// The following is a series of benchmarks demonstrating the value of the signal
// type and the fast-path that it provides. Closing channels to signal
// completion of a task is convenient, but in performance critical code paths it
// is essential to have a way to efficiently check for completion before falling
// back to waiting for the channel to close and entering select blocks. The
// benchmarks demonstrate that a channel on its own cannot be used to perform an
// efficient completion check, which is why the signal type mixes channels with
// atomics. The reason for this is that channels are forced to acquire an
// internal mutex before determining that they are closed and can return a zero
// value. This will always be more expensive than a single atomic load.
//
// Results with go1.10.4 on a Mac with a 3.1 GHz Intel Core i7 processor:
//
//   ReadClosedChan-4           24.2ns ± 3%
//   SingleSelectClosedChan-4   24.9ns ± 2%
//   DefaultSelectClosedChan-4  24.6ns ± 1%
//   MultiSelectClosedChan-4    97.9ns ± 2%
//   Signaled-4                 0.35ns ±13%
//

func BenchmarkReadClosedChan(b *testing.B) {
	c := make(chan struct{})
	close(c)
	for i := 0; i < b.N; i++ {
		<-c
	}
}

func BenchmarkSingleSelectClosedChan(b *testing.B) {
	c := make(chan struct{})
	close(c)
	for i := 0; i < b.N; i++ {
		//lint:ignore S1000 we don't want this simplified
		select {
		case <-c:
		}
	}
}

func BenchmarkDefaultSelectClosedChan(b *testing.B) {
	c := make(chan struct{})
	close(c)
	for i := 0; i < b.N; i++ {
		select {
		case <-c:
		default:
		}
	}
}

func BenchmarkMultiSelectClosedChan(b *testing.B) {
	c, c2 := make(chan struct{}), make(chan struct{})
	close(c)
	for i := 0; i < b.N; i++ {
		select {
		case <-c:
		case <-c2:
		}
	}
}

func BenchmarkAtomicLoad(b *testing.B) {
	a := int32(1)
	for i := 0; i < b.N; i++ {
		_ = atomic.LoadInt32(&a)
	}
}
