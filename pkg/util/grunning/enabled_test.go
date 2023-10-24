// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// See grunning.Supported() for an explanation behind this build tag.
//
//go:build !((linux && s390x) || !bazel)
// +build !linux !s390x
// +build bazel

package grunning_test

import (
	"runtime"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// This file tests various properties we should expect over the running time
// value. It does not make assertions given the CI environments we run these
// under (CPU-starved, lot of OS thread pre-emption, dissimilar to healthy CRDB
// deployments). This is also why they're skipped under stress. Still, these
// tests are useful to understand the properties we expect running time to have.

func TestEnabled(t *testing.T) {
	require.True(t, grunning.Supported())
}

// TestEquivalentGoroutines is a variant of the "parallel test" in
// https://github.com/golang/go/issues/36821. It tests whether goroutines that
// (should) spend the same amount of time on-CPU have similar measured on-CPU
// time.
func TestEquivalentGoroutines(t *testing.T) {
	skip.UnderStress(t, "not applicable")

	f := func(wg *sync.WaitGroup, result *int64) {
		defer wg.Done()

		var sum int
		for i := 0; i < 500000000; i++ {
			sum -= i / 2
			sum *= i
			sum /= i/3 + 1
			sum -= i / 4
		}

		nanos := grunning.Time().Nanoseconds()
		*result = nanos
	}

	const threads = 10
	var wg sync.WaitGroup
	results := make([]int64, threads)
	for i := 0; i < threads; i++ {
		i := i // copy loop variable
		wg.Add(1)
		go f(&wg, &results[i])
	}
	wg.Wait()

	total := int64(0)
	for _, result := range results {
		total += result
	}

	exp := 1.0 / threads
	for i, result := range results {
		got := float64(result) / float64(total)

		t.Logf("thread=%02d expected≈%5.2f%% got=%5.2f%% of on-cpu time",
			i+1, exp*100, got*100)
	}
}

// TestProportionalGoroutines is a variant of the "serial test" in
// https://github.com/golang/go/issues/36821. It tests whether goroutines that
// (should) spend a proportion of time on-CPU actually do so as measured by this
// package.
func TestProportionalGoroutines(t *testing.T) {
	skip.UnderStress(t, "not applicable")
	type resultContainer struct {
		mu    *syncutil.Mutex
		value int64
	}
	f := func(wg *sync.WaitGroup, v uint64, trip uint64, result *resultContainer) {
		defer wg.Done()

		ret := v
		for i := trip; i > 0; i-- {
			ret += i
			ret = ret ^ (i + 0xcafebabe)
		}

		nanos := grunning.Time().Nanoseconds()
		result.mu.Lock()
		defer result.mu.Unlock()
		result.value = nanos
	}

	results := make([]resultContainer, 10)
	for i := 0; i < len(results); i++ {
		results[i] = resultContainer{
			mu: &syncutil.Mutex{},
		}
	}
	var wg sync.WaitGroup

	for iters := 0; iters < 10000; iters++ {
		for i := uint64(0); i < 10; i++ {
			i := i // copy loop variable
			wg.Add(1)
			go f(&wg, i+1, (i+1)*100000, &results[i])
		}
	}

	wg.Wait()

	total := int64(0)
	for _, result := range results {
		total += result.value
	}

	initial := float64(results[0].value) / float64(total)

	for i, result := range results {
		got := float64(result.value) / float64(total)
		mult := got / initial
		t.Logf("thread=%02d got %5.2f%% of on-cpu time: expected≈%5.2fx got=%4.2fx ",
			i+1, got*100, float64(i+1), mult)
	}
}

// TestPingPongHog is adapted from a benchmark in the Go runtime, forcing the
// scheduler to continually schedule goroutines. It demonstrates that if two
// goroutines alternately cycle between running and waiting, they will get
// similar running times.
func TestPingPongHog(t *testing.T) {
	skip.UnderStress(t, "not applicable")

	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	// Create a CPU hog. It makes the two goroutines that want to cycle between
	// running and waiting also have to wait in runnable state, until the CPU
	// hog is finished with its time slice.
	stop, done := make(chan bool), make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				done <- true
				return
			default: //lint:ignore SA5004 empty default case is intentional, we want it to spin
			}
		}
	}()

	// Ping-pong 1000000 times.
	const large = 1000000
	ping, pong := make(chan bool), make(chan bool)
	var pingern, pongern int64
	go func() {
		for j := 0; j < large; j++ {
			pong <- <-ping
		}
		pingern = grunning.Time().Nanoseconds()
		close(stop) // stop the CPU hog
		done <- true
	}()
	go func() {
		for i := 0; i < large; i++ {
			ping <- <-pong
		}
		pongern = grunning.Time().Nanoseconds()
		done <- true
	}()
	ping <- true // start ping-pong
	<-stop       // wait until the pinger tells the CPU hog to stop
	<-ping       // wait for the ponger to finish
	<-done       // make sure goroutines exit
	<-done
	<-done

	mult := float64(pingern) / float64(pongern)
	t.Logf("pinger/ponger expected≈1.00x got=%0.2fx", mult)
}

// BenchmarkGRunningTime measures how costly it is to read the current
// goroutine's running time. Results:
//
//	goos: linux
//	goarch: amd64
//	cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
//	BenchmarkGRunningTime
//	BenchmarkGRunningTime-24        38336452                31.59 ns/op
func BenchmarkGRunningTime(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = grunning.Time()
	}
}
