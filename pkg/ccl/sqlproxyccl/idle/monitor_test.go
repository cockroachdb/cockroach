// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package idle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMonitor(t *testing.T) {
	const timeout = 100 * time.Millisecond
	defer leaktest.AfterTest(t)()

	t.Run("subset of connections idled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mon := NewMonitor(ctx, timeout)

		var idled int64
		createTestConns := func(addr string, n int) {
			for i := 0; i < n; i++ {
				conn := &testConn{Addr: addr}
				wrapped := mon.DetectIdle(conn, func() {
					if addr != "addr2" {
						t.Errorf("did not expect %s connections to be idled", addr)
						return
					}
					atomic.AddInt64(&idled, 1)
				})
				t.Cleanup(func() {
					_ = wrapped.Close()
				})
			}
		}

		createTestConns("addr1", 5)
		createTestConns("addr2", 5)
		createTestConns("addr3", 5)

		// Idle only connections to addr2.
		mon.SetIdleChecks("addr2")

		require.Eventually(t, func() bool {
			// >= 5 to handle race condition where the same connection is marked
			// idle multiple times.
			return atomic.LoadInt64(&idled) >= 5
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("don't idle busy connections", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mon := NewMonitor(ctx, timeout)

		wrapped1 := mon.DetectIdle(&testConn{Addr: "test"}, func() {
			t.Error("did not expect conn #1 to be idled")
		})

		wrapped2 := mon.DetectIdle(&testConn{Addr: "test"}, func() {
			t.Error("did not expect conn #2 to be idled")
		})

		var idled int64
		_ = mon.DetectIdle(&testConn{Addr: "test"}, func() {
			atomic.AddInt64(&idled, 1)
		})

		mon.SetIdleChecks("test")

		require.Eventually(t, func() bool {
			_, _ = wrapped1.Read(nil)
			_, _ = wrapped2.Write(nil)

			// >= 1 to handle race condition where the same connection is marked
			// idle multiple times.
			return atomic.LoadInt64(&idled) >= 1
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("ClearIdleChecks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mon := NewMonitor(ctx, timeout)

		_ = mon.DetectIdle(&testConn{Addr: "test"}, func() {
			t.Error("did not expect connection to be idled")
		})

		require.Equal(t, 0, mon.countAddrsToCheck())
		mon.SetIdleChecks("test")
		require.Equal(t, 1, mon.countAddrsToCheck())
		mon.ClearIdleChecks("test")
		require.Equal(t, 0, mon.countAddrsToCheck())
	})

	t.Run("close wrapped connection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mon := NewMonitor(ctx, timeout)

		wrapped1 := mon.DetectIdle(&testConn{Addr: "test"}, func() {})
		wrapped2 := mon.DetectIdle(&testConn{Addr: "test"}, func() {})
		_ = mon.DetectIdle(&testConn{Addr: "test2"}, func() {})

		require.Equal(t, 2, mon.countConnsToAddr("test"))

		require.Equal(t, 0, mon.countAddrsToCheck())
		mon.SetIdleChecks("test")
		require.Equal(t, 1, mon.countAddrsToCheck())

		// Closing the wrapped connection should remove it from the set of
		// monitored connections.
		wrapped1.Close()
		require.Equal(t, 1, mon.countConnsToAddr("test"))
		require.Equal(t, 1, mon.countAddrsToCheck())

		wrapped2.Close()
		require.Equal(t, 0, mon.countConnsToAddr("test"))
		require.Equal(t, 1, mon.countAddrsToCheck())

		mon.ClearIdleChecks("test")
		require.Equal(t, 0, mon.countAddrsToCheck())
	})
}

func TestMonitorStress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numConns = 100
	const timeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mon := NewMonitor(ctx, timeout)

	var wg sync.WaitGroup
	var idled [numConns]int64
	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(i int) {
			addr := fmt.Sprintf("addr%d", i%3)
			log.Infof(ctx, "Start connection %s", addr)

			conn := &testConn{Addr: addr}
			wrapped := mon.DetectIdle(conn, func() {
				atomic.StoreInt64(&idled[i], 1)
			})
			t.Cleanup(func() {
				_ = wrapped.Close()
			})

			wg.Done()

			// Send traffic on 1/2 the connections.
			if i%2 == 0 {
				for {
					// Read 1/2 the connections, write the other 1/2.
					if i%4 == 0 {
						_, _ = wrapped.Read(nil)
					} else {
						_, _ = wrapped.Write(nil)
					}
					time.Sleep(timeout / 10)
					if ctx.Err() != nil {
						break
					}
				}
			}
		}(i)
	}

	// Wait for all goroutines to get running.
	wg.Wait()

	// Detect idleness on 2 of 3 addresses.
	log.Info(ctx, "SetIdleChecks")
	mon.SetIdleChecks("addr0")
	mon.SetIdleChecks("addr2")

	// Let the background routines run for a bit.
	time.Sleep(timeout * 3)

	// Cancel the traffic and ensure the right number of connections have been
	// idled.
	log.Info(ctx, "Cancel")
	cancel()
	for i := 0; i < numConns; i++ {
		idled := int(atomic.LoadInt64(&idled[i]))

		// Only odd connections are idled, and only if they are not targeting
		// addr1.
		if i%2 == 0 || i%3 == 1 {
			require.Equal(t, 0, idled, "connection #%d", i)
		} else {
			require.Equal(t, 1, idled, "connection #%d", i)
		}
	}
}
