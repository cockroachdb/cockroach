// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// registerAndCloseOnNewGoroutine registers a new goroutine with h and closes
// its handle before the goroutine exits.
func registerAndCloseOnNewGoroutine(ctx context.Context, h *SQLCPUHandle) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		gh := h.RegisterGoroutine()
		gh.Close(ctx)
	}()
	wg.Wait()
}

// TestSQLCPUHandleGoroutineCloseUnregisters verifies that closing a
// GoroutineCPUHandle removes it from the parent SQLCPUHandle.
func TestSQLCPUHandleGoroutineCloseUnregisters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(SQLWorkInfo{AtGateway: true}, provider)
	defer h.Close()

	main := h.RegisterGoroutine()
	defer main.Close(ctx)
	require.Equal(t, 1, h.TestingNumRegisteredGoroutines())

	const n = 10000
	for i := 0; i < n; i++ {
		registerAndCloseOnNewGoroutine(ctx, h)
	}
	require.Equal(t, 1, h.TestingNumRegisteredGoroutines(),
		"closed goroutine handles must be removed from the parent")
	// The long-lived registration survives the churn.
	require.Same(t, main, h.RegisterGoroutine())
	// The backing array for two handles is never outgrown by serial churn.
	require.Equal(t, len(h.mu.handlesBacking), cap(h.mu.gHandles))
}

// TestSQLCPUHandleCloseRemovesOnlyOwnHandle closes concurrently registered
// goroutines in non-FIFO order and checks that the others keep their handles.
func TestSQLCPUHandleCloseRemovesOnlyOwnHandle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(SQLWorkInfo{AtGateway: true}, provider)
	defer h.Close()

	type cmd int
	const (
		cmdLookup cmd = iota
		cmdClose
	)
	type worker struct {
		cmds    chan cmd
		results chan *GoroutineCPUHandle
		gh      *GoroutineCPUHandle
	}
	const numWorkers = 5
	workers := make([]*worker, numWorkers)
	var wg sync.WaitGroup
	for i := range workers {
		w := &worker{cmds: make(chan cmd), results: make(chan *GoroutineCPUHandle)}
		workers[i] = w
		wg.Add(1)
		go func() {
			defer wg.Done()
			gh := h.RegisterGoroutine()
			w.results <- gh
			for c := range w.cmds {
				switch c {
				case cmdLookup:
					w.results <- h.RegisterGoroutine()
				case cmdClose:
					gh.Close(ctx)
					w.results <- nil // acknowledge
					return
				}
			}
		}()
		w.gh = <-w.results
	}
	alive := map[int]bool{}
	for i := range workers {
		alive[i] = true
	}
	// On assertion failure, release the surviving workers so wg.Wait returns.
	defer func() {
		for i := range alive {
			close(workers[i].cmds)
		}
		wg.Wait()
	}()
	require.Equal(t, numWorkers, h.TestingNumRegisteredGoroutines())

	for _, i := range []int{1, 3, 0, 4, 2} {
		workers[i].cmds <- cmdClose
		<-workers[i].results
		close(workers[i].cmds)
		delete(alive, i)
		// Closing a handle must not disturb the remaining registrations.
		require.Equal(t, len(alive), h.TestingNumRegisteredGoroutines(), "after closing worker %d", i)
		for j := range alive {
			workers[j].cmds <- cmdLookup
			require.Same(t, workers[j].gh, <-workers[j].results,
				"worker %d lost its registration after worker %d closed", j, i)
		}
	}
	require.Zero(t, h.TestingNumRegisteredGoroutines())
}

// BenchmarkSQLCPUHandleRegisterGoroutine measures RegisterGoroutine for an
// already registered goroutine after other goroutines registered and closed.
func BenchmarkSQLCPUHandleRegisterGoroutine(b *testing.B) {
	ctx := context.Background()
	for _, prior := range []int{0, 1000, 100000} {
		b.Run(fmt.Sprintf("prior=%d", prior), func(b *testing.B) {
			provider := &sqlCPUProviderImpl{}
			h := newSQLCPUAdmissionHandle(SQLWorkInfo{AtGateway: true}, provider)
			defer h.Close()
			main := h.RegisterGoroutine()
			defer main.Close(ctx)
			for i := 0; i < prior; i++ {
				registerAndCloseOnNewGoroutine(ctx, h)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if gh := h.RegisterGoroutine(); gh != main {
					b.Fatal("unexpected handle")
				}
			}
		})
	}
}
