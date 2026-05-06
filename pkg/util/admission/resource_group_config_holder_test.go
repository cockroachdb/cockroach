// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

// formatSnapshot renders a snapshot map deterministically: one line per
// key, sorted ascending by id. Used only by the echotest goldens.
func formatSnapshot(snap map[groupKey]ResourceGroupConfig) string {
	keys := make([]groupKey, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].id < keys[j].id })
	var b strings.Builder
	for _, k := range keys {
		cfg := snap[k]
		fmt.Fprintf(&b, "%s weight=%d maxCPU=%t\n", k, cfg.Weight, cfg.MaxCPU)
	}
	return b.String()
}

// TestResourceGroupConfigHolder pins the constructor seed and the
// unknown-ID fallback against goldens. The behavioral cases for Set,
// GetOrDefault, and Snapshot live in adjacent TestResourceGroupConfigHolder*
// functions.
func TestResourceGroupConfigHolder(t *testing.T) {
	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))

	t.Run("constructor_seed", w.Run(t, "constructor_seed", func(t *testing.T) string {
		h := newResourceGroupConfigHolder()
		return formatSnapshot(h.Snapshot())
	}))

	t.Run("get_or_default_unknown", w.Run(t, "get_or_default_unknown", func(t *testing.T) string {
		h := newResourceGroupConfigHolder()
		cfg := h.GetOrDefault(rgGroupKey(9999))
		return fmt.Sprintf("weight=%d maxCPU=%t\n", cfg.Weight, cfg.MaxCPU)
	}))
}

// TestResourceGroupConfigHolderSet covers Set's wholesale-replace and
// input-aliasing contracts.
func TestResourceGroupConfigHolderSet(t *testing.T) {
	t.Run("replaces_wholesale_dropping_seed", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		h.Set(map[groupKey]ResourceGroupConfig{
			rgGroupKey(42): {Weight: 100, MaxCPU: false},
		})
		// Set is wholesale: the seed (high/low) must be gone, and only the
		// freshly-Set key must remain.
		require.Equal(t, map[groupKey]ResourceGroupConfig{
			rgGroupKey(42): {Weight: 100, MaxCPU: false},
		}, h.Snapshot())
	})

	t.Run("input_aliasing_safe", func(t *testing.T) {
		h := newResourceGroupConfigHolder()
		input := map[groupKey]ResourceGroupConfig{
			rgGroupKey(7): {Weight: 100, MaxCPU: false},
		}
		h.Set(input)

		// Mutate the input post-Set; the holder must not observe it.
		input[rgGroupKey(7)] = ResourceGroupConfig{Weight: 1, MaxCPU: true}
		input[rgGroupKey(8)] = ResourceGroupConfig{Weight: 1, MaxCPU: false}

		require.Equal(t, map[groupKey]ResourceGroupConfig{
			rgGroupKey(7): {Weight: 100, MaxCPU: false},
		}, h.Snapshot())

		// And the holder's internal map is not aliased to the input.
		// Same-package access lets us check the underlying pointer.
		h.mu.RLock()
		internalPtr := reflect.ValueOf(h.mu.config).Pointer()
		h.mu.RUnlock()
		require.NotEqual(t, reflect.ValueOf(input).Pointer(), internalPtr,
			"holder must not retain a reference to the caller's input map")
	})
}

// TestResourceGroupConfigHolderGet covers GetOrDefault for a configured key.
func TestResourceGroupConfigHolderGet(t *testing.T) {
	h := newResourceGroupConfigHolder()
	h.Set(map[groupKey]ResourceGroupConfig{
		rgGroupKey(highResourceGroupID): {Weight: 75, MaxCPU: true},
	})
	require.Equal(t,
		ResourceGroupConfig{Weight: 75, MaxCPU: true},
		h.GetOrDefault(rgGroupKey(highResourceGroupID)))
}

// TestResourceGroupConfigHolderSnapshot verifies Snapshot returns an
// independent map: mutating it does not affect subsequent snapshots, and
// the returned map is not aliased to the holder's internal storage.
func TestResourceGroupConfigHolderSnapshot(t *testing.T) {
	h := newResourceGroupConfigHolder()
	snap1 := h.Snapshot()

	// Mutate snap1 and confirm the holder's state is unchanged.
	snap1[rgGroupKey(999)] = ResourceGroupConfig{Weight: 1, MaxCPU: true}
	delete(snap1, rgGroupKey(highResourceGroupID))
	require.Equal(t, defaultRMResourceGroupConfig, h.Snapshot())

	// Snapshot must return a fresh map each call, not an alias of the
	// internal storage. Pointer-identity check guards against a regression
	// that returns the internal map directly.
	h.mu.RLock()
	internalPtr := reflect.ValueOf(h.mu.config).Pointer()
	h.mu.RUnlock()
	snap2 := h.Snapshot()
	require.NotEqual(t, internalPtr, reflect.ValueOf(snap2).Pointer(),
		"Snapshot must not alias the holder's internal map")
}

// TestResourceGroupConfigHolderConcurrent exercises Set, GetOrDefault, and
// Snapshot from multiple goroutines. The holder exists to be safely callable
// from multiple goroutines (e.g. WorkQueue.Admit while a SQL operator
// changes resource groups), so a regression that drops a defensive copy or
// otherwise leaks the internal map should surface here under -race. Each
// Snapshot caller mutates the returned map to catch any aliasing.
func TestResourceGroupConfigHolderConcurrent(t *testing.T) {
	const goroutines = 8
	const iterations = 200

	h := newResourceGroupConfigHolder()
	var wg sync.WaitGroup
	wg.Add(3 * goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				h.Set(map[groupKey]ResourceGroupConfig{
					rgGroupKey(uint64(i)): {Weight: uint32(j % 100), MaxCPU: j%2 == 0},
				})
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = h.GetOrDefault(rgGroupKey(uint64(i)))
			}
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				snap := h.Snapshot()
				snap[rgGroupKey(9999)] = ResourceGroupConfig{Weight: 99, MaxCPU: true}
				_ = snap
			}
		}()
	}
	wg.Wait()
}
