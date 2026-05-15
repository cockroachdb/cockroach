// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiqueue

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Queue type constants used by the WeightedMultiQueue tests. Their integer
// values do not affect dispatch order — only the order of TypeConfig entries
// passed to NewWeightedMultiQueue does.
const (
	wHigh    = 100
	wLow     = 200
	wBlocker = 300
)

// verifyWMQOrder asserts that the given queue dispatches tasks in the
// specified order, releasing each permit before checking for the next.
func verifyWMQOrder(t *testing.T, queue *WeightedMultiQueue, tasks ...*Task[WeightedPermit]) {
	t.Helper()
	for i, task := range tasks {
		// No later task should be ready yet.
		for j, t2 := range tasks[i+1:] {
			select {
			case <-t2.GetWaitChan():
				require.Failf(t, "task ready out of order",
					"iter %d, future task index %d", i, i+1+j)
			default:
			}
		}
		select {
		case p := <-task.GetWaitChan():
			queue.Release(p)
		default:
			require.Failf(t, "expected task ready", "iter %d", i)
		}
	}
}

// TestWeightedMultiQueueWeightedRRRatio verifies that with both queue types
// backlogged, the dispatcher follows the configured weight ratio.
func TestWeightedMultiQueueWeightedRRRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 8},
		{QueueType: wLow, Weight: 1},
		{QueueType: wBlocker, Weight: 1},
	})
	// Hold the single concurrency slot so all subsequent Adds queue.
	blocker, err := queue.Add(wBlocker, 0, -1)
	require.NoError(t, err)

	const cycles = 3
	const highPerCycle = 8
	highTasks := make([]*Task[WeightedPermit], cycles*highPerCycle)
	for i := range highTasks {
		// Distinct, descending priorities → heap pops in insertion order.
		highTasks[i], err = queue.Add(wHigh, float64(len(highTasks)-i), -1)
		require.NoError(t, err)
	}
	lowTasks := make([]*Task[WeightedPermit], cycles)
	for i := range lowTasks {
		lowTasks[i], err = queue.Add(wLow, float64(len(lowTasks)-i), -1)
		require.NoError(t, err)
	}
	queue.Release(<-blocker.GetWaitChan())

	// blocker dispatches from cycle position 9 (it's the third declared type
	// with weight 1, so it owns the last slot of the 10-position cycle).
	// After dispatching, cyclePosition advances to (9+1) % 10 = 0, so high-pri
	// (positions 0..7) drains first, then low-pri (position 8), then
	// blocker's empty slot is skipped, then the cycle wraps. Pattern: 8H, 1L,
	// 8H, 1L, ...
	want := make([]*Task[WeightedPermit], 0, len(highTasks)+len(lowTasks))
	for c := 0; c < cycles; c++ {
		for j := 0; j < highPerCycle; j++ {
			want = append(want, highTasks[c*highPerCycle+j])
		}
		want = append(want, lowTasks[c])
	}
	verifyWMQOrder(t, queue, want...)
}

// TestWeightedMultiQueueEmptyHighDrainsLow verifies that when the high queue
// is empty, every dispatch goes to low (subject to its cap, if any).
func TestWeightedMultiQueueEmptyHighDrainsLow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 8},
		{QueueType: wLow, Weight: 1},
		{QueueType: wBlocker, Weight: 1},
	})
	blocker, err := queue.Add(wBlocker, 0, -1)
	require.NoError(t, err)

	l1, _ := queue.Add(wLow, 3, -1)
	l2, _ := queue.Add(wLow, 2, -1)
	l3, _ := queue.Add(wLow, 1, -1)

	queue.Release(<-blocker.GetWaitChan())
	verifyWMQOrder(t, queue, l1, l2, l3)
}

// TestWeightedMultiQueueLowCapEnforced verifies that the per-type cap on the
// low-pri queue prevents dispatch beyond the cap, even with free pool slots.
func TestWeightedMultiQueueLowCapEnforced(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(4, []TypeConfig{
		{QueueType: wHigh, Weight: 8},
		{QueueType: wLow, Weight: 1, MaxActive: 2},
	})

	// Fill the low-pri cap.
	low1, _ := queue.Add(wLow, 0, -1)
	low2, _ := queue.Add(wLow, 0, -1)
	p1 := <-low1.GetWaitChan()
	p2 := <-low2.GetWaitChan()

	// A third low-pri task must not be dispatched even though there are 2
	// free pool slots.
	low3, _ := queue.Add(wLow, 0, -1)
	select {
	case <-low3.GetWaitChan():
		t.Fatal("low3 should not be dispatched while low cap is reached")
	default:
	}

	// A high-pri task should receive a permit immediately from the free pool.
	high1, _ := queue.Add(wHigh, 0, -1)
	select {
	case p := <-high1.GetWaitChan():
		queue.Release(p)
	default:
		t.Fatal("high1 should be dispatched immediately")
	}

	// Releasing one of the low-pri tasks should free up a low-pri slot and
	// allow low3 to start.
	queue.Release(p1)
	select {
	case p := <-low3.GetWaitChan():
		queue.Release(p)
	default:
		t.Fatal("low3 should be dispatched after a low-pri permit is released")
	}
	queue.Release(p2)
}

// TestWeightedMultiQueueLowCapHighDrainsAll verifies that when low is at cap
// and high has waiters, freed slots flow to high regardless of cycle position.
func TestWeightedMultiQueueLowCapHighDrainsAll(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(3, []TypeConfig{
		{QueueType: wHigh, Weight: 8},
		{QueueType: wLow, Weight: 1, MaxActive: 1},
	})
	// Saturate the low-pri cap.
	low1, _ := queue.Add(wLow, 0, -1)
	pLow1 := <-low1.GetWaitChan()

	// Add a backlog of low-pri and high-pri waiters.
	low2, _ := queue.Add(wLow, 0, -1)
	low3, _ := queue.Add(wLow, 0, -1)
	high1, _ := queue.Add(wHigh, 0, -1)
	high2, _ := queue.Add(wHigh, 0, -1)

	// Free the low-pri slot; high1 dispatches (low at cap → skipped; cycle
	// favors high anyway).
	queue.Release(pLow1)
	pHigh1 := <-high1.GetWaitChan()

	// Free another slot; high2.
	queue.Release(pHigh1)
	pHigh2 := <-high2.GetWaitChan()

	// Free another slot; high backlog drained → low2 dispatches.
	queue.Release(pHigh2)
	pLow2 := <-low2.GetWaitChan()

	// low3 must wait until pLow2 is released because of the cap.
	select {
	case <-low3.GetWaitChan():
		t.Fatal("low3 should not be dispatched while low cap is reached")
	default:
	}
	queue.Release(pLow2)
	pLow3 := <-low3.GetWaitChan()
	queue.Release(pLow3)
}

// TestWeightedMultiQueueUpdateConfigTypeMaxActive verifies that runtime cap
// changes take effect immediately for new dispatches, and that relaxing the
// cap wakes every waiter newly made eligible (not just one).
func TestWeightedMultiQueueUpdateConfigTypeMaxActive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(4, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
		{QueueType: wLow, Weight: 1, MaxActive: 1},
	})

	low1, _ := queue.Add(wLow, 0, -1)
	pLow1 := <-low1.GetWaitChan()

	// low2 and low3 both block on the cap.
	low2, _ := queue.Add(wLow, 0, -1)
	low3, _ := queue.Add(wLow, 0, -1)
	for i, t2 := range []*Task[WeightedPermit]{low2, low3} {
		select {
		case <-t2.GetWaitChan():
			t.Fatalf("low%d should not be dispatched while low cap is reached", i+2)
		default:
		}
	}

	// Relax the cap to 3. Both low2 and low3 should now dispatch without any
	// release — UpdateConfig must drain all newly-eligible waiters.
	queue.UpdateConfig(WithTypeMaxActive(wLow, 3))
	p2 := <-low2.GetWaitChan()
	p3 := <-low3.GetWaitChan()
	queue.Release(p2)
	queue.Release(p3)
	queue.Release(pLow1)
}

// TestWeightedMultiQueueUpdateConfigConcurrencyLimit verifies raise (wakes
// multiple waiters) and lower (already-active unaffected, new dispatches
// respect the new cap).
func TestWeightedMultiQueueUpdateConfigConcurrencyLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
	})

	// Saturate the single slot, then queue two waiters.
	t1, _ := queue.Add(wHigh, 0, -1)
	p1 := <-t1.GetWaitChan()
	t2, _ := queue.Add(wHigh, 0, -1)
	t3, _ := queue.Add(wHigh, 0, -1)
	for i, task := range []*Task[WeightedPermit]{t2, t3} {
		select {
		case <-task.GetWaitChan():
			t.Fatalf("task t%d should not be dispatched at concurrency=1", i+2)
		default:
		}
	}

	// Raise to 3: both t2 and t3 should wake on the same call.
	queue.UpdateConfig(WithConcurrencyLimit(3))
	p2 := <-t2.GetWaitChan()
	p3 := <-t3.GetWaitChan()

	// Lower back to 1. Already-held permits are not revoked; remainingRuns
	// goes negative-then-clamped via the diff.
	queue.UpdateConfig(WithConcurrencyLimit(1))

	// A new task must wait until enough permits release to bring activity
	// below the new limit.
	t4, _ := queue.Add(wHigh, 0, -1)
	select {
	case <-t4.GetWaitChan():
		t.Fatal("t4 should not be dispatched while 3 permits are still held under limit=1")
	default:
	}
	queue.Release(p1)
	queue.Release(p2)
	select {
	case <-t4.GetWaitChan():
		t.Fatal("t4 should not be dispatched until activity drops below the new limit")
	default:
	}
	queue.Release(p3)
	p4 := <-t4.GetWaitChan()
	queue.Release(p4)
}

// TestWeightedMultiQueueDoubleRelease verifies that releasing the same
// permit twice panics with an assertion-failed error.
func TestWeightedMultiQueueDoubleRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
	})
	task, _ := queue.Add(wHigh, 0, -1)
	p := <-task.GetWaitChan()
	queue.Release(p)
	require.Panics(t, func() { queue.Release(p) })
}

// TestWeightedMultiQueueCancelDispatchRace exercises the tryDrainPermit
// branch in Cancel: the dispatcher delivered a permit before the caller
// received it, and Cancel arrives in the same window. The drained permit
// must be returned to the pool so that a queued waiter can dispatch.
func TestWeightedMultiQueueCancelDispatchRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
	})

	// Hold the only slot.
	hold, _ := queue.Add(wHigh, 0, -1)
	pHold := <-hold.GetWaitChan()

	// Queue two waiters. Releasing the held slot will dispatch the first.
	t1, _ := queue.Add(wHigh, 0, -1)
	t2, _ := queue.Add(wHigh, 0, -1)
	queue.Release(pHold)

	// t1's permit is now sitting in the channel. Cancel without receiving:
	// Cancel must drain the permit and release it so t2 can dispatch.
	queue.Cancel(t1)

	select {
	case p := <-t2.GetWaitChan():
		queue.Release(p)
	default:
		t.Fatal("t2 should dispatch after Cancel released t1's drained permit")
	}
}

// TestWeightedMultiQueueMaxQueueLength verifies that Add rejects when the
// pool is saturated and the queue length cap would be exceeded.
func TestWeightedMultiQueueMaxQueueLength(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
	})

	// Saturate the pool.
	hold, err := queue.Add(wHigh, 0, -1)
	require.NoError(t, err)
	pHold := <-hold.GetWaitChan()
	defer queue.Release(pHold)

	// First queued task fits under maxQueueLength=1 (queueLenLocked returns
	// 1 — "if one more is added").
	_, err = queue.Add(wHigh, 0, 1)
	require.NoError(t, err)

	// Second queued task pushes queue length to 2, exceeding the cap.
	_, err = queue.Add(wHigh, 0, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "queue full")
}

// TestWeightedMultiQueueAvailableLenQueueLen exercises the public
// AvailableLen and QueueLen accessors across saturation transitions.
func TestWeightedMultiQueueAvailableLenQueueLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(2, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
	})

	require.Equal(t, 2, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())

	// Take one slot.
	t1, _ := queue.Add(wHigh, 0, -1)
	p1 := <-t1.GetWaitChan()
	require.Equal(t, 1, queue.AvailableLen())
	require.Equal(t, 0, queue.QueueLen())

	// Take the second slot.
	t2, _ := queue.Add(wHigh, 0, -1)
	p2 := <-t2.GetWaitChan()
	require.Equal(t, 0, queue.AvailableLen())
	// Pool saturated: QueueLen reports the length if one more were added.
	require.Equal(t, 1, queue.QueueLen())

	// Queue a waiter; QueueLen now reports current(1) + hypothetical(1) = 2.
	_, _ = queue.Add(wHigh, 0, -1)
	require.Equal(t, 2, queue.QueueLen())

	queue.Release(p1)
	queue.Release(p2)
}

// TestWeightedMultiQueueUnknownType verifies that Add rejects an unknown
// queue type rather than panicking or silently misrouting.
func TestWeightedMultiQueueUnknownType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queue := NewWeightedMultiQueue(1, []TypeConfig{
		{QueueType: wHigh, Weight: 1},
	})
	_, err := queue.Add(wLow, 0, -1)
	require.Error(t, err)
}

// TestWeightedMultiQueueConstructorValidation verifies that NewWeightedMultiQueue
// panics on invalid configurations.
func TestWeightedMultiQueueConstructorValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.Panics(t, func() {
		NewWeightedMultiQueue(1, nil)
	}, "empty types")
	require.Panics(t, func() {
		NewWeightedMultiQueue(1, []TypeConfig{
			{QueueType: wHigh, Weight: 0},
		})
	}, "zero weight")
	require.Panics(t, func() {
		NewWeightedMultiQueue(1, []TypeConfig{
			{QueueType: wHigh, Weight: 1, MaxActive: -1},
		})
	}, "negative MaxActive")
	require.Panics(t, func() {
		NewWeightedMultiQueue(1, []TypeConfig{
			{QueueType: wHigh, Weight: 1},
			{QueueType: wHigh, Weight: 1},
		})
	}, "duplicate QueueType")
}
