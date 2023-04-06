// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mon

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// randomSize generates a size greater or equal to zero, with a random
// distribution that is skewed towards zero and ensures that most
// generated values are smaller than `mag`.
func randomSize(rnd *rand.Rand, mag int64) int64 {
	return int64(rnd.ExpFloat64() * float64(mag) * 0.3679)
}

func TestMemoryAllocations(t *testing.T) {
	maxs := []int64{1, 9, 10, 11, 99, 100, 101, 0}
	hysteresisFactors := []int{1, 2, 10, 10000}
	poolAllocSizes := []int64{1, 2, 9, 10, 11, 100}
	preBudgets := []int64{0, 1, 2, 9, 10, 11, 100}

	rnd, seed := randutil.NewTestRand()
	t.Logf("random seed: %v", seed)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	var pool *BytesMonitor
	var paramHeader func()

	m := NewMonitor("test", MemoryResource, nil, nil, 0, 1000, st)
	m.StartNoReserved(ctx, nil /* pool */)
	accs := make([]BoundAccount, 4)
	for i := range accs {
		accs[i] = m.MakeBoundAccount()
	}

	// The following invariants will be checked at every step of the
	// test underneath.
	checkInvariants := func() {
		t.Helper()

		var sum int64
		fail := false
		for accI := range accs {
			if accs[accI].used < 0 {
				t.Errorf("account %d went negative: %d", accI, accs[accI].used)
				fail = true
			}
			sum += accs[accI].Allocated()
		}
		if m.mu.curAllocated < 0 {
			t.Errorf("monitor current count went negative: %d", m.mu.curAllocated)
			fail = true
		}
		if sum != m.mu.curAllocated {
			t.Errorf("total account sum %d different from monitor count %d", sum, m.mu.curAllocated)
			fail = true
		}
		if m.mu.curBudget.used < 0 {
			t.Errorf("monitor current budget went negative: %d", m.mu.curBudget.used)
			fail = true
		}
		avail := m.mu.curBudget.Allocated() + m.reserved.used
		if sum > avail {
			t.Errorf("total account sum %d greater than total monitor budget %d", sum, avail)
			fail = true
		}
		if pool.mu.curAllocated > pool.reserved.used {
			t.Errorf("pool cur %d exceeds max %d", pool.mu.curAllocated, pool.reserved.used)
			fail = true
		}
		if m.mu.curBudget.Allocated() != pool.mu.curAllocated {
			t.Errorf("monitor budget %d different from pool cur %d", m.mu.curBudget.used, pool.mu.curAllocated)
			fail = true
		}

		if fail {
			t.Fatal("invariants not preserved")
		}
	}

	const numAccountOps = 200
	var linesBetweenHeaderReminders int
	var generateHeader func()
	var reportAndCheck func(string, ...interface{})
	if log.V(2) {
		// Detailed output: report the intermediate values of the
		// important variables at every stage of the test.
		linesBetweenHeaderReminders = 5
		generateHeader = func() {
			fmt.Println("")
			paramHeader()
			fmt.Printf(" mcur  mbud  mpre  pool ")
			for accI := range accs {
				fmt.Printf("%5s ", fmt.Sprintf("a%d", accI))
			}
			fmt.Println("")
		}
		reportAndCheck = func(extraFmt string, extras ...interface{}) {
			t.Helper()
			fmt.Printf("%5d %5d %5d %5d ", m.mu.curAllocated, m.mu.curBudget.used, m.reserved.used, pool.mu.curAllocated)
			for accI := range accs {
				fmt.Printf("%5d ", accs[accI].used)
			}
			fmt.Print("\t")
			fmt.Printf(extraFmt, extras...)
			fmt.Println("")
			checkInvariants()
		}
	} else {
		// More compact output.
		linesBetweenHeaderReminders = numAccountOps
		if testing.Verbose() {
			generateHeader = func() { paramHeader() }
		} else {
			generateHeader = func() {}
		}
		reportAndCheck = func(_ string, _ ...interface{}) {
			t.Helper()
			checkInvariants()
		}
	}

	for _, max := range maxs {
		pool = NewMonitor("test", MemoryResource, nil, nil, 1, 1000, st)
		pool.Start(ctx, nil, NewStandaloneBudget(max))

		for _, hf := range hysteresisFactors {
			maxAllocatedButUnusedBlocks = hf

			for _, pb := range preBudgets {
				mmax := pb + max

				for _, pa := range poolAllocSizes {
					paramHeader = func() { fmt.Printf("max %d, pb %d, as %d, hf %d\n", max, pb, pa, hf) }

					// We start with a fresh monitor for every set of
					// parameters.
					m = NewMonitor("test", MemoryResource, nil, nil, pa, 1000, st)
					m.Start(ctx, pool, NewStandaloneBudget(pb))

					for i := 0; i < numAccountOps; i++ {
						if i%linesBetweenHeaderReminders == 0 {
							generateHeader()
						}

						// The following implements a random operation generator.
						// At every test iteration a random account is selected
						// and then a random operation is performed for that
						// account.

						accI := rnd.Intn(len(accs))
						switch rnd.Intn(3 /* number of states below */) {
						case 0:
							sz := randomSize(rnd, mmax)
							reportAndCheck("G [%5d] %5d", accI, sz)
							err := accs[accI].Grow(ctx, sz)
							if err == nil {
								reportAndCheck("G [%5d] ok", accI)
							} else {
								reportAndCheck("G [%5d] %s", accI, err)
							}
						case 1:
							reportAndCheck("C [%5d]", accI)
							accs[accI].Clear(ctx)
							reportAndCheck("C [%5d]", accI)
						case 2:
							osz := rnd.Int63n(accs[accI].used + 1)
							nsz := randomSize(rnd, mmax)
							reportAndCheck("R [%5d] %5d %5d", accI, osz, nsz)
							err := accs[accI].Resize(ctx, osz, nsz)
							if err == nil {
								reportAndCheck("R [%5d] ok", accI)
							} else {
								reportAndCheck("R [%5d] %s", accI, err)
							}
						}
					}

					// After all operations have been performed, ensure
					// that closing everything comes back to the initial situation.
					for accI := range accs {
						reportAndCheck("CL[%5d]", accI)
						accs[accI].Clear(ctx)
						reportAndCheck("CL[%5d]", accI)
					}

					m.Stop(ctx)
					if pool.mu.curAllocated != 0 {
						t.Fatalf("pool not empty after monitor close: %d", pool.mu.curAllocated)
					}
				}
			}
		}
		pool.Stop(ctx)
	}
}

func TestBoundAccount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := NewMonitor("test", MemoryResource, nil, nil, 1, 1000, st)
	m.Start(ctx, nil, NewStandaloneBudget(100))
	m.poolAllocationSize = 1
	maxAllocatedButUnusedBlocks = 1

	a1 := m.MakeBoundAccount()
	a2 := m.MakeBoundAccount()
	if err := a1.Grow(ctx, 10); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := a2.Grow(ctx, 30); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := a1.Grow(ctx, 61); err == nil {
		t.Fatalf("monitor accepted excessive allocation")
	}

	if err := a2.Grow(ctx, 61); err == nil {
		t.Fatalf("monitor accepted excessive allocation")
	}

	a1.Clear(ctx)

	if err := a2.Grow(ctx, 61); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := a2.Resize(ctx, 50, 60); err == nil {
		t.Fatalf("monitor accepted excessive allocation")
	}

	if err := a1.Resize(ctx, 0, 5); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := a2.Resize(ctx, a2.used, 40); err != nil {
		t.Fatalf("monitor refused reset + allocation: %v", err)
	}

	a1.Close(ctx)
	a2.Close(ctx)

	if m.mu.curAllocated != 0 {
		t.Fatal("closing spans leaves bytes in monitor")
	}

	if m2 := a1.Monitor(); m2 != m {
		t.Fatalf("a1.Monitor() returned %v, wanted %v", m2, &m)
	}

	m.Stop(ctx)
}

func TestNilBoundAccount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var ba *BoundAccount
	_ = ba.Used()
	_ = ba.Monitor()
	_ = ba.Allocated()
	ba.Empty(ctx)
	ba.Clear(ctx)
	ba.Close(ctx)
	require.Nil(t, ba.Resize(ctx, 0, 10))
	require.Nil(t, ba.ResizeTo(ctx, 10))
	require.Nil(t, ba.Grow(ctx, 10))
	ba.Shrink(ctx, 10)
}

func TestBytesMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := NewMonitor("test", MemoryResource, nil, nil, 1, 1000, st)
	m.Start(ctx, nil, NewStandaloneBudget(100))
	maxAllocatedButUnusedBlocks = 1

	if err := m.reserveBytes(ctx, 10); err != nil {
		t.Fatalf("monitor refused small allocation: %v", err)
	}
	if err := m.reserveBytes(ctx, 91); err == nil {
		t.Fatalf("monitor accepted excessive allocation: %v", err)
	}
	if err := m.reserveBytes(ctx, 90); err != nil {
		t.Fatalf("monitor refused top allocation: %v", err)
	}
	if m.mu.curAllocated != 100 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.mu.curAllocated, 100)
	}

	m.releaseBytes(ctx, 90) // Should succeed without panic.
	if m.mu.curAllocated != 10 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.mu.curAllocated, 10)
	}
	if m.mu.maxAllocated != 100 {
		t.Fatalf("incorrect max allocation: got %d, expected %d", m.mu.maxAllocated, 100)
	}
	if m.MaximumBytes() != 100 {
		t.Fatalf("incorrect MaximumBytes(): got %d, expected %d", m.mu.maxAllocated, 100)
	}

	m.releaseBytes(ctx, 10) // Should succeed without panic.
	if m.mu.curAllocated != 0 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.mu.curAllocated, 0)
	}

	limitedMonitor := NewMonitorWithLimit(
		"testlimit", MemoryResource, 10, nil, nil, 1, 1000, cluster.MakeTestingClusterSettings())
	limitedMonitor.StartNoReserved(ctx, m)

	if err := limitedMonitor.reserveBytes(ctx, 10); err != nil {
		t.Fatalf("limited monitor refused small allocation: %v", err)
	}
	if err := limitedMonitor.reserveBytes(ctx, 1); err == nil {
		t.Fatal("limited monitor allowed allocation over limit")
	}
	limitedMonitor.releaseBytes(ctx, 10)

	limitedMonitor.Stop(ctx)
	m.Stop(ctx)
}

func TestMemoryAllocationEdgeCases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := NewMonitor("test", MemoryResource,
		nil /* curCount */, nil /* maxHist */, 1e9 /* increment */, 1e9 /* noteworthy */, st)
	m.Start(ctx, nil, NewStandaloneBudget(1e9))

	a := m.MakeBoundAccount()
	if err := a.Grow(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Grow(ctx, math.MaxInt64); err == nil {
		t.Fatalf("expected error, but found success")
	}

	a.Close(ctx)
	m.Stop(ctx)
}

func TestMultiSharedGauge(t *testing.T) {
	ctx := context.Background()
	resourceGauge := metric.NewGauge(metric.Metadata{})
	minAllocation := int64(1000)

	parent := NewMonitor("root", MemoryResource, resourceGauge, nil, minAllocation, 0,
		cluster.MakeTestingClusterSettings())
	parent.Start(ctx, nil, NewStandaloneBudget(100000))

	child := NewMonitorInheritWithLimit("child", 20000, parent)
	child.StartNoReserved(ctx, parent)

	acc := child.MakeBoundAccount()
	require.NoError(t, acc.Grow(ctx, 100))

	require.Equal(t, minAllocation, resourceGauge.Value(), "Metric")
}

func TestReservedAccountCleared(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	root := NewMonitor(
		"root" /* name */, MemoryResource, nil /* curCount */, nil, /* maxHist */
		1 /* increment */, 1000 /* noteworthy */, st,
	)
	root.Start(ctx, nil /* pool */, NewStandaloneBudget(math.MaxInt64))
	root.RelinquishAllOnReleaseBytes()

	// Pre-reserve a budget of 100 bytes.
	reserved := root.MakeBoundAccount()
	require.NoError(t, reserved.Grow(ctx, 100))

	m := NewMonitor(
		"test" /* name */, MemoryResource, nil /* curCount */, nil, /* maxHist */
		1 /* increment */, 1000 /* noteworthy */, st,
	)
	m.Start(ctx, nil /* pool */, &reserved)
	acc := m.MakeBoundAccount()

	// Grow the account by 50 bytes, then close the account and stop the
	// monitor.
	require.NoError(t, acc.Grow(ctx, 50))
	acc.Close(ctx)
	m.Stop(ctx)

	// Stopping the monitor should have clear the reserved account and returned
	// all pre-reserved memory back to the root monitor.
	require.Equal(t, int64(0), reserved.used)
	require.Equal(t, int64(0), root.mu.curBudget.used)
}

func getMonitor(
	ctx context.Context, st *cluster.Settings, name string, parent *BytesMonitor,
) *BytesMonitor {
	m := NewMonitor(redact.RedactableString(name), MemoryResource, nil, nil, 1, math.MaxInt64, st)
	var reserved *BoundAccount
	if parent == nil {
		reserved = NewStandaloneBudget(math.MaxInt64)
	} else {
		reserved = &noReserved
	}
	m.Start(ctx, parent, reserved)
	return m
}

// TestBytesMonitorTree is a sanity check that the tree structure of related
// monitors is maintained and traversed as expected.
func TestBytesMonitorTree(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	export := func(m *BytesMonitor) string {
		var monitors []MonitorState
		_ = m.TraverseTree(func(monitor MonitorState) error {
			monitors = append(monitors, monitor)
			return nil
		})
		var sb strings.Builder
		for _, e := range monitors {
			for i := 0; i < e.Level; i++ {
				sb.WriteString("-")
			}
			sb.WriteString(e.Name + "\n")
		}
		return sb.String()
	}

	parent := getMonitor(ctx, st, "parent", nil /* parent */)
	child1 := getMonitor(ctx, st, "child1", parent)
	child2 := getMonitor(ctx, st, "child2", parent)
	require.Equal(t, "parent\n-child2\n-child1\n", export(parent))
	require.Equal(t, "child1\n", export(child1))
	require.Equal(t, "child2\n", export(child2))

	grandchild1 := getMonitor(ctx, st, "grandchild1", child1)
	grandchild2 := getMonitor(ctx, st, "grandchild2", child2)
	require.Equal(t, "parent\n-child2\n--grandchild2\n-child1\n--grandchild1\n", export(parent))
	require.Equal(t, "child1\n-grandchild1\n", export(child1))
	require.Equal(t, "child2\n-grandchild2\n", export(child2))

	grandchild2.Stop(ctx)
	child2.Stop(ctx)

	require.Equal(t, "parent\n-child1\n--grandchild1\n", export(parent))
	require.Equal(t, "child1\n-grandchild1\n", export(child1))

	grandchild1.Stop(ctx)
	child1.Stop(ctx)

	require.Equal(t, "parent\n", export(parent))
	parent.Stop(ctx)
}

// TestBytesMonitorNoDeadlocks ensures that no deadlocks can occur when monitors
// are started and stopped concurrently with the monitor tree traversal.
func TestBytesMonitorNoDeadlocks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	root := getMonitor(ctx, st, "root", nil /* parent */)
	defer root.Stop(ctx)

	// Spin up 10 goroutines that repeatedly start and stop child monitors while
	// also making reservations against them.
	var wg sync.WaitGroup
	const numGoroutines = 10
	// done will be closed when the concurrent goroutines should exit.
	done := make(chan struct{})
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng, _ := randutil.NewTestRand()
			for {
				select {
				case <-done:
					return
				default:
					func() {
						m := getMonitor(ctx, st, fmt.Sprintf("m%d", i), root)
						defer m.Stop(ctx)
						numOps := rng.Intn(10 + 1)
						var reserved int64
						defer func() {
							m.releaseBytes(ctx, reserved)
						}()
						for op := 0; op < numOps; op++ {
							if reserved > 0 && rng.Float64() < 0.5 {
								toRelease := int64(rng.Intn(int(reserved))) + 1
								m.releaseBytes(ctx, toRelease)
								reserved -= toRelease
							} else {
								toReserve := int64(rng.Intn(1000) + 1)
								// We shouldn't hit any errors since we have an
								// unlimited root budget.
								_ = m.reserveBytes(ctx, toReserve)
								reserved += toReserve
							}
							// Sleep up to 1ms in-between operations.
							time.Sleep(time.Duration(rng.Intn(1000)) * time.Microsecond)
						}
					}()
					// Sleep up to 2ms after having stopped the monitor.
					time.Sleep(time.Duration(rng.Intn(2000)) * time.Microsecond)
				}
			}
		}(i)
	}

	// In the main goroutine, perform the tree traversal several times with
	// sleeps in-between.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		// We mainly want to ensure that no deadlocks nor data races are
		// occurring, but also we do a sanity check that each "row" in the
		// output of TraverseTree() is a non-empty MonitorState.
		var monitors []MonitorState
		_ = root.TraverseTree(func(monitor MonitorState) error {
			monitors = append(monitors, monitor)
			return nil
		})
		for _, m := range monitors {
			require.NotEqual(t, MonitorState{}, m)
		}
		// Sleep up to 3ms.
		time.Sleep(time.Duration(rng.Intn(3000)) * time.Microsecond)
	}
	close(done)
	wg.Wait()
}

func BenchmarkBoundAccountGrow(b *testing.B) {
	ctx := context.Background()
	m := NewMonitor("test", MemoryResource,
		nil /* curCount */, nil /* maxHist */, 1e9 /* increment */, 1e9, /* noteworthy */
		cluster.MakeTestingClusterSettings())
	m.Start(ctx, nil, NewStandaloneBudget(1e9))

	a := m.MakeBoundAccount()
	for i := 0; i < b.N; i++ {
		_ = a.Grow(ctx, 1)
	}
}

func BenchmarkTraverseTree(b *testing.B) {
	makeMonitorTree := func(numLevels int, numChildrenPerMonitor int) (root *BytesMonitor, cleanup func()) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		allMonitors := make([][]*BytesMonitor, numLevels)
		allMonitors[0] = []*BytesMonitor{getMonitor(ctx, st, "root", nil /* parent */)}
		for level := 1; level < numLevels; level++ {
			allMonitors[level] = make([]*BytesMonitor, 0, len(allMonitors[level-1])*numChildrenPerMonitor)
			for parent, parentMon := range allMonitors[level-1] {
				for child := 0; child < numChildrenPerMonitor; child++ {
					name := fmt.Sprintf("child%d_parent%d", child, parent)
					allMonitors[level] = append(allMonitors[level], getMonitor(ctx, st, name, parentMon))
				}
			}
		}
		cleanup = func() {
			// Simulate the production setting where we stop the children before
			// their parent (this is not strictly necessary since we don't
			// reserve budget from the monitors below).
			for i := len(allMonitors) - 1; i >= 0; i-- {
				for _, m := range allMonitors[i] {
					m.Stop(ctx)
				}
			}
		}
		return allMonitors[0][0], cleanup
	}
	for _, numLevels := range []int{2, 4, 8} {
		for _, numChildrenPerMonitor := range []int{2, 4, 8} {
			b.Run(fmt.Sprintf("levels=%d/children=%d", numLevels, numChildrenPerMonitor), func(b *testing.B) {
				root, cleanup := makeMonitorTree(numLevels, numChildrenPerMonitor)
				defer cleanup()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var numMonitors int
					_ = root.TraverseTree(func(MonitorState) error {
						numMonitors++
						return nil
					})
				}
			})
		}
	}
}

func TestLimit(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	m := NewMonitor("test", MemoryResource, nil, nil, 0, 1000, st)

	m.StartNoReserved(ctx, nil /* pool */)
	require.Equal(t, int64(0), m.Limit())
	m.Stop(ctx)

	m.Start(ctx, nil, NewStandaloneBudget(1000))
	require.Equal(t, int64(1000), m.Limit())
	m.Stop(ctx)

	m2 := NewMonitor("test", MemoryResource, nil, nil, 0, 1000, st)

	m2.StartNoReserved(ctx, m)
	require.Equal(t, int64(1000), m2.Limit())
	m2.Stop(ctx)

	m2.Start(ctx, m, NewStandaloneBudget(123))
	require.Equal(t, int64(1123), m2.Limit())
	m2.Stop(ctx)
}
