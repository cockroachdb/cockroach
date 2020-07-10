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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	rnd, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %v", seed)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	var pool *BytesMonitor
	var paramHeader func()

	m := NewMonitor("test", MemoryResource, nil, nil, 0, 1000, st)
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
			sum += accs[accI].allocated()
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
		avail := m.mu.curBudget.allocated() + m.reserved.used
		if sum > avail {
			t.Errorf("total account sum %d greater than total monitor budget %d", sum, avail)
			fail = true
		}
		if pool.mu.curAllocated > pool.reserved.used {
			t.Errorf("pool cur %d exceeds max %d", pool.mu.curAllocated, pool.reserved.used)
			fail = true
		}
		if m.mu.curBudget.allocated() != pool.mu.curAllocated {
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
		pool.Start(ctx, nil, MakeStandaloneBudget(max))

		for _, hf := range hysteresisFactors {
			maxAllocatedButUnusedBlocks = hf

			for _, pb := range preBudgets {
				mmax := pb + max

				for _, pa := range poolAllocSizes {
					paramHeader = func() { fmt.Printf("max %d, pb %d, as %d, hf %d\n", max, pb, pa, hf) }

					// We start with a fresh monitor for every set of
					// parameters.
					m = NewMonitor("test", MemoryResource, nil, nil, pa, 1000, st)
					m.Start(ctx, pool, MakeStandaloneBudget(pb))

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
	m.Start(ctx, nil, MakeStandaloneBudget(100))
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

func TestBytesMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	m := NewMonitor("test", MemoryResource, nil, nil, 1, 1000, st)
	m.Start(ctx, nil, MakeStandaloneBudget(100))
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
	limitedMonitor.Start(ctx, m, BoundAccount{})

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
	m.Start(ctx, nil, MakeStandaloneBudget(1e9))

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

func BenchmarkBoundAccountGrow(b *testing.B) {
	ctx := context.Background()
	m := NewMonitor("test", MemoryResource,
		nil /* curCount */, nil /* maxHist */, 1e9 /* increment */, 1e9, /* noteworthy */
		cluster.MakeTestingClusterSettings())
	m.Start(ctx, nil, MakeStandaloneBudget(1e9))

	a := m.MakeBoundAccount()
	for i := 0; i < b.N; i++ {
		_ = a.Grow(ctx, 1)
	}
}
