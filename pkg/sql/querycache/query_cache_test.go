// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package querycache

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

func toStr(c *C) string {
	c.check()

	c.mu.Lock()
	defer c.mu.Unlock()

	var b strings.Builder
	for e := c.mu.used.next; e != &c.mu.used; e = e.next {
		if b.Len() != 0 {
			b.WriteString(",")
		}
		b.WriteString(e.SQL)
	}
	return b.String()
}

func expect(t *testing.T, c *C, exp string) {
	t.Helper()
	if s := toStr(c); s != exp {
		t.Errorf("expected %s, got %s", exp, s)
	}
}

func data(sql string, mem *memo.Memo, memEstimate int64) *CachedData {
	cd := &CachedData{SQL: sql, Memo: mem, PrepareMetadata: &PrepareMetadata{}}
	n := memEstimate - cd.memoryEstimate()
	if n < 0 {
		panic(errors.AssertionFailedf("size %d too small", memEstimate))
	}
	// Add characters to AnonymizedStr which should increase the estimate.
	s := make([]byte, n)
	for i := range s {
		s[i] = 'x'
	}
	cd.PrepareMetadata.AnonymizedStr = string(s)
	if cd.memoryEstimate() != memEstimate {
		panic(errors.AssertionFailedf("failed to create CachedData of size %d", memEstimate))
	}
	return cd
}

// TestCache tests the main operations of the cache.
func TestCache(t *testing.T) {
	sa := &memo.Memo{}
	sb := &memo.Memo{}
	sc := &memo.Memo{}
	sd := &memo.Memo{}

	// In this test, all entries have the same size: avgCachedSize.
	c := New(3 * avgCachedSize)

	var s Session
	s.Init()

	expect(t, c, "")
	c.Add(&s, data("a", sa, avgCachedSize))
	expect(t, c, "a")
	c.Add(&s, data("b", sb, avgCachedSize))
	expect(t, c, "b,a")
	c.Add(&s, data("c", sc, avgCachedSize))
	expect(t, c, "c,b,a")
	c.Add(&s, data("d", sd, avgCachedSize))
	expect(t, c, "d,c,b")
	if _, ok := c.Find(&s, "a"); ok {
		t.Errorf("a shouldn't be in the cache")
	}
	if res, ok := c.Find(&s, "c"); !ok {
		t.Errorf("c should be in the cache")
	} else if res.Memo != sc {
		t.Errorf("invalid Memo for c")
	}
	expect(t, c, "c,d,b")

	if res, ok := c.Find(&s, "b"); !ok {
		t.Errorf("b should be in the cache")
	} else if res.Memo != sb {
		t.Errorf("invalid Memo for b")
	}
	expect(t, c, "b,c,d")

	c.Add(&s, data("a", sa, avgCachedSize))
	expect(t, c, "a,b,c")

	c.Purge("b")
	expect(t, c, "a,c")
	if _, ok := c.Find(&s, "b"); ok {
		t.Errorf("b shouldn't be in the cache")
	}

	c.Purge("c")
	expect(t, c, "a")

	c.Add(&s, data("b", sb, avgCachedSize))
	expect(t, c, "b,a")

	c.Clear()
	expect(t, c, "")
	if _, ok := c.Find(&s, "b"); ok {
		t.Errorf("b shouldn't be in the cache")
	}
}

func TestCacheMemory(t *testing.T) {
	m := &memo.Memo{}

	c := New(10 * avgCachedSize)
	var s Session
	s.Init()
	expect(t, c, "")
	for i := 0; i < 10; i++ {
		c.Add(&s, data(fmt.Sprintf("%d", i), m, avgCachedSize/2))
	}
	expect(t, c, "9,8,7,6,5,4,3,2,1,0")

	// Verify handling when we have no more entries.
	c.Add(&s, data("10", m, avgCachedSize/2))
	expect(t, c, "10,9,8,7,6,5,4,3,2,1")

	// Verify handling when we have larger entries.
	c.Add(&s, data("large", m, avgCachedSize*8))
	expect(t, c, "large,10,9,8,7")
	c.Add(&s, data("verylarge", m, avgCachedSize*10))
	expect(t, c, "verylarge")

	for i := 0; i < 10; i++ {
		c.Add(&s, data(fmt.Sprintf("%d", i), m, avgCachedSize))
	}
	expect(t, c, "9,8,7,6,5,4,3,2,1,0")

	// Verify that we don't try to add an entry that's larger than the cache size.
	c.Add(&s, data("large", m, avgCachedSize*11))
	expect(t, c, "9,8,7,6,5,4,3,2,1,0")

	// Verify handling when we update an existing entry with one that uses more
	// memory.
	c.Add(&s, data("5", m, avgCachedSize*5))
	expect(t, c, "5,9,8,7,6,4")

	c.Add(&s, data("0", m, avgCachedSize))
	expect(t, c, "0,5,9,8,7,6")

	// Verify handling when we update an existing entry with one that uses less
	// memory.
	c.Add(&s, data("5", m, avgCachedSize))
	expect(t, c, "5,0,9,8,7,6")
	c.Add(&s, data("1", m, avgCachedSize))
	c.Add(&s, data("2", m, avgCachedSize))
	c.Add(&s, data("3", m, avgCachedSize))
	c.Add(&s, data("4", m, avgCachedSize))
	expect(t, c, "4,3,2,1,5,0,9,8,7,6")

	// Verify Purge updates the available memory.
	c.Purge("3")
	expect(t, c, "4,2,1,5,0,9,8,7,6")
	c.Add(&s, data("x", m, avgCachedSize))
	expect(t, c, "x,4,2,1,5,0,9,8,7,6")
	c.Add(&s, data("y", m, avgCachedSize))
	expect(t, c, "y,x,4,2,1,5,0,9,8,7")
}

// TestSynchronization verifies that the cache doesn't crash (or cause a race
// detector error) when multiple goroutines are using it in parallel.
func TestSynchronization(t *testing.T) {
	const size = 100
	c := New(size * avgCachedSize)

	var wg sync.WaitGroup
	const goroutines = 20
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			rng, _ := randutil.NewPseudoRand()
			var s Session
			s.Init()
			for j := 0; j < 5000; j++ {
				sql := fmt.Sprintf("%d", rng.Intn(2*size))
				switch r := rng.Intn(100); {
				case r == 0:
					// 1% of the time, clear the entire cache.
					c.Clear()
				case r <= 10:
					// 10% of the time, purge an entry.
					c.Purge(sql)
				case r <= 35:
					// 25% of the time, add an entry.
					c.Add(&s, data(sql, &memo.Memo{}, int64(256+rng.Intn(10*avgCachedSize))))
				default:
					// The rest of the time, find an entry.
					_, _ = c.Find(&s, sql)
				}
				c.check()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestSession(t *testing.T) {
	var s Session
	s.Init()

	// Find the number of misses that are necessary to reach the high miss ratio.
	n := 1
	for {
		n++
		s.registerMiss()
		if s.highMissRatio() {
			break
		}
	}
	if n < 1000 {
		t.Errorf("high miss ratio threshold reached too quickly (n=%d)", n)
	} else if n > 10000 {
		t.Errorf("high miss ratio threshold reached too slowly (n=%d)", n)
	}

	// Verify we can get the average close to 0.5.
	for i := 0; i < 2000; i++ {
		s.registerHit()
		s.registerMiss()
	}
	v := float64(s.missRatioMMA) / mmaScale
	if v < 0.5 || v > 0.6 {
		t.Errorf("invalid miss ratio %f, expected close to 0.5", v)
	}
	// Verify we can get the average close to 0.
	for i := 0; i < 2000; i++ {
		s.registerHit()
	}
	v = float64(s.missRatioMMA) / mmaScale
	if v > 0.1 {
		t.Errorf("invalid miss ratio %f, expected close to 0", v)
	}
}

// BenchmarkWorstCase is a worst case benchmark where every session
// misses the cache. The result of the benchmark is the time to do a pair of
// Find, Add operations.
//
// For server-level benchmarks, see BenchmarkQueryCache in pkg/sql.
func BenchmarkWorstCase(b *testing.B) {
	for _, mitigation := range []bool{false, true} {
		name := "NoMitigation"
		if mitigation {
			name = "WithMitigation"
		}
		b.Run(name, func(b *testing.B) {
			for _, numWorkers := range []int{1, 10, 100} {
				b.Run(fmt.Sprintf("%d", numWorkers), func(b *testing.B) {
					const cacheSize = 10
					c := New(cacheSize * maxCachedSize)
					numOpsPerWorker := (b.N + numWorkers - 1) / numWorkers
					var wg sync.WaitGroup
					wg.Add(numWorkers)
					for i := 0; i < numWorkers; i++ {
						workerID := i
						go func() {
							var s Session
							s.Init()
							cd := CachedData{Memo: &memo.Memo{}}
							for j := 0; j < numOpsPerWorker; j++ {
								str := fmt.Sprintf("%d/%d", workerID, j)
								if _, ok := c.Find(&s, str); ok {
									panic("found")
								}
								cd.SQL = str
								if !mitigation {
									// Disable the mitigation mechanism.
									s.missRatioMMA = 0
								}
								c.Add(&s, &cd)
							}
							wg.Done()
						}()
					}
					wg.Wait()
				})
			}
		})
	}
}
