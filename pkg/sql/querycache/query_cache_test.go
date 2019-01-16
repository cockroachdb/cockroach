// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package querycache

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
	cd := &CachedData{SQL: sql, Memo: mem, PrepareMetadata: &sqlbase.PrepareMetadata{}}
	n := memEstimate - cd.memoryEstimate()
	if n < 0 {
		panic(fmt.Sprintf("size %d too small", memEstimate))
	}
	// Add characters to AnonymizedStr which should increase the estimate.
	s := make([]byte, n)
	for i := range s {
		s[i] = 'x'
	}
	cd.PrepareMetadata.AnonymizedStr = string(s)
	if cd.memoryEstimate() != memEstimate {
		panic(fmt.Sprintf("failed to create CachedData of size %d", memEstimate))
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

	expect(t, c, "")
	c.Add(data("a", sa, avgCachedSize))
	expect(t, c, "a")
	c.Add(data("b", sb, avgCachedSize))
	expect(t, c, "b,a")
	c.Add(data("c", sc, avgCachedSize))
	expect(t, c, "c,b,a")
	c.Add(data("d", sd, avgCachedSize))
	expect(t, c, "d,c,b")
	if _, ok := c.Find("a"); ok {
		t.Errorf("a shouldn't be in the cache")
	}
	if res, ok := c.Find("c"); !ok {
		t.Errorf("c should be in the cache")
	} else if res.Memo != sc {
		t.Errorf("invalid Memo for c")
	}
	expect(t, c, "c,d,b")

	if res, ok := c.Find("b"); !ok {
		t.Errorf("b should be in the cache")
	} else if res.Memo != sb {
		t.Errorf("invalid Memo for b")
	}
	expect(t, c, "b,c,d")

	c.Add(data("a", sa, avgCachedSize))
	expect(t, c, "a,b,c")

	c.Purge("b")
	expect(t, c, "a,c")
	if _, ok := c.Find("b"); ok {
		t.Errorf("b shouldn't be in the cache")
	}

	c.Purge("c")
	expect(t, c, "a")

	c.Add(data("b", sb, avgCachedSize))
	expect(t, c, "b,a")

	c.Clear()
	expect(t, c, "")
	if _, ok := c.Find("b"); ok {
		t.Errorf("b shouldn't be in the cache")
	}
}

func TestCacheMemory(t *testing.T) {
	m := &memo.Memo{}

	c := New(10 * avgCachedSize)
	expect(t, c, "")
	for i := 0; i < 10; i++ {
		c.Add(data(fmt.Sprintf("%d", i), m, avgCachedSize/2))
	}
	expect(t, c, "9,8,7,6,5,4,3,2,1,0")

	// Verify handling when we have no more entries.
	c.Add(data("10", m, avgCachedSize/2))
	expect(t, c, "10,9,8,7,6,5,4,3,2,1")

	// Verify handling when we have larger entries.
	c.Add(data("large", m, avgCachedSize*8))
	expect(t, c, "large,10,9,8,7")
	c.Add(data("verylarge", m, avgCachedSize*10))
	expect(t, c, "verylarge")

	for i := 0; i < 10; i++ {
		c.Add(data(fmt.Sprintf("%d", i), m, avgCachedSize))
	}
	expect(t, c, "9,8,7,6,5,4,3,2,1,0")

	// Verify that we don't try to add an entry that's larger than the cache size.
	c.Add(data("large", m, avgCachedSize*11))
	expect(t, c, "9,8,7,6,5,4,3,2,1,0")

	// Verify handling when we update an existing entry with one that uses more
	// memory.
	c.Add(data("5", m, avgCachedSize*5))
	expect(t, c, "5,9,8,7,6,4")

	c.Add(data("0", m, avgCachedSize))
	expect(t, c, "0,5,9,8,7,6")

	// Verify handling when we update an existing entry with one that uses less
	// memory.
	c.Add(data("5", m, avgCachedSize))
	expect(t, c, "5,0,9,8,7,6")
	c.Add(data("1", m, avgCachedSize))
	c.Add(data("2", m, avgCachedSize))
	c.Add(data("3", m, avgCachedSize))
	c.Add(data("4", m, avgCachedSize))
	expect(t, c, "4,3,2,1,5,0,9,8,7,6")

	// Verify Purge updates the available memory.
	c.Purge("3")
	expect(t, c, "4,2,1,5,0,9,8,7,6")
	c.Add(data("x", m, avgCachedSize))
	expect(t, c, "x,4,2,1,5,0,9,8,7,6")
	c.Add(data("y", m, avgCachedSize))
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
					c.Add(data(sql, &memo.Memo{}, int64(256+rng.Intn(10*avgCachedSize))))
				default:
					// The rest of the time, find an entry.
					_, _ = c.Find(sql)
				}
				c.check()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// BenchmarkQueryCache is a worst case benchmark where every session misses the
// cache. The result of the benchmark is the time to do a pair of Find, Add
// operations.
func BenchmarkQueryCache(b *testing.B) {
	// Run a worst case benchmark where every worker misses the cache.
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
					cd := CachedData{Memo: &memo.Memo{}}
					for j := 0; j < numOpsPerWorker; j++ {
						str := fmt.Sprintf("%d/%d", workerID, j)
						if _, ok := c.Find(str); ok {
							panic("found")
						}
						cd.SQL = str
						c.Add(&cd)
					}
					wg.Done()
				}()
			}
			wg.Wait()
		})
	}
}
