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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func toStr(c *C) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var b strings.Builder
	for e := c.mu.sentinel.next; e != &c.mu.sentinel; e = e.next {
		if e.SQL == "" {
			continue
		}
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

// TestCache tests the main operations of the cache.
func TestCache(t *testing.T) {
	sa := &memo.Memo{}
	sb := &memo.Memo{}
	sc := &memo.Memo{}
	sd := &memo.Memo{}

	c := New(3 * maxCachedSize)

	expect(t, c, "")
	c.Add(&CachedData{SQL: "a", Memo: sa})
	expect(t, c, "a")
	c.Add(&CachedData{SQL: "b", Memo: sb})
	expect(t, c, "b,a")
	c.Add(&CachedData{SQL: "c", Memo: sc})
	expect(t, c, "c,b,a")
	c.Add(&CachedData{SQL: "d", Memo: sd})
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

	c.Add(&CachedData{SQL: "a", Memo: sa})
	expect(t, c, "a,b,c")

	c.Purge("b")
	expect(t, c, "a,c")
	if _, ok := c.Find("b"); ok {
		t.Errorf("b shouldn't be in the cache")
	}

	c.Purge("c")
	expect(t, c, "a")

	c.Add(&CachedData{SQL: "b", Memo: sb})
	expect(t, c, "b,a")

	c.Clear()
	expect(t, c, "")
	if _, ok := c.Find("b"); ok {
		t.Errorf("b shouldn't be in the cache")
	}
}

// TestSynchronization verifies that the cache doesn't crash (or cause a race
// detector error) when multiple goroutines are using it in parallel.
func TestSynchronization(t *testing.T) {
	const size = 100
	c := New(size * maxCachedSize)

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
					c.Add(&CachedData{SQL: sql, Memo: &memo.Memo{}})
				default:
					// The rest of the time, find an entry.
					_, _ = c.Find(sql)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
