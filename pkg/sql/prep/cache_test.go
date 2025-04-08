// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package prep

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const (
	// The number of random test runs.
	numRuns = 100
	// The number of cache operations to perform per test run.
	numOps = 5000
	// The number of distinct names to use.
	numNames = 50
	// The upperbound for choosing a random statement size.
	stmtSizeUpperbound = 20
	// The upperbound for choosing a random max size of the cache during
	// commits.
	maxSizeUpperbound = 100
)

func TestCache(t *testing.T) {
	ctx := context.Background()

	t.Run("empty", func(t *testing.T) {
		var c Cache
		c.Init(ctx)
		assertEmpty(t, &c)
		if c.Dirty() {
			t.Errorf("expected c to not be dirty")
		}
		_ = c.Commit(ctx, 10)
		assertEmpty(t, &c)
		if c.Dirty() {
			t.Errorf("expected c to not be dirty")
		}
		_ = c.Commit(ctx, 10)
		c.Rewind(ctx)
		if c.Dirty() {
			t.Errorf("expected c to not be dirty")
		}
	})

	t.Run("add-remove", func(t *testing.T) {
		var c Cache
		c.Init(ctx)
		c.Add("s1", stmt(), 1)
		c.Remove("s1")
		if c.Len() != 0 {
			t.Errorf("expected cache to be empty after removing s1")
		}
		if c.Size() != 0 {
			t.Errorf("expected empty cache to have size 0")
		}
	})

	t.Run("init", func(t *testing.T) {
		var c Cache
		c.Init(ctx)
		c.Add("s1", stmt(), 1)
		_ = c.Commit(ctx, 0)
		c.Init(ctx)
		if c.Len() != 0 {
			t.Errorf("expected cache to be empty after reinitializing")
		}
		if c.Size() != 0 {
			t.Errorf("expected empty cache to have size 0")
		}
	})

	t.Run("evict-none", func(t *testing.T) {
		var c Cache
		c.Init(ctx)
		c.Add("s1", stmt(), 1)
		c.Add("s2", stmt(), 1)
		evicted := c.Commit(ctx, 2)
		if len(evicted) > 0 {
			t.Errorf("expected no evictions")
		}
		c.Add("s3", stmt(), 10)
		assertContains(t, &c, "s1", "s2", "s3")
		c.Rewind(ctx)
		evicted = c.Commit(ctx, 3)
		if len(evicted) > 0 {
			t.Errorf("expected no evictions, got %v", evicted)
		}
		evicted = c.Commit(ctx, 2)
		if len(evicted) > 0 {
			t.Errorf("expected no evictions, got %v", evicted)
		}
		assertContains(t, &c, "s1", "s2")
	})

	t.Run("evict", func(t *testing.T) {
		var c Cache
		c.Init(ctx)
		c.Add("s1", stmt(), 2)
		c.Add("s2", stmt(), 2)
		c.Add("s3", stmt(), 2)
		c.Add("s4", stmt(), 2)
		_, _ = c.Get("s1")
		evicted := c.Commit(ctx, 6)
		if !slices.Equal(evicted, []string{"s2"}) {
			t.Errorf("expected s2 to be evicted, got: %v", evicted)
		}
		// Clear the cache.
		c.Init(ctx)
		c.Add("s5", stmt(), 2)
		c.Add("s6", stmt(), 2)
		c.Add("s7", stmt(), 2)
		c.Add("s8", stmt(), 2)
		evicted = c.Commit(ctx, 2)
		if !slices.Equal(evicted, []string{"s5", "s6", "s7"}) {
			t.Errorf("expected s5, s6, and s7 to be evicted, got %v", evicted)
		}
		assertContains(t, &c, "s8")
	})

	t.Run("random", func(t *testing.T) {
		for i := 0; i < numRuns; i++ {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				t.Parallel() // SAFE FOR TESTING (this comment is for the linter)
				rng, _ := randutil.NewTestRand()
				var c Cache
				var o oracle
				c.Init(ctx)
				o.Init()
				for j := 0; j < numOps; j++ {
					switch n := rng.Intn(10); n {
					case 0:
						maxSize := rng.Int63n(maxSizeUpperbound)
						cEvicted := c.Commit(ctx, maxSize)
						oEvicted := o.Commit(maxSize)
						if !slices.Equal(cEvicted, oEvicted) {
							t.Errorf("expected evicted statements %v, got %v",
								oEvicted, cEvicted)
						}
					case 1:
						c.Rewind(ctx)
						o.Rewind()
					default:
						name := fmt.Sprintf("stmt_%d", rng.Intn(numNames))
						switch n := rng.Intn(10); n {
						case 0:
							if !o.Has(name) {
								s := stmt()
								size := rng.Int63n(stmtSizeUpperbound)
								c.Add(name, s, size)
								o.Add(name, s, size)
							}
						case 1:
							c.Remove(name)
							o.Remove(name)
						default:
							c.Get(name)
							o.Get(name)
						}
					}
					validate(t, &o, &c)
				}
			})
		}
	})
}

func stmt() *Statement { return &Statement{refCount: 1} }

func assertEmpty(t *testing.T, c *Cache) {
	assertContains(t, c)
}

func assertContains(t *testing.T, c *Cache, names ...string) {
	for _, name := range names {
		if !c.Has(name) {
			t.Errorf("expected cache to have %s", name)
		}
	}
	c.ForEach(func(name string, _ *Statement) {
		found := false
		for _, n := range names {
			if n == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("unexpected statement %s in cache", name)
		}
	})
}

func validate(t *testing.T, o *oracle, c *Cache) {
	if o.Len() != c.Len() {
		t.Errorf("expected cache to have %d items, got %d", o.Len(), c.Len())
	}
	if o.Size() != c.Size() {
		t.Errorf("expected cache to have size %d, got %d", o.Size(), c.Size())
	}
	if c.Len() == 0 && c.Size() != 0 {
		t.Errorf("expected empty cache to have size 0, got %d", c.Size())
	}
	// Check that every statement in the oracle is in the cache.
	o.ForEach(func(name string, stmt *Statement) {
		if !c.Has(name) {
			t.Errorf("expected c.Has to be true for %q", name)
		}
		// Access the map directly instead of using Get so it is not counted as
		// an access.
		ce, ok := c.m[name]
		if !ok {
			t.Errorf("expected c.Get to return ok=true for %q", name)
		}
		if stmt != ce.stmt {
			t.Errorf("incorrect statement mapped to %q", name)
		}
	})
	// Check that every statement in the cache is in the oracle.
	c.ForEach(func(name string, stmt *Statement) {
		if !o.Has(name) {
			t.Errorf("unexpected statement for %q in c", name)
		}
		// Access the map directly instead of using Get so it is not counted as
		// an access.
		oe, ok := o.m[name]
		if !ok {
			t.Errorf("unexpected entry for %q in c", name)
		}
		if stmt != oe.stmt {
			t.Errorf("incorrect statement mapped to %q", name)
		}
	})
	if o.Dirty() != c.Dirty() {
		t.Errorf("expected Dirty to return %v, got %v", o.Dirty(), c.Dirty())
	}
}

// oracle implements the same API as Cache in a simpler, less efficient way. It
// is used for randomized tests for Cache.
type oracle struct {
	oMap
	snapshot oMap
}

type oMap struct {
	m     map[string]oEntry
	size  int64
	clock int
}

type oEntry struct {
	name string
	stmt *Statement
	size int64
	t    int
}

func (o *oracle) Init() {
	o.m = make(map[string]oEntry)
	o.snapshot.m = make(map[string]oEntry)
}

func (o *oracle) Add(name string, stmt *Statement, size int64) {
	if _, ok := o.m[name]; ok {
		panic(errors.AssertionFailedf("cache entry %q already exists", name))
	}
	o.m[name] = oEntry{name: name, stmt: stmt, size: size, t: o.clock}
	o.size += size
	o.clock++
}

func (o *oracle) Remove(name string) {
	e, ok := o.m[name]
	if !ok {
		return
	}
	delete(o.m, name)
	o.size -= e.size
	o.clock++
}

func (o *oracle) Has(name string) bool {
	_, ok := o.m[name]
	return ok
}

func (o *oracle) Get(name string) (stmt *Statement, ok bool) {
	e, ok := o.m[name]
	if !ok {
		return nil, false
	}
	e.t = o.clock
	o.m[name] = e
	o.clock++
	return e.stmt, ok
}

func (o *oracle) Len() int {
	return len(o.m)
}

func (o *oracle) Size() int64 {
	return o.size
}

func (o *oracle) Commit(maxSize int64) (evicted []string) {
	// Evict committed entries, if necessary.
	entries := o.orderedEntries()
	for i := 0; i < len(entries)-1 && maxSize > 0 && o.size > maxSize; i++ {
		delete(o.m, entries[i].name)
		o.size -= entries[i].size
		evicted = append(evicted, entries[i].name)
	}
	// Make a snapshot of the map.
	maps.Clear(o.snapshot.m)
	maps.Copy(o.snapshot.m, o.m)
	o.snapshot.size = o.size
	o.snapshot.clock = o.clock
	return evicted
}

func (o *oracle) orderedEntries() []oEntry {
	var res []oEntry
	for _, e := range o.m {
		res = append(res, e)
	}
	slices.SortFunc(res, func(a, b oEntry) int {
		return a.t - b.t
	})
	return res
}

func (o *oracle) Rewind() {
	maps.Clear(o.m)
	maps.Copy(o.m, o.snapshot.m)
	o.size = o.snapshot.size
	o.clock = o.snapshot.clock
}

func (o *oracle) Dirty() bool {
	return o.clock != o.snapshot.clock
}

func (o *oracle) ForEach(fn func(name string, stmt *Statement)) {
	for name, e := range o.m {
		fn(name, e.stmt)
	}
}
