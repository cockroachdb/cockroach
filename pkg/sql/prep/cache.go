// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package prep

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Cache maps prepared statement names to Statements. In order to facilitate
// automatic retries which rewind the command buffer, Cache implements the
// Rewind method to restore the Cache to its state at the time of the last
// Commit.
//
// Cache applies an LRU eviction policy during Commit. See Commit for more
// details.
//
// Cache is optimized for the most common interactions in most workloads:
//
//  1. Fetching prepared statements with Get.
//  2. Snapshotting the Cache with Commit when no changes have been applied
//     since the last Commit.
//
// Cache is not thread-safe.
type Cache struct {
	m map[string]*entry
	// "uses" contains all committed statements ordered from most recently used
	// to least recently used.
	uses list
	// tape records calls to Add, Remove, and Get since the last Commit or
	// Rewind.
	tape []event
	size int64
}

type entry struct {
	name       string
	stmt       *Statement
	prev, next *entry
	size       int64
}

type op uint8

const (
	add op = iota
	remove
	get
)

// event represent an Add, Remove, or Get in the tape.
type event struct {
	e *entry
	op
}

// Init initializes an empty cache. If the cache contains any statements, they
// are cleared.
func (c *Cache) Init(ctx context.Context) {
	// Clear the tape and decrement added statement reference counts.
	for i := range c.tape {
		if c.tape[i].op == remove {
			// Remove deletes the statement from the map without decrementing
			// its reference count, so it must be decremented here.
			c.tape[i].e.stmt.DecRef(ctx)
		}
		c.tape[i] = event{}
	}
	// Clear the map and decrement the statement reference counts.
	for name, e := range c.m {
		e.stmt.DecRef(ctx)
		delete(c.m, name)
	}
	// Reuse the tape and the map.
	*c = Cache{
		m:    c.m,
		tape: c.tape[:0],
	}
	// Initialize the map and "uses" list.
	if c.m == nil {
		c.m = make(map[string]*entry)
	}
	c.uses.init()
}

// Add adds a statement to the cache with the given name.
//
// NOTE: Panics if a statement with the given name already exists.
func (c *Cache) Add(name string, stmt *Statement, size int64) {
	if _, ok := c.m[name]; ok {
		panic(errors.AssertionFailedf("cache entry %q already exists", name))
	}
	e := &entry{
		name: name,
		stmt: stmt,
		size: size,
	}
	c.m[name] = e
	c.size += size
	c.tape = append(c.tape, event{e, add})
}

// Remove removes the statement with the given name from the cache. No-op if the
// name does not already exist in the cache.
func (c *Cache) Remove(name string) {
	e, ok := c.m[name]
	if !ok {
		return
	}
	delete(c.m, name)
	c.tape = append(c.tape, event{e, remove})
	c.size -= e.size
}

// Has returns true if the prepared statement with the given name is in the
// cache. It is not considered an access and does not affect the ordering of
// statement eviction during Commit.
func (c *Cache) Has(name string) bool {
	_, ok := c.m[name]
	return ok
}

// Get looks up a prepared statement with the given name. It is considered an
// access so it affects the ordering of statement eviction during Commit.
func (c *Cache) Get(name string) (stmt *Statement, ok bool) {
	e, ok := c.m[name]
	if !ok {
		return nil, false
	}
	c.tape = append(c.tape, event{e, get})
	return e.stmt, true
}

// Len returns the number of prepared statements in the cache.
func (c *Cache) Len() int {
	return len(c.m)
}

// Size returns the total size of the statements in the cache.
func (c *Cache) Size() int64 {
	return c.size
}

// Commit makes permanent any changes to the cache via Add or Remove since the
// last Commit. Those changes can no longer be undone with Rewind.
//
// If maxSize is positive, Commit will evict prepared statements in the cache
// until the total size of statements is less than maxSize or until there is
// only one remaining statement in the cache, whichever comes first. The least
// recently accessed statements are evicted first, where Add and Get are
// considered accesses.
//
// The time complexity of Commit scales linearly with the number of changes made
// to the cache since the last Commit.
func (c *Cache) Commit(ctx context.Context, maxSize int64) (evicted []string) {
	// Apply the events in the tape to the "uses" list and clear the tape.
	for i := range c.tape {
		switch c.tape[i].op {
		case add:
			c.uses.push(c.tape[i].e)
		case remove:
			c.tape[i].e.stmt.DecRef(ctx)
			c.uses.remove(c.tape[i].e)
		case get:
			c.uses.remove(c.tape[i].e)
			c.uses.push(c.tape[i].e)
		default:
			panic(errors.AssertionFailedf("unexpected op %d", c.tape[i].op))
		}
		c.tape[i] = event{}
	}
	c.tape = c.tape[:0]
	// Evict statements, if necessary, until the size is less than maxSize or
	// there is only one statement in the cache.
	for e := c.uses.tail(); e != nil && e != c.uses.head() && maxSize > 0 && c.size > maxSize; {
		prev := c.uses.prev(e)
		delete(c.m, e.name)
		c.uses.remove(e)
		c.size -= e.size
		evicted = append(evicted, e.name)
		e.stmt.DecRef(ctx)
		e = prev
	}
	return evicted
}

// Rewind returns the cache to its state at the previous Commit time.
//
// Rewind is a constant-time operation if nothing has been added or removed
// since the last Commit or Rewind.
func (c *Cache) Rewind(ctx context.Context) {
	// Undo the events in the tape in reverse order and clear the tape.
	for i := len(c.tape) - 1; i >= 0; i-- {
		e := c.tape[i].e
		switch c.tape[i].op {
		case add:
			delete(c.m, e.name)
			c.size -= e.size
			e.stmt.DecRef(ctx)
		case remove:
			c.m[e.name] = e
			c.size += e.size
		case get:
			// No-op. The "uses" list does not need to be reverted because it is
			// only altered during Commit.
		}
		c.tape[i] = event{}
	}
	c.tape = c.tape[:0]
}

// Dirty returns true if Add, Remove, or Get has been called on the cache since
// the last Commit or Rewind. Remove and Get calls with non-existent prepared
// statement names do not make the cache dirty.
func (c *Cache) Dirty() bool {
	return len(c.tape) > 0
}

// ForEach calls fn on every statement in the cache. The order of the statements
// is non-deterministic.
func (c *Cache) ForEach(fn func(name string, stmt *Statement)) {
	for name, e := range c.m {
		fn(name, e.stmt)
	}
}

// ForEachLRU calls fn on every statement in the cache in order from least
// recently used to most recently used. The cache must not be dirty—the ordering
// of "uses" is not maintained while the Cache is dirty so an LRU ordering
// cannot be provided.
//
// NOTE: Panics if the cache is dirty. This is required because the Cache does
// not keep track of LRU orderings of mutations until they are committed.
func (c *Cache) ForEachLRU(fn func(name string, stmt *Statement)) {
	if c.Dirty() {
		panic(errors.AssertionFailedf("cannot iterate over dirty cache"))
	}
	for e := c.uses.tail(); e != nil; e = c.uses.prev(e) {
		fn(e.name, e.stmt)
	}
}

// list is a doubly-linked list of entries.
type list struct {
	// root is a sentinel value. root.next and root.prev are the head and the
	// tail.
	root entry
}

// init initializes a list.
func (l *list) init() {
	l.root.next = &l.root
	l.root.prev = &l.root
}

// head returns the first entry in the list or nil if the list is empty.
func (l *list) head() *entry {
	if l.root.next == &l.root {
		return nil
	}
	return l.root.next
}

// tail returns the last entry in the list or nil if the list is empty.
func (l *list) tail() *entry {
	if l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

// prev returns the entry in the list before e or nil if one does not exist.
func (l *list) prev(e *entry) *entry {
	if e.prev == &l.root {
		return nil
	}
	return e.prev
}

// push pushes the entry onto the front of the list. The entry must not already
// be in the list.
func (l *list) push(e *entry) {
	h := l.root.next
	e.next = h
	e.prev = h.prev
	h.prev = e
	l.root.next = e
}

// remove removes the entry from the list. The entry must already be in the
// list.
func (l *list) remove(e *entry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil
	e.prev = nil
}
