// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's internal/singleflight package.

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// call is an in-flight or completed singleflight.Do call
type call struct {
	wg sync.WaitGroup

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val interface{}
	err error

	// These fields are read and written with the singleflight
	// mutex held before the WaitGroup is done, and are read but
	// not written after the WaitGroup is done.
	dups  int
	chans []chan<- Result
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group struct {
	mu syncutil.Mutex   // protects m
	m  map[string]*call // lazily initialized
}

// Result holds the results of Do, so they can be passed
// on a channel.
type Result struct {
	Val    interface{}
	Err    error
	Shared bool
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
func (g *Group) Do(
	key string, fn func() (interface{}, error),
) (v interface{}, shared bool, err error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, true, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	g.doCall(c, key, fn)
	return c.val, c.dups > 0, c.err
}

// DoChan is like Do but returns a channel that will receive the results when
// they are ready. The method also returns a boolean specifying whether the
// caller's fn function will be called or not. This return value lets callers
// identify a unique "leader" for a flight.
//
// NOTE: DoChan makes it possible to initiate or join a flight while holding a
// lock without holding it for the duration of the flight. A common usage
// pattern is:
// 1. Check some datastructure to see if it contains the value you're looking
// for.
// 2. If it doesn't, initiate or join a flight to produce it.
//
// Step one is expected to be done while holding a lock. Modifying the
// datastructure in the callback is expected to need to take the same lock. Once
// a caller proceeds to step two, it likely wants to keep the lock until
// DoChan() returned a channel, in order to ensure that a flight is only started
// before any modifications to the datastructure occurred (relative to the state
// observed in step one). Were the lock to be released before calling DoChan(),
// a previous flight might modify the datastructure before our flight began.
func (g *Group) DoChan(key string, fn func() (interface{}, error)) (<-chan Result, bool) {
	ch := make(chan Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)
		g.mu.Unlock()
		return ch, false
	}
	c := &call{chans: []chan<- Result{ch}}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	go g.doCall(c, key, fn)

	return ch, true
}

// doCall handles the single call for a key.
func (g *Group) doCall(c *call, key string, fn func() (interface{}, error)) {
	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	for _, ch := range c.chans {
		ch <- Result{c.val, c.err, c.dups > 0}
	}
	g.mu.Unlock()
}

var _ = (*Group).Forget

// Forget tells the singleflight to forget about a key.  Future calls
// to Do for this key will call the function rather than waiting for
// an earlier call to complete.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// NumCalls returns the number of in-flight calls for a given key.
func (g *Group) NumCalls(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.m[key]; ok {
		return c.dups + 1
	}
	return 0
}
