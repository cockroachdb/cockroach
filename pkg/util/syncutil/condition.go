// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	_ "unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
)

// Condition is the predicate used with CondMutex.
// NB: condition function *must* be side effects free.
// NB: condition function *must* be a function of the state protected by the lock.
// If the above requires are not true, behavior is undefined.
type Condition func() bool

// CondMutex is a Mutex that adds conditional locking.
// Zero initialized CondMutex is ready to use.
type CondMutex struct {
	w    Mutex
	cond struct {
		RWMutex // Acquired before w.
		cv      sync.Cond
		nw      int // Number of callers waiting for cond.
	}
}

// Lock implements Locker interface.
func (c *CondMutex) Lock() {
	c.w.Lock()
}

// Unlock implements Locker interface.
func (c *CondMutex) Unlock() {
	c.cond.RLock()
	if c.cond.nw > 0 {
		c.cond.cv.Signal()
	}
	c.cond.RUnlock()
	c.w.Unlock()
}

// LockWhen acquires the lock and waits for condition to be true.
func (c *CondMutex) LockWhen(cond Condition) {
	if err := c.condLock(noncancelable(cond)); err != nil {
		panic(fmt.Sprintf("unexpected error %s", err))
	}
	c.w.AssertHeld()
}

// LockWhenCtx locks this lock and waits for condition to be true.
// Acquires the lock and returns nil when condition becomes true.
// Does not acquire the lock and returns context error if condition
// failed to evaluate to true before context completion.
func (c *CondMutex) LockWhenCtx(ctx context.Context, cond Condition) error {
	if ctx.Done() == nil {
		c.LockWhen(cond)
		return nil
	}

	cCond := cancelable{cond: cond}
	if err := ctxutil.WhenDone(ctx, func(err error) {
		cCond.cancel.Store(err)
		c.cond.cv.Signal()
	}); err != nil {
		return err
	}

	if err := c.condLock(&cCond); err != nil {
		return err
	}
	c.w.AssertHeld()
	return nil
}

// LockCtx acquires the lock, or returns an error if the lock could not
// be acquired prior to context completion.
func (c *CondMutex) LockCtx(ctx context.Context) error {
	return c.LockWhenCtx(ctx, func() bool { return true })
}

type condition interface {
	holds() bool
	err() error
}

type cancelable struct {
	cond   Condition
	cancel atomic.Value // of error
}

func (c *cancelable) holds() bool {
	return c.cond()
}
func (c *cancelable) err() error {
	if v := c.cancel.Load(); v != nil {
		return v.(error)
	}
	return nil
}

type noncancelable Condition

func (c noncancelable) holds() bool {
	return c()
}
func (c noncancelable) err() error {
	return nil
}

func (c *CondMutex) condLock(cond condition) error {
	{
		// Fast path.
		c.cond.RLock()
		locked := false
		if c.w.TryLock() {
			if cond.holds() {
				locked = true
			} else {
				c.w.Unlock() // Condition was false.  Drop write lock; go on slow path below.
			}
		}
		c.cond.RUnlock()
		if locked {
			return nil
		}
	}

	c.cond.Lock()
	if c.cond.cv.L == nil {
		c.cond.cv.L = &c.cond.RWMutex
	}
	c.cond.nw++
	defer func() {
		c.cond.nw--
		c.cond.Unlock()
	}()

	for {
		if c.w.TryLock() {
			if cond.holds() {
				return nil // c.cond mutex released by defer.
			}
			c.w.Unlock() // Drop write lock; we need to wait on cond.
		}

		// Before we return an error or go sleep to wait on cv,
		// make sure to wake up any other waiters that may be sleeping, waiting for signal.
		if c.cond.nw > 1 {
			c.cond.cv.Signal()
		}

		if err := cond.err(); err != nil {
			return err // c.cond mutex released by defer.
		}

		c.cond.cv.Wait()
	}
}
