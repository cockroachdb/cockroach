// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// The structure definitions in this file have been cross-checked against
// go1.16, and go1.17beta1 (needs revalidation). Before allowing newer
// versions, please check that the structures still match with those in
// go/src/runtime.
// +build gc,go1.16,!go1.18

package goschedstats

import (
	"sync/atomic"
	_ "unsafe" // required by go:linkname
)

type puintptr uintptr

type muintptr uintptr

type guintptr uintptr

type sysmontick struct {
	schedtick   uint32
	schedwhen   int64
	syscalltick uint32
	syscallwhen int64
}

type pageCache struct {
	base  uintptr // base address of the chunk
	cache uint64  // 64-bit bitmap representing free pages (1 means free)
	scav  uint64  // 64-bit bitmap representing scavenged pages (1 means scavenged)
}

type p struct {
	id          int32
	status      uint32 // one of pidle/prunning/...
	link        puintptr
	schedtick   uint32     // incremented on every scheduler call
	syscalltick uint32     // incremented on every system call
	sysmontick  sysmontick // last tick observed by sysmon
	m           muintptr   // back-link to associated m (nil if idle)
	mcache      uintptr
	pcache      pageCache
	raceprocctx uintptr

	deferpool    [5][]uintptr // pool of available defer structs of different sizes (see panic.go)
	deferpoolbuf [5][32]uintptr

	// Cache of goroutine ids, amortizes accesses to runtime·sched.goidgen.
	goidcache    uint64
	goidcacheend uint64

	// Queue of runnable goroutines. Accessed without lock.
	runqhead uint32
	runqtail uint32
	runq     [256]guintptr
	// runnext, if non-nil, is a runnable G that was ready'd by
	// the current G and should be run next instead of what's in
	// runq if there's time remaining in the running G's time
	// slice. It will inherit the time left in the current time
	// slice. If a set of goroutines is locked in a
	// communicate-and-wait pattern, this schedules that set as a
	// unit and eliminates the (potentially large) scheduling
	// latency that otherwise arises from adding the ready'd
	// goroutines to the end of the run queue.
	runnext guintptr

	// The rest of the fields aren't important.
}

type mutex struct {
	// Empty struct if lock ranking is disabled, otherwise includes the lock rank
	//lockRankStruct
	// Futex-based impl treats it as uint32 key,
	// while sema-based impl as M* waitm.
	// Used to be a union, but unions break precise GC.
	key uintptr
}

type gQueue struct {
	head guintptr
	tail guintptr
}

type schedt struct {
	// accessed atomically. keep at top to ensure alignment on 32-bit systems.
	goidgen   uint64
	lastpoll  uint64 // time of last network poll, 0 if currently polling
	pollUntil uint64 // time to which current poll is sleeping

	lock mutex

	// When increasing nmidle, nmidlelocked, nmsys, or nmfreed, be
	// sure to call checkdead().

	midle        muintptr // idle m's waiting for work
	nmidle       int32    // number of idle m's waiting for work
	nmidlelocked int32    // number of locked m's waiting for work
	mnext        int64    // number of m's that have been created and next M ID
	maxmcount    int32    // maximum number of m's allowed (or die)
	nmsys        int32    // number of system m's not counted for deadlock
	nmfreed      int64    // cumulative number of freed m's

	ngsys uint32 // number of system goroutines; updated atomically

	pidle      puintptr // idle p's
	npidle     uint32
	nmspinning uint32 // See "Worker thread parking/unparking" comment in proc.go.

	// Global runnable queue.
	runq     gQueue
	runqsize int32

	// The rest of the fields aren't important.
}

//go:linkname allp runtime.allp
var allp []*p

//go:linkname sched runtime.sched
var sched schedt

//go:linkname lock runtime.lock
func lock(l *mutex)

//go:linkname unlock runtime.unlock
func unlock(l *mutex)

func numRunnableGoroutines() (numRunnable int, numProcs int) {
	lock(&sched.lock)
	numRunnable = int(sched.runqsize)
	numProcs = len(allp)

	// Note that holding sched.lock prevents the number of Ps from changing, so
	// it's safe to loop over allp.
	for _, p := range allp {
		// Retry loop for concurrent updates.
		for {
			h := atomic.LoadUint32(&p.runqhead)
			t := atomic.LoadUint32(&p.runqtail)
			next := atomic.LoadUintptr((*uintptr)(&p.runnext))
			runnable := int32(t - h)
			if atomic.LoadUint32(&p.runqhead) != h || runnable < 0 {
				// A concurrent update messed with us; try again.
				continue
			}
			if next != 0 {
				runnable++
			}
			numRunnable += int(runnable)
			break
		}
	}
	unlock(&sched.lock)
	return numRunnable, numProcs
}
