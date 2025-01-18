// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admit_long"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble"
	"github.com/dustin/go-humanize"
)

type compactionScheduler struct {
	granter                    admit_long.WorkGranter
	db                         pebble.DBForCompaction
	numGoroutinesPerCompaction int
	mu                         struct {
		syncutil.Mutex
		storeID roachpb.StoreID
		// unregistered is used to avoid registering if SetStoreID is called after
		// Unregister.
		unregistered  bool
		tempScheduler struct {
			isGranting     bool
			isGrantingCond *sync.Cond
			runningCount   int
		}
	}
}

type CompactionSchedulerPlus interface {
	pebble.CompactionScheduler
	// SetStoreID is called after Register, but can be concurrent with other methods.
	SetStoreID(storeID roachpb.StoreID)
}

var _ CompactionSchedulerPlus = &compactionScheduler{}

func NewCompactionScheduler(granter admit_long.WorkGranter) CompactionSchedulerPlus {
	c := &compactionScheduler{
		granter: granter,
	}
	c.mu.tempScheduler.isGrantingCond = sync.NewCond(&c.mu.Mutex)
	return c
}

func (c *compactionScheduler) Register(numGoroutinesPerCompaction int, db pebble.DBForCompaction) {
	c.db = db
	c.numGoroutinesPerCompaction = numGoroutinesPerCompaction
}

func (c *compactionScheduler) SetStoreID(storeID roachpb.StoreID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Waiting until the tempScheduler is no longer active is mainly defensive.
	// It is simpler to reason about correctness if we don't allow concurrent
	// activity of the temp scheduler and the real scheduler.
	for c.mu.tempScheduler.isGranting {
		c.mu.tempScheduler.isGrantingCond.Wait()
	}
	if c.mu.unregistered {
		return
	}
	if c.mu.storeID != 0 {
		if c.mu.storeID != storeID {
			panic("storeID is already set to a different value")
		}
		return
	}
	c.mu.storeID = storeID
	c.granter.RegisterRequester(admit_long.WorkCategoryAndStore{
		Category: admit_long.PebbleCompaction,
		Store:    c.mu.storeID,
	}, c, c.numGoroutinesPerCompaction)
}

func (c *compactionScheduler) Unregister() {
	c.mu.Lock()
	c.mu.unregistered = true
	// Wait until tempScheduler is no longer active.
	for c.mu.tempScheduler.isGranting {
		c.mu.tempScheduler.isGrantingCond.Wait()
	}
	// Sample the value of c.mu.storeID. If it is not set, then as we have set
	// unregistered, we will never register with the real scheduler. If set, we
	// must have already registered with the real scheduler, so need to
	// unregister.
	storeID := c.mu.storeID
	c.mu.Unlock()
	if storeID != 0 {
		c.granter.UnregisterRequester(admit_long.WorkCategoryAndStore{
			Category: admit_long.PebbleCompaction,
			Store:    storeID,
		})
	}
}

func (c *compactionScheduler) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	c.mu.Lock()
	if c.mu.storeID == 0 {
		allowedCount := c.db.GetAllowedWithoutPermission()
		log.Infof(context.Background(), "workGranterImpl.2 allowedCount: %d", allowedCount)
		if c.mu.tempScheduler.runningCount < allowedCount {
			c.mu.tempScheduler.runningCount++
			c.mu.Unlock()
			log.Infof(context.Background(), "workGranterImpl.TryGet not called TrySchedule true")
			return true, compactionGrantHandle{tempScheduler: c}
		}
		c.mu.Unlock()
		log.Infof(context.Background(), "workGranterImpl.TryGet not called TrySchedule false")
		return false, nil
	}
	storeID := c.mu.storeID
	c.mu.Unlock()
	// storeID != 0. So already registered with the real scheduler.
	success, reporter := c.granter.TryGet(admit_long.WorkCategoryAndStore{
		Category: admit_long.PebbleCompaction,
		Store:    storeID,
	})
	log.Infof(context.Background(), "workGranterImpl.TryGet caller success: %t", success)
	if !success {
		return false, nil
	}
	return true, compactionGrantHandle{reporter: reporter}
}

func (c *compactionScheduler) GetAllowedWithoutPermissionForStore() int {
	// Must have already registered with the real scheduler.
	allowed := c.db.GetAllowedWithoutPermission()
	log.Infof(context.Background(), "workGranterImpl.GetAllowedWithoutPermissionForStore: %d", allowed)
	return allowed
}

func (c *compactionScheduler) GetScore() (bool, admit_long.WorkScore) {
	// Must have already registered with the real scheduler.
	waiting, compaction := c.db.GetWaitingCompaction()
	if !waiting {
		return false, admit_long.WorkScore{}
	}
	return true, admit_long.WorkScore{
		Optional: compaction.Optional,
		// We don't expect scores to exceed 100, since compactions try to happen
		// when score >= 1.0.
		Score: float64(compaction.Priority*100) + compaction.Score,
	}
}

func (c *compactionScheduler) Grant(reporter admit_long.WorkResourceUsageReporter) bool {
	// Must have already registered with the real scheduler.
	h := compactionGrantHandle{reporter: reporter}
	return c.db.Schedule(h)
}

func (c *compactionScheduler) tempDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.tempScheduler.runningCount--
	if c.mu.storeID != 0 {
		log.Infof(context.Background(), "workGranterImpl.WorkDone not called. in tempDone. Switched to real scheduler")
		// Do a TryGet to ensure that the real scheduler knows this requester may be waiting.
		storeID := c.mu.storeID
		c.mu.tempScheduler.isGranting = true
		c.mu.Unlock()
		for {
			success, reporter := c.granter.TryGet(admit_long.WorkCategoryAndStore{
				Category: admit_long.PebbleCompaction,
				Store:    storeID,
			})
			if !success {
				break
			}
			h := compactionGrantHandle{reporter: reporter}
			if !c.db.Schedule(h) {
				// Note this will not cause a call to tempDone since this a real usage reporter.
				reporter.WorkDone()
				break
			}
		}
		c.mu.Lock()
		c.mu.tempScheduler.isGranting = false
		return
	}
	// tempScheduler is still active.

	log.Infof(context.Background(), "workGranterImpl.WorkDone not called. in tempDone. NOT Switched to real scheduler")
	c.mu.tempScheduler.isGranting = true
	c.mu.Unlock()
	waiting, _ := c.db.GetWaitingCompaction()
	// For loop exits with c.mu not held.
	for waiting {
		log.Infof(context.Background(), "workGranterImpl.WorkDone not called. waiting: %t", waiting)
		c.mu.Lock()
		allowedCount := c.db.GetAllowedWithoutPermission()
		log.Infof(context.Background(), "workGranterImpl.1 allowedCount: %d", allowedCount)

		if c.mu.tempScheduler.runningCount < allowedCount {
			c.mu.tempScheduler.runningCount++
			c.mu.Unlock()
			success := c.db.Schedule(compactionGrantHandle{tempScheduler: c})
			c.mu.Lock()
			if !success {
				c.mu.tempScheduler.runningCount--
			}
			c.mu.Unlock()
			waiting, _ = c.db.GetWaitingCompaction()
		} else {
			c.mu.Unlock()
			break
		}
	}
	c.mu.Lock()
	c.mu.tempScheduler.isGranting = false
}

type compactionGrantHandle struct {
	tempScheduler *compactionScheduler
	reporter      admit_long.WorkResourceUsageReporter
}

func (h compactionGrantHandle) Started() {
	if h.tempScheduler != nil {
		return
	}
	h.reporter.WorkStart()
}

func (h compactionGrantHandle) MeasureCPU(g int) {
	if h.tempScheduler != nil {
		return
	}
	h.reporter.MeasureCPU(g)
}

func (h compactionGrantHandle) CumulativeStats(stats pebble.CompactionGrantHandleStats) {
	if h.tempScheduler != nil {
		return
	}
	id := h.reporter.GetIDForDebugging()
	if false {
		log.Infof(context.Background(), "CumulativeStats ID: %d , stats: %s (%s)", id,
			humanize.IBytes(stats.CumWriteBytes), humanize.IBytes(stats.Debugging.EstimatedSize))
	}
	writeBytes := admit_long.WriteBytes{Bytes: int64(stats.CumWriteBytes)}
	writeBytes.Debugging.Estimated = int64(stats.Debugging.EstimatedSize)
	h.reporter.CumulativeWriteBytes(writeBytes)
}

func (h compactionGrantHandle) Done() {
	if h.tempScheduler != nil {
		h.tempScheduler.tempDone()
		return
	}
	h.reporter.WorkDone()
}
