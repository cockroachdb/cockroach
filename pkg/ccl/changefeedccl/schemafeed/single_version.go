// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/sync/errgroup"
)

// The deal is that I need to track the underlying schema feed and make sure
// that my consumption point overlaps with validity period for each singleVersionLease.

type singleVersionLease struct {
	tableID          descpb.ID
	maxUsedTimestamp hlc.Timestamp
	minTs            hlc.Timestamp
	l                singleversion.Lease
}

// leasedSchemaFeed wraps the regular SchemaFeed to provide lower latency time
// to event emission by coordinating directly with schema changes.
type leasedSchemaFeed struct {
	sf SchemaFeed

	targets jobspb.ChangefeedTargets
	sva     singleversion.Acquirer

	leaseCh          chan singleversion.Lease
	eventCh          chan singleversion.Notification
	initialTimestamp hlc.Timestamp

	mu struct {
		syncutil.RWMutex

		highWater hlc.Timestamp
		leases    map[descpb.ID]singleVersionLease
	}
}

func newLeasedSchemaFeed(
	sva singleversion.Acquirer,
	targets jobspb.ChangefeedTargets,
	initialHighWater hlc.Timestamp,
	underlying SchemaFeed,
) *leasedSchemaFeed {
	f := &leasedSchemaFeed{
		targets:          targets,
		sva:              sva,
		initialTimestamp: initialHighWater,
		leaseCh:          make(chan singleversion.Lease),
		eventCh:          make(chan singleversion.Notification),
	}
	f.mu.leases = make(map[descpb.ID]singleVersionLease, len(f.targets))
	for tableID := range f.targets {
		f.mu.leases[tableID] = singleVersionLease{tableID: tableID, minTs: initialHighWater}
	}
	f.sf = underlying
	return f
}

func (f *leasedSchemaFeed) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return f.sf.Run(ctx) })
	g.Go(func() error { return f.run(ctx, g) })
	return g.Wait()
}

func (f *leasedSchemaFeed) run(ctx context.Context, g *errgroup.Group) error {
	defer f.cleanupLeasesOnExit()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g.Go(func() error {
		return f.sva.WatchForNotifications(
			ctx, f.initialTimestamp, targetIDs(f.targets), f.eventCh,
		)
	})
	f.acquireInitialLeases(ctx, g)
	// The logic in this loop ensures that there is at most one goroutine fetching
	// a singleVersionLease for a table.
	for {
		select {
		case l := <-f.leaseCh:
			// We need to make sure that we're allowed to hold this singleVersionLease.
			if !f.maybeAddLease(ctx, l) {
				if log.V(1) {
					log.Infof(ctx, "dropping shared singleVersionLease for %v@%v", l.ID(), l.Start())
				}
				g.Go(func() error {
					l.Release(ctx, hlc.Timestamp{})
					return f.acquireLeaseFor(ctx, l.ID())
				})
			}
		case e := <-f.eventCh:
			f.handleNotification(ctx, g, e)

		case <-ctx.Done():
			return nil
		}
	}
}

func (f *leasedSchemaFeed) cleanupLeasesOnExit() {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Which context should I use here? If we're shutting down then I don't
	// need to cleanup my transactions but otherwise I really do.
	ctx := context.TODO()
	for _, t := range f.mu.leases {
		if t.l != nil {
			t.l.Release(ctx, t.maxUsedTimestamp)
		}
	}
}

func (f *leasedSchemaFeed) acquireInitialLeases(ctx context.Context, g *errgroup.Group) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// We need to acquire leases for all of the targets
	for id := range f.targets {
		tableID := id
		// We need to coordinate goroutines to acquire the locks.
		g.Go(func() error {
			return f.acquireLeaseFor(ctx, tableID)
		})
	}
}

func (f *leasedSchemaFeed) handleNotification(
	ctx context.Context, g *errgroup.Group, e singleversion.Notification,
) {
	if log.V(1) {
		log.Infof(ctx, "got event for %d@%v", e.ID, e.Timestamp)
	}
	toRelease, maxUsedTimestamp := f.handleInvalidation(e.ID, e.Timestamp)
	if toRelease != nil {
		g.Go(func() error {
			// We really don't want to leave this txn hanging around, use context.Background().
			toRelease.Release(ctx, maxUsedTimestamp)
			return f.acquireLeaseFor(ctx, e.ID)
		})
	}
}

func (f *leasedSchemaFeed) handleInvalidation(
	tableID descpb.ID, timestamp hlc.Timestamp,
) (toRelease singleversion.Lease, maxUsedTimestamp hlc.Timestamp) {
	f.mu.Lock()
	defer f.mu.Unlock()
	existing := f.mu.leases[tableID]
	l := existing
	l.minTs.Forward(timestamp)
	l.l = nil
	f.mu.leases[tableID] = l
	return existing.l, existing.maxUsedTimestamp
}

func (f *leasedSchemaFeed) maybeAddLease(ctx context.Context, l singleversion.Lease) (added bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	existing := f.mu.leases[l.ID()]
	if l.Start().Less(existing.minTs) {
		if log.V(1) {
			log.Infof(ctx, "not using acquired single-version lease for %v@%v due"+
				" to min timestamp %v", l.ID(), l.Start(), existing.minTs)
		}
		return false
	}
	if log.V(1) {
		log.Infof(ctx, "acquired single-version lease for %v@%v", l.ID(), l.Start())
	}
	existing.l = l
	f.mu.leases[l.ID()] = existing
	return true
}

func (f *leasedSchemaFeed) acquireLeaseFor(ctx context.Context, tableID descpb.ID) (err error) {
	for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
		l, err := f.sva.Acquire(ctx, tableID)
		if err != nil {
			// TODO(ajwerner): this is not going to fly - we should do some retry with
			// backoff.
			log.Errorf(ctx, "failed to acquire shared singleVersionLease for %d: %v", tableID, err)
			continue
		}
		select {
		case f.leaseCh <- l:
			return nil
		case <-ctx.Done():
			l.Release(ctx, hlc.Timestamp{})
			return ctx.Err()
		}
	}

	return nil
}

func (f *leasedSchemaFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) ([]TableEvent, error) {
	return f.peekOrPop(ctx, atOrBefore, false /* pop */)
}

func (f *leasedSchemaFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) ([]TableEvent, error) {
	return f.peekOrPop(ctx, atOrBefore, true /* pop */)
}

func (f *leasedSchemaFeed) peekOrPop(
	ctx context.Context, atOrBefore hlc.Timestamp, pop bool,
) (events []TableEvent, err error) {
	if f.atOrBelowHighWater(atOrBefore) {
		if log.V(3) {
			log.Infof(ctx, "returning due to at or before %v", atOrBefore)
		}

		return nil, nil
	}

	if f.hasValidLeasesFor(atOrBefore) {
		if log.V(3) {
			log.Infof(ctx, "returning due to leases %v", atOrBefore)
		}
		return nil, nil
	}

	if log.V(3) {
		log.Infof(ctx, "calling into underlying %v", atOrBefore)
	}
	if pop {
		events, err = f.sf.Pop(ctx, atOrBefore)
	} else {
		events, err = f.sf.Peek(ctx, atOrBefore)
	}

	if err != nil {
		return nil, err
	}
	if log.V(3) {
		log.Infof(ctx, "returning with events for %v %v %v", pop, atOrBefore, len(events))
	}
	if pop || len(events) == 0 {
		if log.V(3) {
			log.Infof(ctx, "updating highwater to %v", atOrBefore)
		}
		f.maybeUpdateHighWater(atOrBefore)
	}
	return events, nil
}

func (f *leasedSchemaFeed) maybeUpdateHighWater(atOrBefore hlc.Timestamp) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.highWater.Less(atOrBefore) {
		f.mu.highWater = atOrBefore
	}
}

func (f *leasedSchemaFeed) hasValidLeasesFor(ts hlc.Timestamp) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.mu.highWater.IsEmpty() {
		return false
	}
	var maxStart hlc.Timestamp
	for _, l := range f.mu.leases {
		if l.l == nil {
			return false
		}
		start, expiration := l.l.Start(), l.l.Expiration()
		// TODO(ajwerner): These are conservative on the endpoints.
		if !start.Less(ts) || !ts.Less(expiration) {
			return false
		}
		if maxStart.IsEmpty() {
			maxStart = start
		} else {
			maxStart.Forward(start)
		}
		l.maxUsedTimestamp.Forward(ts)
	}
	return maxStart.Less(f.mu.highWater)
}

func (f *leasedSchemaFeed) atOrBelowHighWater(ts hlc.Timestamp) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return ts.LessEq(f.mu.highWater)
}

func targetIDs(targets jobspb.ChangefeedTargets) (s catalog.DescriptorIDSet) {
	for id := range targets {
		s.Add(id)
	}
	return s
}
