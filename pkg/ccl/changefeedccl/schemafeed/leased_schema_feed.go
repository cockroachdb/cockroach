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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/internal/client/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// The deal is that I need to track the underlying schema feed and make sure
// that my consumption point overlaps with validity period for each lease.

type lease struct {
	tableID sqlbase.ID
	minTs   hlc.Timestamp
	l       leasemanager.Lease
}

type Lock interface {
	Txn() *client.Txn
	Key() []byte
	Exclusive() bool

	Validity() (start, currentExpiration hlc.Timestamp)
}

type LockManager interface {
	AcquireShared(ctx context.Context, txn *client.Txn, key []byte) (leasemanager.Lease, error)
	AcquireExclusive(ctx context.Context, txn *client.Txn, key []byte) (leasemanager.Lease, error)
}

var tableDescPrefix = keys.MakeTablePrefix(keys.TableDescriptorSingleVersionLockTableID)

// LeasedSchemaFeed wraps the regular SchemaFeed to provide lower latency time
// to event emission by coordinating directly with schema changes.
type LeasedSchemaFeed struct {
	sf      *SchemaFeed
	db      *client.DB
	targets jobspb.ChangefeedTargets
	lm      LockManager
	ds      *kv.DistSender

	leaseCh          chan lease
	eventCh          chan *roachpb.RangeFeedEvent
	initialTimestamp hlc.Timestamp

	mu struct {
		syncutil.RWMutex

		highWater hlc.Timestamp
		leases    map[sqlbase.ID]lease
	}
}

var _ LockManager = (*leasemanager.LeaseManager)(nil)

func NewLeasedSchemaFeed(cfg Config) *LeasedSchemaFeed {
	if cfg.LockManager == nil {
		panic("nil lock manager!")
	}
	f := &LeasedSchemaFeed{
		db:               cfg.DB,
		targets:          cfg.Targets,
		lm:               cfg.LockManager,
		ds:               cfg.DistSender,
		initialTimestamp: cfg.InitialHighWater,
		leaseCh:          make(chan lease),
		eventCh:          make(chan *roachpb.RangeFeedEvent),
	}
	f.mu.leases = make(map[sqlbase.ID]lease, len(f.targets))
	for tableID := range f.targets {
		f.mu.leases[tableID] = lease{tableID: tableID, minTs: cfg.InitialHighWater}
	}
	f.sf = New(cfg)

	return f
}

func (f *LeasedSchemaFeed) Run(ctx context.Context) error {
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(f.sf.Run)
	g.GoCtx(func(ctx context.Context) error {
		return f.run(ctx, &g)
	})
	return g.Wait()
}

func (f *LeasedSchemaFeed) run(ctx context.Context, g *ctxgroup.Group) error {
	defer f.cleanupLeasesOnExit()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g.GoCtx(f.runNotifications)
	f.acquireInitialLeases(ctx, g)
	// The logic in this loop ensures that there is at most one goroutine fetching
	// a lease for a table.
	for {
		select {
		case l := <-f.leaseCh:
			// We need to make sure that we're allowed to hold this lease.
			if log.V(1) {
				log.Infof(ctx, "acquired shared lease for %v@%v", l.tableID, l.l.StartTime())
			}
			if !f.maybeAddLease(l) {
				log.Infof(ctx, "dropping shared lease for %v@%v", l.tableID, l.l.StartTime())
				g.GoCtx(func(ctx context.Context) error {
					cleanupLease(ctx, l)
					return f.acquireLeaseFor(ctx, l.tableID)
				})
			}
		case e := <-f.eventCh:
			f.handleNotification(ctx, g, e)
		case <-ctx.Done():
			return nil
		}
	}
}

func (f *LeasedSchemaFeed) runNotifications(ctx context.Context) error {
	notifySpanPrefix := roachpb.Key(keys.MakeTablePrefix(keys.TableDescriptorSingleVersionLockNotifyTableID))
	sp := roachpb.Span{
		Key:    notifySpanPrefix,
		EndKey: notifySpanPrefix.PrefixEnd(),
	}
	return f.ds.RangeFeed(ctx, sp, f.initialTimestamp, false, f.eventCh)
}

func (f *LeasedSchemaFeed) cleanupLeasesOnExit() {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Which context should I use here? If we're shutting down then I don't
	// need to cleanup my transactions but otherwise I really do.
	ctx := context.TODO()
	for _, t := range f.mu.leases {
		if t.l != nil {

			if err := t.l.Txn().CommitOrCleanup(ctx); err != nil {
				log.Infof(ctx, "failed to commit shared lease during cleanup: %v", err)
			}
		}
	}
}

func (f *LeasedSchemaFeed) acquireInitialLeases(ctx context.Context, g *ctxgroup.Group) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// We need to acquire leases for all of the targets
	for id := range f.targets {
		tableID := id
		// We need to coordinate goroutines to acquire the locks.
		g.GoCtx(func(ctx context.Context) error {
			return f.acquireLeaseFor(ctx, tableID)
		})
	}
}

func (f *LeasedSchemaFeed) handleNotification(
	ctx context.Context, g *ctxgroup.Group, e *roachpb.RangeFeedEvent,
) error {
	if e.Val == nil { // nothing to do with resolved timestamps
		return nil
	}

	tableID, err := decodeNotificationRangefeedEvent(e)
	if err != nil {
		log.Errorf(ctx, "failed to decode event: %v %v", e.Val.Key, err)
		return err
	}
	if log.V(1) {
		log.Infof(ctx, "got event for %d@%v", tableID, e.Val.Value.Timestamp)
	}
	l := f.handleInvalidation(tableID, e.Val.Value.Timestamp)
	if l.l != nil {
		g.GoCtx(func(ctx context.Context) error {
			// We really don't want to leave this txn hanging around, use context.Background().
			cleanupLease(ctx, l)
			return f.acquireLeaseFor(ctx, l.tableID)
		})
	}
	return nil
}

func cleanupLease(ctx context.Context, l lease) {
	if err := l.l.Txn().CommitOrCleanup(context.Background()); err != nil {
		log.Warningf(ctx, "failed to commit lease txn: %v", err)
	}
}

func (f *LeasedSchemaFeed) handleInvalidation(tableID sqlbase.ID, timestamp hlc.Timestamp) lease {
	f.mu.Lock()
	defer f.mu.Unlock()
	existing := f.mu.leases[tableID]
	l := existing
	l.minTs = timestamp
	l.l = nil
	f.mu.leases[tableID] = l
	return existing
}

func decodeNotificationRangefeedEvent(e *roachpb.RangeFeedEvent) (sqlbase.ID, error) {
	k, parent, err := keys.DecodeTablePrefix(e.Val.Key)
	if parent != keys.TableDescriptorSingleVersionLockNotifyTableID {
		return 0, errors.AssertionFailedf("expected notifications from table %d, got %d",
			keys.TableDescriptorSingleVersionLockNotifyTableID, parent)
	}
	if err != nil {
		return 0, errors.Wrapf(err, "failed to decode table prefix")
	}
	_, id, err := encoding.DecodeUntaggedIntValue(k)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to decode table key")
	}
	return sqlbase.ID(id), nil
}

func (f *LeasedSchemaFeed) maybeAddLease(l lease) (added bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	existing := f.mu.leases[l.tableID]
	log.Infof(context.TODO(), "in maybeAddLease %v %v %v", l.tableID, existing.minTs, l.l.StartTime())
	if l.l.StartTime().Less(existing.minTs) {
		log.Infof(context.TODO(), "not using lease for %d@%v", l.tableID, l.l.StartTime())
		return false
	}
	existing.l = l.l
	f.mu.leases[l.tableID] = existing
	return true
}

func (f *LeasedSchemaFeed) acquireLeaseFor(ctx context.Context, tableID sqlbase.ID) (err error) {
	for r := retry.StartWithCtx(ctx, retry.Options{}); r.Next(); {
		txn := f.db.NewTxn(ctx, "schemalease")
		lockKey := encoding.EncodeUntaggedIntValue(nil, int64(tableID))
		l, err := f.lm.AcquireShared(ctx, txn, lockKey)
		if err != nil {
			// TODO(ajwerner): this is not going to fly - we should do some retry with
			// backoff.
			log.Errorf(ctx, "failed to acquire shared lease for %v %q: %v", tableID, lockKey, err)
			if err := txn.CommitOrCleanup(context.TODO()); err != nil {
				log.Errorf(ctx, "failed to cleanup shared acquire lease for %v %q: %v", tableID, lockKey, err)
			}
			continue
		}
		select {
		case f.leaseCh <- lease{tableID: tableID, l: l}:
			return nil
		case <-ctx.Done():
			if err := txn.CommitOrCleanup(context.TODO()); err != nil {
				log.Errorf(ctx, "failed to cleanup shared acquire lease for %v %q: %v", tableID, lockKey, err)
			}
			return ctx.Err()
		}
	}

	return nil
}

func (f *LeasedSchemaFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) ([]TableEvent, error) {
	return f.peekOrPop(ctx, atOrBefore, false /* pop */)
}

func (f *LeasedSchemaFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) ([]TableEvent, error) {
	return f.peekOrPop(ctx, atOrBefore, true /* pop */)
}

func (f *LeasedSchemaFeed) peekOrPop(
	ctx context.Context, atOrBefore hlc.Timestamp, pop bool,
) ([]TableEvent, error) {
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

	events, err := f.sf.peekOrPop(ctx, atOrBefore, pop)

	if err != nil {
		return nil, err
	}
	if log.V(3) {
		log.Infof(ctx, "returning with events for %v %v %v", pop, atOrBefore, len(events))
	}
	if pop || len(events) == 0 {
		f.maybeUpdateHighWater(atOrBefore)
	}
	return events, nil
}

func (f *LeasedSchemaFeed) maybeUpdateHighWater(atOrBefore hlc.Timestamp) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.highWater.Less(atOrBefore) {
		f.mu.highWater = atOrBefore
	}
}

func (f *LeasedSchemaFeed) hasValidLeasesFor(ts hlc.Timestamp) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var maxStart hlc.Timestamp
	for _, l := range f.mu.leases {
		if l.l == nil {
			return false
		}
		start, expiration := l.l.StartTime(), l.l.GetExpiration()
		// TODO(ajwerner): These are conservative on the endpoints.
		if !start.Less(ts) || !ts.Less(expiration) {
			return false
		}
		if maxStart.IsEmpty() || maxStart.Less(start) {
			maxStart = start
		}
		l.l.Txn().PushTo(ts)
	}
	if !maxStart.Less(f.mu.highWater) {
		return false
	}
	return true
}

func (f *LeasedSchemaFeed) atOrBelowHighWater(ts hlc.Timestamp) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return ts.LessEq(f.mu.highWater)
}
