// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rgreconciler watches a tenant's local system.resource_groups
// table over a rangefeed and forwards changes to the host through a
// resourcegroup.Pusher.
//
// Reconcile runs an outer retry loop. Each iteration starts a
// rangefeedcache.Watcher; its onUpdate callback ferries updates to
// the main goroutine over a channel. The main goroutine performs the
// Push/Replace and returns an error on failure, which tears down the
// watcher and restarts with backoff — guaranteeing a fresh
// CompleteUpdate on the next iteration.
//
// On CompleteUpdate, the reconciler calls Replace, which atomically
// deletes all host-side rows for the tenant and re-inserts the
// snapshot. This is self-healing: no in-memory state is carried
// across restarts, and stale rows from dropped groups are cleaned up.
//
// On IncrementalUpdate, events are deduplicated per-id (keeping the
// latest) and pushed as a batch.
package rgreconciler

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

// bufferSize bounds how many in-flight rangefeed events may be queued
// before the next frontier advance.
const bufferSize = 4096

// Reconciler watches a tenant's system.resource_groups via a
// rangefeed and pushes changes to the host through a Pusher.
type Reconciler struct {
	codec     keys.SQLCodec
	clock     *hlc.Clock
	rfFactory *rangefeed.Factory
	stopper   *stop.Stopper
	resolver  catalog.SystemTableIDResolver
	pusher    resourcegroup.Pusher
}

// New constructs a Reconciler.
func New(
	codec keys.SQLCodec,
	clock *hlc.Clock,
	rfFactory *rangefeed.Factory,
	stopper *stop.Stopper,
	resolver catalog.SystemTableIDResolver,
	pusher resourcegroup.Pusher,
) *Reconciler {
	return &Reconciler{
		codec:     codec,
		clock:     clock,
		rfFactory: rfFactory,
		stopper:   stopper,
		resolver:  resolver,
		pusher:    pusher,
	}
}

// event is what translateEvent emits per rangefeed KV. The Watcher's
// buffer collects these and hands them back as Update.Events.
type event struct {
	upsert    *rgpb.ResourceGroupUpsert
	id        int64
	tombstone bool
	ts        hlc.Timestamp
}

// Timestamp implements rangefeedbuffer.Event.
func (e *event) Timestamp() hlc.Timestamp { return e.ts }

// Reconcile blocks until ctx is cancelled. It retries with backoff on
// any error from the rangefeed or from pushing to the host; the
// backoff resets if the inner loop ran for longer than 5 minutes.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	tableID, err := startup.RunIdempotentWithRetryEx(ctx,
		r.stopper.ShouldQuiesce(),
		"resource group reconciler table id lookup",
		func(ctx context.Context) (uint32, error) {
			id, err := r.resolver.LookupSystemTableID(
				ctx, systemschema.ResourceGroupsTable.GetName(),
			)
			return uint32(id), err
		})
	if err != nil {
		return err
	}
	tablePrefix := r.codec.IndexPrefix(tableID, 1)
	tableSpan := roachpb.Span{
		Key: tablePrefix, EndKey: tablePrefix.PrefixEnd(),
	}

	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2,
	}
	for retrier := retry.StartWithCtx(ctx, retryOpts); retrier.Next(); {
		started := crtime.NowMono()
		err := r.reconcileOnce(ctx, tableSpan)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}
		log.Dev.Warningf(ctx,
			"rgreconciler: restarting after error: %v", err)
		if started.Elapsed() > 5*time.Minute {
			retrier.Reset()
		}
	}
	return ctx.Err()
}

// reconcileOnce starts a single watcher lifetime. It returns on any
// Push/Replace error or when ctx is cancelled. The caller is
// responsible for restarting.
func (r *Reconciler) reconcileOnce(ctx context.Context, tableSpan roachpb.Span) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dec := makeRowDecoder()
	updates := make(chan rangefeedcache.Update[*event], 1)

	translateEvent := func(
		ctx context.Context, kv *kvpb.RangeFeedValue,
	) (*event, bool) {
		id, upsert, tombstone, err := dec.decode(r.codec, roachpb.KeyValue{
			Key: kv.Key, Value: kv.Value,
		})
		if err != nil {
			if buildutil.CrdbTestBuild {
				panic(errors.Wrap(err, "rgreconciler: decode failure in test"))
			}
			log.Dev.Warningf(ctx,
				"rgreconciler: skipping undecodable event %v: %v",
				kv.Key, err)
			return nil, false
		}
		return &event{
			upsert:    upsert,
			id:        id,
			tombstone: tombstone,
			ts:        kv.Value.Timestamp,
		}, true
	}

	onUpdate := func(
		ctx context.Context, u rangefeedcache.Update[*event],
	) {
		select {
		case updates <- u:
		case <-ctx.Done():
		}
	}

	c := rangefeedcache.NewWatcher(
		"resource-groups-reconciler",
		r.clock, r.rfFactory,
		bufferSize,
		[]roachpb.Span{tableSpan},
		false, /* withPrevValue */
		true,  /* withRowTSInInitialScan */
		translateEvent,
		onUpdate,
		nil, /* knobs */
	)
	if err := rangefeedcache.Start(ctx, r.stopper, c, nil /* onError */); err != nil {
		return err
	}

	for {
		select {
		case u := <-updates:
			if err := r.processUpdate(ctx, u); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processUpdate handles a single watcher update by pushing changes
// to the host. Returns an error if the push fails.
func (r *Reconciler) processUpdate(
	ctx context.Context, update rangefeedcache.Update[*event],
) error {
	if update.Type == rangefeedcache.CompleteUpdate {
		var ups []*rgpb.ResourceGroupUpsert
		for _, e := range update.Events {
			if !e.tombstone {
				ups = append(ups, e.upsert)
			}
		}
		return r.pusher.Replace(ctx, ups)
	}

	// IncrementalUpdate: dedup by id, keeping the latest event.
	latest := map[int64]*event{}
	for _, e := range update.Events {
		latest[e.id] = e
	}
	if len(latest) == 0 {
		return nil
	}
	var ups []*rgpb.ResourceGroupUpsert
	var dels []*rgpb.ResourceGroupDelete
	for _, e := range latest {
		if e.tombstone {
			dels = append(dels, &rgpb.ResourceGroupDelete{Id: e.id})
		} else {
			ups = append(ups, e.upsert)
		}
	}
	return r.pusher.Push(ctx, ups, dels)
}
