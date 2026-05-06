// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rgreconciler watches a tenant's local system.resource_groups
// table over a rangefeed and forwards changes to the host through a
// resourcegroup.Pusher.
//
// During the rangefeed's initial scan every row is delivered as a
// put-style event; on the CompleteUpdate that closes the scan, the
// reconciler diffs that snapshot against the last-pushed snapshot
// and pushes the delta. This is what catches deletes that happened
// during a rangefeed disconnect.
//
// During steady state, each event is buffered by the watcher and
// handed back as Update.Events on each frontier advance.
package rgreconciler

import (
	"context"

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
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

// Reconcile blocks until ctx is cancelled or the rangefeed
// permanently fails.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	tableID, err := startup.RunIdempotentWithRetryEx(ctx,
		r.stopper.ShouldQuiesce(),
		"resource group reconciler table id lookup",
		func(ctx context.Context) (uint32, error) {
			id, err := r.resolver.LookupSystemTableID(ctx, systemschema.ResourceGroupsTable.GetName())
			return uint32(id), err
		})
	if err != nil {
		return err
	}
	tablePrefix := r.codec.IndexPrefix(tableID, 1)
	tableSpan := roachpb.Span{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}

	dec := makeRowDecoder()
	// sent is the set of upserts most recently durably pushed to the
	// host, keyed by id. Used to compute deletes after a rangefeed
	// restart, which would otherwise miss any rows deleted during the
	// disconnect.
	sent := map[int64]*rgpb.ResourceGroupUpsert{}
	// pending holds the latest event per id from incremental updates
	// that haven't yet been successfully pushed. Carrying state across
	// onUpdate calls lets us retry on Push failure without losing
	// events (the watcher's buffer is drained on each frontier
	// advance), and dedups multiple events for the same id within or
	// across batches so the writer sees at most one op per id per
	// Push (which also fixes within-batch ordering, e.g. delete
	// followed by upsert of the same id).
	var pending map[int64]*event

	translateEvent := func(ctx context.Context, kv *kvpb.RangeFeedValue) (*event, bool) {
		id, upsert, tombstone, err := dec.decode(r.codec, roachpb.KeyValue{
			Key: kv.Key, Value: kv.Value,
		})
		if err != nil {
			if buildutil.CrdbTestBuild {
				panic(errors.Wrap(err, "rgreconciler: decode failure in test"))
			}
			log.Dev.Warningf(ctx, "rgreconciler: skipping undecodable event %v: %v", kv.Key, err)
			return nil, false
		}
		return &event{
			upsert:    upsert,
			id:        id,
			tombstone: tombstone,
			ts:        kv.Value.Timestamp,
		}, true
	}

	onUpdate := func(ctx context.Context, update rangefeedcache.Update[*event]) {
		if update.Type == rangefeedcache.CompleteUpdate {
			snap := map[int64]*rgpb.ResourceGroupUpsert{}
			for _, e := range update.Events {
				if !e.tombstone {
					snap[e.id] = e.upsert
				}
			}
			var ups []*rgpb.ResourceGroupUpsert
			for id, u := range snap {
				if prev, ok := sent[id]; !ok || !equalUpsert(prev, u) {
					ups = append(ups, u)
				}
			}
			var dels []*rgpb.ResourceGroupDelete
			for id := range sent {
				if _, ok := snap[id]; !ok {
					dels = append(dels, &rgpb.ResourceGroupDelete{Id: id})
				}
			}
			// The snapshot supersedes anything still pending from
			// before the disconnect.
			pending = nil
			if err := r.pusher.Push(ctx, ups, dels); err != nil {
				log.Dev.Warningf(ctx, "rgreconciler: push after initial scan failed: %v", err)
				return
			}
			sent = snap
			return
		}
		// IncrementalUpdate: merge the buffered batch into pending,
		// keeping the latest event per id, then push pending.
		for _, e := range update.Events {
			if pending == nil {
				pending = map[int64]*event{}
			}
			pending[e.id] = e
		}
		if len(pending) == 0 {
			return
		}
		var ups []*rgpb.ResourceGroupUpsert
		var dels []*rgpb.ResourceGroupDelete
		for _, e := range pending {
			if e.tombstone {
				dels = append(dels, &rgpb.ResourceGroupDelete{Id: e.id})
			} else {
				ups = append(ups, e.upsert)
			}
		}
		if err := r.pusher.Push(ctx, ups, dels); err != nil {
			// Retain pending: the next onUpdate will retry. Without
			// this, events drained from the watcher's buffer would
			// be lost on transient host errors.
			log.Dev.Warningf(ctx, "rgreconciler: incremental push failed: %v", err)
			return
		}
		for _, e := range pending {
			if e.tombstone {
				delete(sent, e.id)
			} else {
				sent[e.id] = e.upsert
			}
		}
		pending = nil
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
	// onError is nil: the watcher already logs and retries with backoff,
	// its internal buffer resets on each Run, and our only piece of
	// cross-restart state (sent) is supposed to survive — the next
	// CompleteUpdate's diff against it is how we recover dropped
	// deletes.
	if err := rangefeedcache.Start(ctx, r.stopper, c, nil /* onError */); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func equalUpsert(a, b *rgpb.ResourceGroupUpsert) bool {
	if a.Name != b.Name {
		return false
	}
	return a.Config.Equal(&b.Config)
}
