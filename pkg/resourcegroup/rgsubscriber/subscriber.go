// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rgsubscriber owns the host-side in-memory cache of resource
// group configurations across all tenants. The cache is driven by a
// rangefeed on system.tenant_resource_groups; admission control
// queries it through the Reader interface to attribute and shape work
// per (tenantID, groupID).
package rgsubscriber

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// bufferSize bounds how many in-flight rangefeed events may be queued
// before the next frontier advance. Resource groups are O(small per
// tenant) so this is generous.
const bufferSize = 4096

// ConfigMap is the shape of the cache returned by Snapshot: an
// immutable map keyed first by tenant ID and then by group ID. The
// outer and inner maps and the *Config values they contain must not
// be mutated by callers.
type ConfigMap = map[roachpb.TenantID]map[int64]*admissionpb.ResourceGroupConfig

// Reader is the lookup surface AC depends on.
type Reader interface {
	// Get returns the config for (tenantID, id) and whether it was
	// found. The returned pointer must not be mutated by the caller.
	Get(tenantID roachpb.TenantID, id int64) (*admissionpb.ResourceGroupConfig, bool)
	// Snapshot returns the entire current configuration set as a
	// shared, immutable view.
	Snapshot() ConfigMap
	// LastUpdated returns the wall-clock time at which the most recent
	// rangefeed update completed, and false if no update has been
	// processed yet.
	LastUpdated() (time.Time, bool)
}

// Subscriber is the host-side cache.
type Subscriber struct {
	settings  *cluster.Settings
	clock     *hlc.Clock
	rfFactory *rangefeed.Factory
	stopper   *stop.Stopper

	snap atomic.Pointer[snapshot]
}

type snapshot struct {
	configs     ConfigMap
	lastUpdated time.Time
}

// event is what the subscriber's translateEvent emits per rangefeed
// KV. The Watcher's buffer collects these and hands them back to
// onUpdate as Update.Events at each frontier advance.
type event struct {
	tenantID  roachpb.TenantID
	id        int64
	cfg       *admissionpb.ResourceGroupConfig
	tombstone bool
	ts        hlc.Timestamp
}

// Timestamp implements rangefeedbuffer.Event.
func (e *event) Timestamp() hlc.Timestamp { return e.ts }

// New constructs a Subscriber. Start must be called before Get or
// Snapshot return useful results.
func New(
	settings *cluster.Settings, clock *hlc.Clock, rfFactory *rangefeed.Factory, stopper *stop.Stopper,
) *Subscriber {
	s := &Subscriber{
		settings:  settings,
		clock:     clock,
		rfFactory: rfFactory,
		stopper:   stopper,
	}
	s.snap.Store(&snapshot{configs: ConfigMap{}})
	return s
}

// Start establishes the rangefeed and waits for the initial scan
// before returning. It first blocks until the cluster version gate
// that creates system.tenant_resource_groups is active.
func (s *Subscriber) Start(ctx context.Context, resolver catalog.SystemTableIDResolver) error {
	if err := resourcegroup.WaitForTenantResourceGroupsTable(ctx, s.settings, s.stopper); err != nil {
		return err
	}
	tableID, err := startup.RunIdempotentWithRetryEx(ctx,
		s.stopper.ShouldQuiesce(),
		"resource group subscriber table id lookup",
		func(ctx context.Context) (uint32, error) {
			id, err := resolver.LookupSystemTableID(ctx, systemschema.TenantResourceGroupsTable.GetName())
			return uint32(id), err
		})
	if err != nil {
		return err
	}
	tablePrefix := keys.SystemSQLCodec.IndexPrefix(tableID, 1)
	tableSpan := roachpb.Span{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}

	dec := makeRowDecoder()

	translateEvent := func(ctx context.Context, kv *kvpb.RangeFeedValue) (*event, bool) {
		tid, id, cfg, tombstone, err := dec.decode(roachpb.KeyValue{Key: kv.Key, Value: kv.Value})
		if err != nil {
			if buildutil.CrdbTestBuild {
				panic(errors.Wrap(err, "rgsubscriber: decode failure in test"))
			}
			log.Dev.Warningf(ctx, "rgsubscriber: skipping undecodable event %v: %v", kv.Key, err)
			return nil, false
		}
		return &event{
			tenantID:  tid,
			id:        id,
			cfg:       cfg,
			tombstone: tombstone,
			ts:        kv.Value.Timestamp,
		}, true
	}

	var initialScanOnce sync.Once
	initialScanDone := make(chan struct{})

	onUpdate := func(ctx context.Context, update rangefeedcache.Update[*event]) {
		now := s.clock.PhysicalTime()
		if update.Type == rangefeedcache.CompleteUpdate {
			// Initial scan: rebuild the snapshot from scratch.
			configs := ConfigMap{}
			for _, e := range update.Events {
				if e.tombstone {
					continue
				}
				groups, ok := configs[e.tenantID]
				if !ok {
					groups = map[int64]*admissionpb.ResourceGroupConfig{}
					configs[e.tenantID] = groups
				}
				groups[e.id] = e.cfg
			}
			s.snap.Store(&snapshot{configs: configs, lastUpdated: now})
			initialScanOnce.Do(func() { close(initialScanDone) })
			return
		}
		// Incremental: apply the buffered batch atomically via copy-on-write.
		if len(update.Events) == 0 {
			cur := s.snap.Load()
			s.snap.Store(&snapshot{configs: cur.configs, lastUpdated: now})
			return
		}
		cur := s.snap.Load()
		next := copyConfigs(cur.configs)
		for _, e := range update.Events {
			applyChange(next, e.tenantID, e.id, e.cfg, e.tombstone)
		}
		s.snap.Store(&snapshot{configs: next, lastUpdated: now})
	}

	c := rangefeedcache.NewWatcher(
		"resource-group-subscriber",
		s.clock, s.rfFactory,
		bufferSize,
		[]roachpb.Span{tableSpan},
		false, /* withPrevValue */
		true,  /* withRowTSInInitialScan */
		translateEvent,
		onUpdate,
		nil, /* knobs */
	)
	// onError is nil: the watcher already logs and retries with backoff,
	// and we hold no cross-restart state — the next CompleteUpdate
	// rebuilds the snapshot from scratch.
	if err := rangefeedcache.Start(ctx, s.stopper, c, nil /* onError */); err != nil {
		return err
	}
	select {
	case <-initialScanDone:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "waiting for initial scan")
	case <-s.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable, "waiting for initial scan")
	}
}

// copyConfigs makes a shallow copy of the outer map. Inner maps are
// not copied because applyChange replaces them on touch, so writes
// in next don't leak into the previous snapshot.
func copyConfigs(in ConfigMap) ConfigMap {
	out := make(ConfigMap, len(in))
	for tid, groups := range in {
		out[tid] = groups
	}
	return out
}

// applyChange mutates next to reflect a single rangefeed event. The
// inner map for tenantID is replaced (cloned then mutated) so the
// previous snapshot's view of that tenant is unaffected.
func applyChange(
	next ConfigMap,
	tenantID roachpb.TenantID,
	id int64,
	cfg *admissionpb.ResourceGroupConfig,
	tombstone bool,
) {
	groups := map[int64]*admissionpb.ResourceGroupConfig{}
	if existing, ok := next[tenantID]; ok {
		for k, v := range existing {
			if k == id && tombstone {
				continue
			}
			groups[k] = v
		}
	}
	if !tombstone {
		groups[id] = cfg
	}
	if len(groups) == 0 {
		delete(next, tenantID)
		return
	}
	next[tenantID] = groups
}

// Get implements Reader.
func (s *Subscriber) Get(
	tenantID roachpb.TenantID, id int64,
) (*admissionpb.ResourceGroupConfig, bool) {
	cur := s.snap.Load()
	groups, ok := cur.configs[tenantID]
	if !ok {
		return nil, false
	}
	cfg, ok := groups[id]
	return cfg, ok
}

// Snapshot implements Reader.
func (s *Subscriber) Snapshot() ConfigMap {
	return s.snap.Load().configs
}

// LastUpdated implements Reader.
func (s *Subscriber) LastUpdated() (time.Time, bool) {
	cur := s.snap.Load()
	if cur.lastUpdated.IsZero() {
		return time.Time{}, false
	}
	return cur.lastUpdated, true
}
