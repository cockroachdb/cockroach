// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitieswatcher

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Watcher is a concrete implementation of the tenantcapabilities.Watcher
// interface.
type Watcher struct {
	clock            *hlc.Clock
	rangeFeedFactory *rangefeed.Factory
	stopper          *stop.Stopper
	decoder          *decoder
	bufferMemLimit   int64

	tenantsTableID uint32 // overriden for tests
	knobs          TestingKnobs

	mu struct {
		syncutil.RWMutex

		store map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities
	}
}

var _ tenantcapabilities.Reader = &Watcher{}

// New constructs a new Watcher.
func New(
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	tenantsTableID uint32,
	stopper *stop.Stopper,
	bufferMemLimit int64,
	knobs *tenantcapabilities.TestingKnobs,
) *Watcher {
	watcherKnobs := TestingKnobs{}
	if knobs != nil && knobs.WatcherTestingKnobs != nil {
		watcherKnobs = *knobs.WatcherTestingKnobs.(*TestingKnobs)
	}
	w := &Watcher{
		clock:            clock,
		rangeFeedFactory: rangeFeedFactory,
		stopper:          stopper,
		decoder:          newDecoder(),
		tenantsTableID:   tenantsTableID,
		bufferMemLimit:   bufferMemLimit,
		knobs:            watcherKnobs,
	}
	w.mu.store = make(map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities)
	return w
}

// GetCapabilities implements the tenantcapabilities.Reader interface.
func (w *Watcher) GetCapabilities(
	id roachpb.TenantID,
) (*tenantcapabilitiespb.TenantCapabilities, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	cp, found := w.mu.store[id]
	return cp, found
}

// GetGlobalCapabilityState implements the tenantcapabilities.Reader interface.
func (w *Watcher) GetGlobalCapabilityState() map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities, len(w.mu.store))
	for tenID, cp := range w.mu.store {
		result[tenID] = cp
	}
	return result
}

// capabilityEntrySize is an estimate for a (tenantID, capability) pair that the
// rangefeed buffer tracks. This is extremely conservative for now, given we
// don't have too many capabilities in the system. We should re-evaluate this
// constant as that changes.
const capabilityEntrySize = 50 // bytes

// Start implements the tenantcapabilities.Watcher interface.
// Start asycnhronously establishes a rangefeed over the global tenant
// capability state.
func (w *Watcher) Start(ctx context.Context) error {
	tenantsTableStart := keys.SystemSQLCodec.IndexPrefix(
		w.tenantsTableID, keys.TenantsTablePrimaryKeyIndexID,
	)
	tenantsTableSpan := roachpb.Span{
		Key:    tenantsTableStart,
		EndKey: tenantsTableStart.PrefixEnd(),
	}

	var rfcTestingKnobs *rangefeedcache.TestingKnobs
	if w.knobs.WatcherRangeFeedKnobs != nil {
		rfcTestingKnobs = w.knobs.WatcherRangeFeedKnobs.(*rangefeedcache.TestingKnobs)
	}
	rfc := rangefeedcache.NewWatcher(
		"tenant-capability-watcher",
		w.clock,
		w.rangeFeedFactory,
		int(w.bufferMemLimit/capabilityEntrySize), /* bufferSize */
		[]roachpb.Span{tenantsTableSpan},
		true, /* withPrevValue */
		w.decoder.translateEvent,
		w.handleUpdate,
		rfcTestingKnobs,
	)
	return rangefeedcache.Start(ctx, w.stopper, rfc, nil /* onError */)
}

func (w *Watcher) handleUpdate(ctx context.Context, u rangefeedcache.Update) {
	var updates []tenantcapabilities.Update
	for _, ev := range u.Events {
		update := ev.(*bufferEvent).update
		updates = append(updates, update)
	}

	if fn := w.knobs.WatcherUpdatesInterceptor; fn != nil {
		fn(u.Type, updates)
	}

	switch u.Type {
	case rangefeedcache.CompleteUpdate:
		log.Info(ctx, "received results of a full table scan for tenant capabilities")
		w.handleCompleteUpdate(updates)
	case rangefeedcache.IncrementalUpdate:
		w.handleIncrementalUpdate(updates)
	default:
		log.Fatalf(ctx, "unknown update type: %v", u.Type)
	}
}

func (w *Watcher) handleCompleteUpdate(updates []tenantcapabilities.Update) {
	// Populate a fresh store with the supplied updates.
	// A Complete update indicates that the initial table scan is complete. This
	// happens when the rangefeed is first established, or if it's restarted for
	// some reason. Either way, we want to throw away any accumulated state so
	// far, and reconstruct it using the result of the scan.
	freshStore := make(map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities)
	for _, up := range updates {
		freshStore[up.TenantID] = up.TenantCapabilities
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.store = freshStore
}

func (w *Watcher) handleIncrementalUpdate(updates []tenantcapabilities.Update) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, update := range updates {
		if update.Deleted {
			delete(w.mu.store, update.TenantID)
		} else {
			w.mu.store[update.TenantID] = update.TenantCapabilities
		}
	}
}

// TestingFlushCapabilitiesState flushes the underlying global tenant capability
// state for testing purposes. The returned entries are sorted by tenant ID.
func (w *Watcher) TestingFlushCapabilitiesState() (entries []tenantcapabilities.Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for id, capability := range w.mu.store {
		entries = append(entries, tenantcapabilities.Entry{
			TenantID:           id,
			TenantCapabilities: capability,
		})
	}

	// Sort entries by tenant ID before returning, to ensure the return value of
	// this function is stable. This is useful for things like data-driven tests.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TenantID.ToUint64() < entries[j].TenantID.ToUint64()
	})
	return entries
}

type bufferEvent struct {
	update tenantcapabilities.Update
	ts     hlc.Timestamp
}

func (e *bufferEvent) Timestamp() hlc.Timestamp {
	return e.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
