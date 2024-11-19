// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitieswatcher

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Watcher is a concrete implementation of the tenantcapabilities.Watcher
// interface.
type Watcher struct {
	clock            *hlc.Clock
	st               *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	stopper          *stop.Stopper
	decoder          *decoder
	bufferMemLimit   int64

	tenantsTableID uint32 // overriden for tests
	knobs          TestingKnobs

	mu struct {
		syncutil.RWMutex

		store      map[roachpb.TenantID]*watcherEntry
		byName     map[roachpb.TenantName]roachpb.TenantID
		allTenants []tenantcapabilities.Entry

		// lastUpdate is the timestamp of the last update.
		lastUpdate map[roachpb.TenantID]hlc.Timestamp

		// anyChangeCh is closed on any change to the set of
		// tenants.
		anyChangeCh chan struct{}
	}

	// startCh is closed once the rangefeed starts.
	startCh  chan struct{}
	startErr error

	// rfc provides access to the underlying
	// rangefeedcache.Watcher for testing.
	rfc *rangefeedcache.Watcher[rangefeedbuffer.Event]

	// initialScan is used to synchronize the Start() method with the
	// reception of the initial batch of values from the rangefeed
	// (which happens asynchronously).
	initialScan struct {
		err  error
		done bool
		ch   chan struct{}
	}
}

type watcherEntry struct {
	// Entry is the actual tenant metadata.
	*tenantcapabilities.Entry

	// changeCh is closed whenever the entry is updated or deleted.
	changeCh chan struct{}
}

var _ tenantcapabilities.Reader = &Watcher{}

// New constructs a new Watcher.
func New(
	clock *hlc.Clock,
	st *cluster.Settings,
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
		st:               st,
		rangeFeedFactory: rangeFeedFactory,
		stopper:          stopper,
		decoder:          newDecoder(st),
		tenantsTableID:   tenantsTableID,
		bufferMemLimit:   bufferMemLimit,
		knobs:            watcherKnobs,
	}
	w.initialScan.ch = make(chan struct{})
	w.mu.lastUpdate = make(map[roachpb.TenantID]hlc.Timestamp)
	w.mu.store = make(map[roachpb.TenantID]*watcherEntry)
	w.mu.byName = make(map[roachpb.TenantName]roachpb.TenantID)
	w.mu.anyChangeCh = make(chan struct{})
	return w
}

// getInternal returns the internal entry for the given tenant ID.
func (w *Watcher) getInternal(tenantID roachpb.TenantID) *watcherEntry {
	w.mu.RLock()
	cp, found := w.mu.store[tenantID]
	w.mu.RUnlock()
	if found {
		return cp
	}
	// If we did not find it, initialize an empty structure. This is
	// necessary because the caller of GetInfo wants to register as a
	// listener on the change channel, and we need to return a valid
	// channel that will get closed later when we actually find
	// data for this tenant.
	w.mu.Lock()
	defer w.mu.Unlock()
	// Check again though, maybe it was initialized just now.
	if cp, found = w.mu.store[tenantID]; found {
		return cp
	}

	cp = &watcherEntry{
		Entry:    nil,
		changeCh: make(chan struct{}),
	}
	w.mu.store[tenantID] = cp
	return cp
}

// GetInfo reads the non-capability fields from the tenant entry.
// TODO(knz): GetInfo and GetCapabilities should probably be combined.
func (w *Watcher) GetInfo(id roachpb.TenantID) (tenantcapabilities.Entry, <-chan struct{}, bool) {
	cp := w.getInternal(id)

	if cp.Entry != nil {
		return *cp.Entry, cp.changeCh, true
	}
	return tenantcapabilities.Entry{}, cp.changeCh, false
}

// GetCapabilities implements the tenantcapabilities.Reader interface.
func (w *Watcher) GetCapabilities(
	id roachpb.TenantID,
) (*tenantcapabilitiespb.TenantCapabilities, bool) {
	cp := w.getInternal(id)
	if cp.Entry != nil {
		return cp.TenantCapabilities, true
	}
	return nil, false
}

// GetAllTenants returns all known tenant entries and a channel that
// is closed if any of the tenants change or any tenants are added or
// removed.
//
// TODO(ssd): Memory ownership could be a bit cleaner here. To avoid
// needlessly allocating the returned slice every time this is called,
// we return the same slice to all callers and assume they don't do
// anything problematic with it. At the moment, there is only one
// caller of this function so this isn't particularly problematic, but
// it could be if we add more callers.
func (w *Watcher) GetAllTenants() ([]tenantcapabilities.Entry, <-chan struct{}) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.mu.allTenants, w.mu.anyChangeCh
}

// GetGlobalCapabilityState implements the tenantcapabilities.Reader interface.
func (w *Watcher) GetGlobalCapabilityState() map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities, len(w.mu.store))
	for tenID, cp := range w.mu.store {
		if cp.Entry == nil {
			continue
		}
		result[tenID] = cp.TenantCapabilities
	}
	return result
}

// tenantInfoEntrySize is an estimate for a tenant table row that the
// rangefeed buffer tracks. This is extremely conservative for now,
// given the info field is still small. We should re-evaluate this
// constant as that changes.
const tenantInfoEntrySize = 200 // bytes

// Start implements the tenantcapabilities.Watcher interface.
// Start asycnhronously establishes a rangefeed over the global tenant
// capability state.
func (w *Watcher) Start(ctx context.Context) error {
	w.startCh = make(chan struct{})
	defer close(w.startCh)
	w.startErr = w.startRangeFeed(ctx)
	return w.startErr
}

func (w *Watcher) startRangeFeed(ctx context.Context) error {
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
		"tenant-entry-watcher",
		w.clock,
		w.rangeFeedFactory,
		int(w.bufferMemLimit/tenantInfoEntrySize), /* bufferSize */
		[]roachpb.Span{tenantsTableSpan},
		true,  /* withPrevValue */
		false, /* withRowTSInInitialScan */
		w.handleIncrementalUpdate,
		w.handleRangefeedCacheEvent,
		rfcTestingKnobs,
	)
	if err := rangefeedcache.Start(ctx, w.stopper, rfc, w.onError); err != nil {
		return err
	}
	w.rfc = rfc

	// Wait for the initial scan before returning.
	select {
	case <-w.initialScan.ch:
		return w.initialScan.err

	case <-w.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable, "failed to retrieve initial tenant state")

	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "failed to retrieve initial tenant state")
	}
}

// WaitForStart waits until the rangefeed is set up. Returns an error if the
// rangefeed setup failed.
func (w *Watcher) WaitForStart(ctx context.Context) error {
	if w.startCh == nil {
		return errors.AssertionFailedf("Start() was not yet called")
	}
	select {
	case <-w.startCh:
		return w.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Watcher) onError(err error) {
	if !w.initialScan.done {
		w.initialScan.err = err
		w.initialScan.done = true
		close(w.initialScan.ch)
	}
}

func (w *Watcher) handleRangefeedCacheEvent(
	ctx context.Context, u rangefeedcache.Update[rangefeedbuffer.Event],
) {
	switch u.Type {
	case rangefeedcache.CompleteUpdate:
		log.Info(ctx, "received results of a full table scan for tenant capabilities")
		if !w.initialScan.done {
			w.initialScan.done = true
			close(w.initialScan.ch)
		} else {
			// If initial scan was already done, this is
			// the result of a rangefeed restart. We may
			// need to clean up deleted data.
			//
			// We configure our rangefeed to emit scan
			// events at their read timestamp. If the last
			// timestamp we have saved is below the scan
			// timestamp, then that item can't have been
			// included in the scan and must have been
			// deleted.
			w.removeEntriesBeforeTimestamp(ctx, u.Timestamp)
		}
	case rangefeedcache.IncrementalUpdate:
		// We ignore incremental updates because we handle
		// them in the translate event callback to provide
		// faster cache updates.
	default:
		err := errors.AssertionFailedf("unknown update type: %v", u.Type)
		logcrash.ReportOrPanic(ctx, &w.st.SV, "%w", err)
	}
}

func (w *Watcher) onAnyChangeLocked() {
	entries := make([]tenantcapabilities.Entry, 0, len(w.mu.store))
	for _, v := range w.mu.store {
		if v.Entry != nil {
			entries = append(entries, *v.Entry)
		}
	}
	w.mu.allTenants = entries

	close(w.mu.anyChangeCh)
	w.mu.anyChangeCh = make(chan struct{})
}

// handleIncrementalUpdate is used as our rangefeedcache.Watcher
// translate event callback. It is called for every rangefeed value
// returned from the rangefeed or an initial scan. This allows us to
// update our cache immediately rather than waiting for the closed
// timestamp.
//
// It always returns nil since the event is already handled after the
// callback.
func (w *Watcher) handleIncrementalUpdate(
	ctx context.Context, ev *kvpb.RangeFeedValue,
) (rangefeedbuffer.Event, bool) {
	hasEvent, update := w.decoder.translateEvent(ctx, ev)
	if !hasEvent {
		return nil, false
	}

	if fn := w.knobs.WatcherUpdatesInterceptor; fn != nil {
		fn(update)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	tid := update.TenantID
	ts := update.Timestamp
	if prevTs, ok := w.mu.lastUpdate[tid]; ok && ts.Less(prevTs) {
		// Skip updates which have an earlier timestamp to avoid
		// regressing on the contents of an entry.
		return nil, false
	}
	w.mu.lastUpdate[tid] = ts

	if entry := w.mu.store[tid]; entry != nil {
		// Notify previous watchers, if any, that the entry is getting updated.
		close(entry.changeCh)
	}

	if update.Deleted {
		w.removeEntryForTenantIDLocked(ctx, tid)
	} else {
		if log.V(2) {
			log.Infof(ctx, "adding tenant entry to cache: %+v", update.Entry)
		}
		w.mu.store[update.TenantID] = &watcherEntry{
			Entry:    &update.Entry,
			changeCh: make(chan struct{}),
		}
		if update.Entry.Name != "" {
			w.mu.byName[update.Entry.Name] = update.TenantID
		}
	}
	w.onAnyChangeLocked()
	return nil, false
}

func (w *Watcher) removeEntriesBeforeTimestamp(ctx context.Context, ts hlc.Timestamp) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var anyDeleted bool
	for tid, prevTs := range w.mu.lastUpdate {
		if prevTs.Less(ts) {
			anyDeleted = true
			w.removeEntryForTenantIDLocked(ctx, tid)
		}
	}
	if anyDeleted {
		w.onAnyChangeLocked()
	}
}

func (w *Watcher) removeEntryForTenantIDLocked(ctx context.Context, tid roachpb.TenantID) {
	if log.V(2) {
		log.Infof(ctx, "removing tenant entry %d from cache", tid)
	}
	// Not finding the ID in the store is expected if we see a
	// duplicate delete.
	if entry, ok := w.mu.store[tid]; ok {
		delete(w.mu.byName, entry.Name)
	}
	delete(w.mu.lastUpdate, tid)
	delete(w.mu.store, tid)
}

func (w *Watcher) TestingRestart() {
	if w.rfc != nil {
		w.rfc.TestingRestart()
	}
}

// TestingFlushCapabilitiesState flushes the underlying global tenant capability
// state for testing purposes. The returned entries are sorted by tenant ID.
func (w *Watcher) TestingFlushCapabilitiesState() (entries []tenantcapabilities.Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range w.mu.store {
		if entry.Entry == nil {
			continue
		}
		entries = append(entries, *entry.Entry)
	}

	// Sort entries by tenant ID before returning, to ensure the return value of
	// this function is stable. This is useful for things like data-driven tests.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TenantID.ToUint64() < entries[j].TenantID.ToUint64()
	})
	return entries
}
