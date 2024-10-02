// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantsettingswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Watcher is used to monitor the tenant_setting_table, allow access to the
// current values, and allow listening for changes.
//
// Sample usage:
//
//	w := tenantsettingswatcher.New(...)
//	if err := w.Start(ctx); err != nil { ... }
//
//	// Get overrides and keep them up to date.
//	all, allCh := w.GetAllTenantOverrides()
//	tenant, tenantCh := w.GetTenantOverrides(ctx,tenantID)
//	select {
//	case <-allCh:
//	  all, allCh = w.GetAllTenantOverrides()
//	case <-tenantCh:
//	  tenant, tenantCh = w.GetTenantOverrides(ctx,tenantID)
//	case <-ctx.Done():
//	  ...
//	}
type Watcher struct {
	clock   *hlc.Clock
	f       *rangefeed.Factory
	stopper *stop.Stopper
	st      *cluster.Settings
	dec     RowDecoder
	store   overridesStore

	// startCh is closed once the rangefeed starts.
	startCh  chan struct{}
	startErr error

	// rfc provides access to the underlying rangefeedcache.Watcher for
	// testing.
	rfc *rangefeedcache.Watcher[rangefeedbuffer.Event]
	mu  struct {
		syncutil.Mutex
		// Used by TestingRestart.
		updateWait chan struct{}
	}
}

// New constructs a new Watcher.
func New(
	clock *hlc.Clock, f *rangefeed.Factory, stopper *stop.Stopper, st *cluster.Settings,
) *Watcher {
	w := &Watcher{
		clock:   clock,
		f:       f,
		stopper: stopper,
		st:      st,
		dec:     MakeRowDecoder(),
	}
	w.store.Init()
	w.mu.updateWait = make(chan struct{})
	return w
}

// Start will start the Watcher.
//
// This function sets up the rangefeed and waits for the initial scan. An error
// will be returned if the initial table scan hits an error, the context is
// canceled or the stopper is stopped prior to the initial data being retrieved.
func (w *Watcher) Start(ctx context.Context, sysTableResolver catalog.SystemTableIDResolver) error {
	w.startCh = make(chan struct{})
	defer close(w.startCh)
	w.startErr = w.startRangeFeed(ctx, sysTableResolver)
	return w.startErr
}

// startRangeFeed starts the range feed and waits for the initial table scan. An
// error will be returned if the initial table scan hits an error, the context
// is canceled or the stopper is stopped prior to the initial data being
// retrieved.
func (w *Watcher) startRangeFeed(
	ctx context.Context, sysTableResolver catalog.SystemTableIDResolver,
) error {
	// We need to retry unavailable replicas here. This is only meant to be called
	// at server startup.
	tableID, err := startup.RunIdempotentWithRetryEx(ctx,
		w.stopper.ShouldQuiesce(),
		"tenant start setting rangefeed",
		func(ctx context.Context) (descpb.ID, error) {
			return sysTableResolver.LookupSystemTableID(ctx, systemschema.TenantSettingsTable.GetName())
		})
	if err != nil {
		return err
	}
	tenantSettingsTablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
	tenantSettingsTableSpan := roachpb.Span{
		Key:    tenantSettingsTablePrefix,
		EndKey: tenantSettingsTablePrefix.PrefixEnd(),
	}

	var initialScan = struct {
		ch   chan struct{}
		done bool
		err  error
	}{
		ch: make(chan struct{}),
	}

	allOverrides := make(map[roachpb.TenantID][]kvpb.TenantSetting)

	translateEvent := func(ctx context.Context, kv *kvpb.RangeFeedValue) (rangefeedbuffer.Event, bool) {
		tenantID, setting, tombstone, err := w.dec.DecodeRow(roachpb.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
		if err != nil {
			log.Warningf(ctx, "failed to decode settings row %v: %v", kv.Key, err)
			return nil, false
		}
		if allOverrides != nil {
			// We are in the process of doing a full table scan
			if tombstone {
				log.Warning(ctx, "unexpected empty value during rangefeed scan")
				return nil, false
			}
			allOverrides[tenantID] = append(allOverrides[tenantID], setting)
		} else {
			// We are processing incremental changes.
			w.store.setTenantOverride(ctx, tenantID, setting)
		}
		return nil, false
	}

	onUpdate := func(ctx context.Context, update rangefeedcache.Update[rangefeedbuffer.Event]) {
		if update.Type == rangefeedcache.CompleteUpdate {
			// The CompleteUpdate indicates that the table scan is complete.
			// Henceforth, all calls to translateEvent will be incremental changes,
			// until we hit an error and have to restart the rangefeed.
			w.store.setAll(ctx, allOverrides)
			allOverrides = nil

			if !initialScan.done {
				initialScan.done = true
				close(initialScan.ch)
			}
			// Used by TestingRestart().
			w.mu.Lock()
			defer w.mu.Unlock()
			close(w.mu.updateWait)
			w.mu.updateWait = make(chan struct{})
		}
	}

	onError := func(err error) {
		if !initialScan.done {
			initialScan.err = err
			initialScan.done = true
			close(initialScan.ch)
		} else {
			// The rangefeed will be restarted and will scan the table anew.
			allOverrides = make(map[roachpb.TenantID][]kvpb.TenantSetting)
		}
	}

	c := rangefeedcache.NewWatcher(
		"tenant-settings-watcher",
		w.clock, w.f,
		0, /* bufferSize */
		[]roachpb.Span{tenantSettingsTableSpan},
		false, /* withPrevValue */
		true,  /* withRowTSInInitialScan */
		translateEvent,
		onUpdate,
		nil, /* knobs */
	)
	w.rfc = c

	// Kick off the rangefeedcache which will retry until the stopper stops.
	if err := rangefeedcache.Start(ctx, w.stopper, c, onError); err != nil {
		return err // we're shutting down
	}

	// Wait for the initial scan before returning.
	select {
	case <-initialScan.ch:
		return initialScan.err

	case <-w.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable, "failed to retrieve initial tenant settings")

	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "failed to retrieve initial tenant settings")
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

// TestingRestart restarts the rangefeeds and waits for the initial
// update after the rangefeed update to be processed.
func (w *Watcher) TestingRestart() {
	if w.rfc != nil {
		w.mu.Lock()
		waitCh := w.mu.updateWait
		w.mu.Unlock()
		w.rfc.TestingRestart()
		<-waitCh
	}
}

// GetTenantOverrides returns the current overrides for the given tenant, and a
// channel that will be closed when the overrides for this tenant change.
//
// The caller must not modify the returned overrides slice.
func (w *Watcher) GetTenantOverrides(
	ctx context.Context, tenantID roachpb.TenantID,
) (overrides []kvpb.TenantSetting, changeCh <-chan struct{}) {
	o := w.store.getTenantOverrides(ctx, tenantID)
	return o.overrides, o.changeCh
}

// GetAllTenantOverrides returns the current overrides for all tenants.
// Each of these overrides apply to any tenant, as long as that tenant doesn't
// have an override for the same setting.
//
// The caller must not modify the returned overrides slice.
func (w *Watcher) GetAllTenantOverrides(
	ctx context.Context,
) (overrides []kvpb.TenantSetting, changeCh <-chan struct{}) {
	return w.GetTenantOverrides(ctx, allTenantOverridesID)
}

// SetAternateDefault configures a custom default value
// for a setting when there is no stored value picked up
// from system.tenant_settings.
//
// The second argument must be sorted by setting key already.
//
// At the time of this writing, this is used for SystemVisible
// settings, so that the values from the system tenant's
// system.settings table are used when there is no override
// in .tenant_settings.
func (w *Watcher) SetAlternateDefaults(ctx context.Context, payloads []kvpb.TenantSetting) {
	w.store.setAlternateDefaults(ctx, payloads)
}
