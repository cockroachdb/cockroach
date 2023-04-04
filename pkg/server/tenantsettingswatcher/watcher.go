// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
//	all, allCh := w.AllTenantOverrides()
//	tenant, tenantCh := w.TenantOverrides(tenantID)
//	select {
//	case <-allCh:
//	  all, allCh = w.AllTenantOverrides()
//	case <-tenantCh:
//	  tenant, tenantCh = w.TenantOverrides(tenantID)
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
	return w
}

// Start will start the Watcher.
//
// This function sets up the rangefeed and waits for the initial scan. An error
// will be returned if the initial table scan hits an error, the context is
// canceled or the stopper is stopped prior to the initial data being retrieved.
func (w *Watcher) Start(ctx context.Context, sysTableResolver catalog.SystemTableIDResolver) error {
	w.startCh = make(chan struct{})
	w.startErr = w.startRangeFeed(ctx, sysTableResolver)
	close(w.startCh)
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

	translateEvent := func(ctx context.Context, kv *kvpb.RangeFeedValue) rangefeedbuffer.Event {
		tenantID, setting, tombstone, err := w.dec.DecodeRow(roachpb.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
		if err != nil {
			log.Warningf(ctx, "failed to decode settings row %v: %v", kv.Key, err)
			return nil
		}
		if allOverrides != nil {
			// We are in the process of doing a full table scan
			if tombstone {
				log.Warning(ctx, "unexpected empty value during rangefeed scan")
				return nil
			}
			allOverrides[tenantID] = append(allOverrides[tenantID], setting)
		} else {
			// We are processing incremental changes.
			w.store.SetTenantOverride(tenantID, setting)
		}
		return nil
	}

	onUpdate := func(ctx context.Context, update rangefeedcache.Update) {
		if update.Type == rangefeedcache.CompleteUpdate {
			// The CompleteUpdate indicates that the table scan is complete.
			// Henceforth, all calls to translateEvent will be incremental changes,
			// until we hit an error and have to restart the rangefeed.
			w.store.SetAll(allOverrides)
			allOverrides = nil

			if !initialScan.done {
				initialScan.done = true
				close(initialScan.ch)
			}
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
		translateEvent,
		onUpdate,
		nil, /* knobs */
	)

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
	// Fast path check.
	select {
	case <-w.startCh:
		return w.startErr
	default:
	}
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

// GetTenantOverrides returns the current overrides for the given tenant, and a
// channel that will be closed when the overrides for this tenant change.
//
// The caller must not modify the returned overrides slice.
func (w *Watcher) GetTenantOverrides(
	tenantID roachpb.TenantID,
) (overrides []kvpb.TenantSetting, changeCh <-chan struct{}) {
	o := w.store.GetTenantOverrides(tenantID)
	return o.overrides, o.changeCh
}

// GetAllTenantOverrides returns the current overrides for all tenants.
// Each of these overrides apply to any tenant, as long as that tenant doesn't
// have an override for the same setting.
//
// The caller must not modify the returned overrides slice.
func (w *Watcher) GetAllTenantOverrides() (
	overrides []kvpb.TenantSetting,
	changeCh <-chan struct{},
) {
	return w.GetTenantOverrides(allTenantOverridesID)
}
