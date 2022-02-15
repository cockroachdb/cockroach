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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Watcher is used to monitor the tenant_setting_table, allow access to the
// current values, and allow listening for changes.
//
// Sample usage:
//
//   w := tenantsettingswatcher.New(...)
//   if err := w.Start(ctx); err != nil { ... }
//
//   // Get overrides and keep them up to date.
//   all, allCh := w.AllTenantOverrides()
//   tenant, tenantCh := w.TenantOverrides(tenantID)
//   select {
//   case <-allCh:
//     all, allCh = w.AllTenantOverrides()
//   case <-tenantCh:
//     tenant, tenantCh = w.TenantOverrides(tenantID)
//   case <-ctx.Done():
//     ...
//   }
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
// If the current cluster version indicates that we have a tenant settings
// table, this function sets up the rangefeed and waits for the initial scan. An
// error will be returned if the initial table scan hits an error, the context
// is canceled or the stopper is stopped prior to the initial data being
// retrieved.
//
// Otherwise, Start sets up a background task that waits for the right version
// and starts the rangefeed when appropriate. WaitUntilStarted can be used to
// wait for the rangefeed setup.
func (w *Watcher) Start(ctx context.Context, sysTableResolver catalog.SystemTableIDResolver) error {
	w.startCh = make(chan struct{})
	if w.st.Version.IsActive(ctx, clusterversion.TenantSettingsTable) {
		// We are not in a mixed-version scenario; start the rangefeed now.
		w.startErr = w.startRangeFeed(ctx, sysTableResolver)
		close(w.startCh)
		return w.startErr
	}
	// Set up an on-change callback that closes this channel once the version
	// supports tenant settings.
	versionOkCh := make(chan struct{})
	var once sync.Once
	w.st.Version.SetOnChange(func(ctx context.Context, newVersion clusterversion.ClusterVersion) {
		if newVersion.IsActive(clusterversion.TenantSettingsTable) {
			once.Do(func() {
				close(versionOkCh)
			})
		}
	})
	// Now check the version again, in case the version changed just before
	// SetOnChange.
	if w.st.Version.IsActive(ctx, clusterversion.TenantSettingsTable) {
		w.startErr = w.startRangeFeed(ctx, sysTableResolver)
		close(w.startCh)
		return w.startErr
	}
	return w.stopper.RunAsyncTask(ctx, "tenantsettingswatcher-start", func(ctx context.Context) {
		log.Infof(ctx, "tenantsettingswatcher waiting for the appropriate version")
		select {
		case <-versionOkCh:
		case <-w.stopper.ShouldQuiesce():
			return
		}
		log.Infof(ctx, "tenantsettingswatcher can now start")
		w.startErr = w.startRangeFeed(ctx, sysTableResolver)
		if w.startErr != nil {
			// We are not equipped to handle this error asynchronously.
			log.Warningf(ctx, "error starting tenantsettingswatcher rangefeed: %v", w.startErr)
		}
		close(w.startCh)
	})
}

// startRangeFeed starts the range feed and waits for the initial table scan. An
// error will be returned if the initial table scan hits an error, the context
// is canceled or the stopper is stopped prior to the initial data being
// retrieved.
func (w *Watcher) startRangeFeed(
	ctx context.Context, sysTableResolver catalog.SystemTableIDResolver,
) error {
	tableID, err := sysTableResolver.LookupSystemTableID(ctx, systemschema.TenantSettingsTable.GetName())
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

	allOverrides := make(map[roachpb.TenantID][]roachpb.TenantSetting)

	translateEvent := func(ctx context.Context, kv *roachpb.RangeFeedValue) rangefeedbuffer.Event {
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
			allOverrides = make(map[roachpb.TenantID][]roachpb.TenantSetting)
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
//
// If the cluster version does not support tenant settings, returns immediately
// with no error. Note that it is still legal to call GetTenantOverrides and
// GetAllTenantOverrides in this state. When the cluster version is upgraded,
// the settings will start being updated.
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
	if !w.st.Version.IsActive(ctx, clusterversion.TenantSettingsTable) {
		// If this happens, then we are running new tenant code against a host
		// cluster that was not fully upgraded.
		log.Warningf(ctx, "tenant requested settings before host cluster version upgrade")
		return nil
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
) (overrides []roachpb.TenantSetting, changeCh <-chan struct{}) {
	o := w.store.GetTenantOverrides(tenantID)
	return o.overrides, o.changeCh
}

// GetAllTenantOverrides returns the current overrides for all tenants.
// Each of these overrides apply to any tenant, as long as that tenant doesn't
// have an override for the same setting.
//
// The caller must not modify the returned overrides slice.
func (w *Watcher) GetAllTenantOverrides() (
	overrides []roachpb.TenantSetting,
	changeCh <-chan struct{},
) {
	return w.GetTenantOverrides(allTenantOverridesID)
}
