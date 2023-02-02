// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package settingswatcher provides utilities to update cluster settings using
// a range feed.
package settingswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// SettingsWatcher is used to watch for cluster settings changes with a
// rangefeed.
type SettingsWatcher struct {
	clock    *hlc.Clock
	codec    keys.SQLCodec
	settings *cluster.Settings
	f        *rangefeed.Factory
	stopper  *stop.Stopper
	dec      RowDecoder
	storage  Storage

	overridesMonitor OverridesMonitor

	mu struct {
		syncutil.Mutex

		updater   settings.Updater
		values    map[string]settingsValue
		overrides map[string]settings.EncodedValue
		// storageClusterVersion is the cache of the storage cluster version
		// inside secondary tenants. It will be uninitialized in a system
		// tenant.
		storageClusterVersion clusterversion.ClusterVersion
	}

	// testingWatcherKnobs allows the client to inject testing knobs into
	// the underlying rangefeedcache.Watcher.
	testingWatcherKnobs *rangefeedcache.TestingKnobs
}

// Storage is used to write a snapshot of KVs out to disk for use upon restart.
type Storage interface {
	SnapshotKVs(ctx context.Context, kvs []roachpb.KeyValue)
}

// New constructs a new SettingsWatcher.
func New(
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	storage Storage, // optional
) *SettingsWatcher {
	return &SettingsWatcher{
		clock:    clock,
		codec:    codec,
		settings: settingsToUpdate,
		f:        f,
		stopper:  stopper,
		dec:      MakeRowDecoder(codec),
		storage:  storage,
	}
}

// NewWithOverrides constructs a new SettingsWatcher which allows external
// overrides, discovered through an OverridesMonitor.
func NewWithOverrides(
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	overridesMonitor OverridesMonitor,
	storage Storage,
) *SettingsWatcher {
	s := New(clock, codec, settingsToUpdate, f, stopper, storage)
	s.overridesMonitor = overridesMonitor
	settingsToUpdate.OverridesInformer = s
	return s
}

// Start will start the SettingsWatcher. It returns after the initial settings
// have been retrieved. An error will be returned if the context is canceled or
// the stopper is stopped prior to the initial data being retrieved.
func (s *SettingsWatcher) Start(ctx context.Context) error {
	settingsTablePrefix := s.codec.TablePrefix(keys.SettingsTableID)
	settingsTableSpan := roachpb.Span{
		Key:    settingsTablePrefix,
		EndKey: settingsTablePrefix.PrefixEnd(),
	}
	s.resetUpdater()
	initialScan := struct {
		ch   chan struct{}
		done bool
		err  error
	}{
		ch: make(chan struct{}),
	}
	noteUpdate := func(update rangefeedcache.Update) {
		if update.Type != rangefeedcache.CompleteUpdate {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.updater.ResetRemaining(ctx)
		if !initialScan.done {
			initialScan.done = true
			close(initialScan.ch)
		}
	}

	s.mu.values = make(map[string]settingsValue)

	if s.overridesMonitor != nil {
		s.mu.overrides = make(map[string]settings.EncodedValue) // Initialize the overrides. We want to do this before processing the
		// settings table, otherwise we could see temporary transitions to the value
		// in the table.
		s.updateOverrides(ctx)

		// Set up a worker to watch the monitor.
		if err := s.stopper.RunAsyncTask(ctx, "setting-overrides", func(ctx context.Context) {
			overridesCh := s.overridesMonitor.RegisterOverridesChannel()
			for {
				select {
				case <-overridesCh:
					s.updateOverrides(ctx)

				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		}); err != nil {
			// We are shutting down.
			return err
		}
	}

	// bufferSize configures how large of a buffer to permit for accumulated
	// changes of settings between resolved timestamps. It's an arbitrary
	// number thought ought to be big enough. Note that if there is no underlying
	// storage, we'll never produce any events in s.handleKV() so we can use a
	// bufferSize of 0.
	var bufferSize int
	if s.storage != nil {
		bufferSize = settings.MaxSettings * 3
	}
	var snapshot []roachpb.KeyValue // used with storage
	maybeUpdateSnapshot := func(update rangefeedcache.Update) {
		// Only record the update to the buffer if we're writing to storage.
		if s.storage == nil ||
			// and the update has some new information to write.
			(update.Type == rangefeedcache.IncrementalUpdate && len(update.Events) == 0) {
			return
		}
		eventKVs := rangefeedbuffer.EventsToKVs(update.Events,
			rangefeedbuffer.RangeFeedValueEventToKV)
		switch update.Type {
		case rangefeedcache.CompleteUpdate:
			snapshot = eventKVs
		case rangefeedcache.IncrementalUpdate:
			snapshot = rangefeedbuffer.MergeKVs(snapshot, eventKVs)
		}
		s.storage.SnapshotKVs(ctx, snapshot)
	}
	c := rangefeedcache.NewWatcher(
		"settings-watcher",
		s.clock, s.f,
		bufferSize,
		[]roachpb.Span{settingsTableSpan},
		false, // withPrevValue
		func(ctx context.Context, kv *roachpb.RangeFeedValue) rangefeedbuffer.Event {
			return s.handleKV(ctx, kv)
		},
		func(ctx context.Context, update rangefeedcache.Update) {
			noteUpdate(update)
			maybeUpdateSnapshot(update)
		},
		s.testingWatcherKnobs,
	)

	// Kick off the rangefeedcache which will retry until the stopper stops.
	if err := rangefeedcache.Start(ctx, s.stopper, c, func(err error) {
		if !initialScan.done {
			initialScan.err = err
			initialScan.done = true
			close(initialScan.ch)
		} else {
			s.resetUpdater()
		}
	}); err != nil {
		return err // we're shutting down
	}

	// Wait for the initial scan before returning.
	select {
	case <-initialScan.ch:
		return initialScan.err

	case <-s.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable, "failed to retrieve initial cluster settings")

	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "failed to retrieve initial cluster settings")
	}
}

func (s *SettingsWatcher) handleKV(
	ctx context.Context, kv *roachpb.RangeFeedValue,
) rangefeedbuffer.Event {
	var alloc tree.DatumAlloc
	name, val, tombstone, err := s.dec.DecodeRow(roachpb.KeyValue{
		Key:   kv.Key,
		Value: kv.Value,
	}, &alloc)
	if err != nil {
		log.Warningf(ctx, "failed to decode settings row %v: %v", kv.Key, err)
		return nil
	}

	if !s.codec.ForSystemTenant() {
		setting, ok := settings.Lookup(name, settings.LookupForLocalAccess, s.codec.ForSystemTenant())
		if !ok {
			log.Warningf(ctx, "unknown setting %s, skipping update", redact.Safe(name))
			return nil
		}
		if setting.Class() != settings.TenantWritable {
			log.Warningf(ctx, "ignoring read-only setting %s", redact.Safe(name))
			return nil
		}
	}

	s.maybeSet(ctx, name, settingsValue{
		val:       val,
		ts:        kv.Value.Timestamp,
		tombstone: tombstone,
	})
	if s.storage != nil {
		return kv
	}
	return nil
}

// maybeSet will update the stored value and the corresponding setting
// in response to a kv event, assuming that event is new.
func (s *SettingsWatcher) maybeSet(ctx context.Context, name string, sv settingsValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Skip updates which have an earlier timestamp to avoid regressing on the
	// value of a setting. Note that we intentionally process values at the same
	// timestamp as the current value. This is important to deal with cases where
	// the underlying rangefeed restarts. When that happens, we'll construct a
	// new settings updater and expect to re-process every setting which is
	// currently set.
	if existing, ok := s.mu.values[name]; ok && sv.ts.Less(existing.ts) {
		return
	}
	_, hasOverride := s.mu.overrides[name]
	s.mu.values[name] = sv
	if sv.tombstone {
		// This event corresponds to a deletion.
		if !hasOverride {
			s.setDefaultLocked(ctx, name)
		}
	} else {
		if !hasOverride {
			s.setLocked(ctx, name, sv.val)
		}
	}
}

// settingValue tracks an observed value from the rangefeed. By tracking the
// timestamp, we can avoid regressing the settings values in the face of
// rangefeed restarts.
type settingsValue struct {
	val       settings.EncodedValue
	ts        hlc.Timestamp
	tombstone bool
}

const versionSettingKey = "version"

// set the current value of a setting.
func (s *SettingsWatcher) setLocked(ctx context.Context, key string, val settings.EncodedValue) {
	// The system tenant (i.e. the KV layer) does not use the SettingsWatcher
	// to propagate cluster version changes (it uses the BumpClusterVersion
	// RPC). However, secondary tenants get word of the new cluster version
	// below. This is to handle the case where there are multiple SQL pods
	// working on behalf of the same secondary tenant. If one pod performs the
	// upgrade, we want to make the other SQL pods aware of the fact that the
	// upgrade occurred.
	if key == versionSettingKey && !s.codec.ForSystemTenant() {
		var newVersion clusterversion.ClusterVersion
		oldVersion := s.settings.Version.ActiveVersion(ctx)
		if err := protoutil.Unmarshal([]byte(val.Value), &newVersion); err != nil {
			log.Warningf(ctx, "failed to set cluster version: %s", err.Error())
		} else if err := s.settings.Version.SetActiveVersion(ctx, newVersion); err != nil {
			log.Warningf(ctx, "failed to set cluster version: %s", err.Error())
		} else if newVersion != oldVersion {
			log.Infof(ctx, "set cluster version to: %v", newVersion)
		}
		return
	}

	if err := s.mu.updater.Set(ctx, key, val); err != nil {
		log.Warningf(ctx, "failed to set setting %s to %s: %v", redact.Safe(key), val.Value, err)
	}
}

// setDefaultLocked sets a setting to its default value.
func (s *SettingsWatcher) setDefaultLocked(ctx context.Context, key string) {
	setting, ok := settings.Lookup(key, settings.LookupForLocalAccess, s.codec.ForSystemTenant())
	if !ok {
		log.Warningf(ctx, "failed to find setting %s, skipping update", redact.Safe(key))
		return
	}
	ws, ok := setting.(settings.NonMaskedSetting)
	if !ok {
		log.Fatalf(ctx, "expected non-masked setting, got %T", s)
	}
	val := settings.EncodedValue{
		Value: ws.EncodedDefault(),
		Type:  ws.Typ(),
	}
	s.setLocked(ctx, key, val)
}

// updateOverrides updates the overrides map and updates any settings
// accordingly.
func (s *SettingsWatcher) updateOverrides(ctx context.Context) {
	newOverrides := s.overridesMonitor.Overrides()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, val := range newOverrides {
		if key == versionSettingKey {
			var newVersion clusterversion.ClusterVersion
			if err := protoutil.Unmarshal([]byte(val.Value), &newVersion); err != nil {
				log.Warningf(ctx, "ignoring invalid cluster version: %newVersion - "+
					"the lack of a refreshed storage cluster version in a secondary tenant may prevent tenant upgrade", err)
			} else {
				// We don't want to fully process the override in the case
				// where we're dealing with the "version" setting, as we want
				// the tenant to have full control over its version setting.
				// Instead, we take the override value and cache it as the
				// storageClusterVersion for use in determining if it's safe to
				// upgrade the tenant (since we don't want to upgrade tenants
				// to a version that's beyond that of the storage cluster).
				log.Infof(ctx, "updating storage cluster cached version from: %v to: %v", s.mu.storageClusterVersion, newVersion)
				s.mu.storageClusterVersion = newVersion
			}
			continue
		}
		if oldVal, hasExisting := s.mu.overrides[key]; hasExisting && oldVal == val {
			// We already have the same override in place; ignore.
			continue
		}
		// A new override was added or an existing override has changed.
		s.mu.overrides[key] = val
		s.setLocked(ctx, key, val)
	}

	// Clean up any overrides that were removed.
	for key := range s.mu.overrides {
		if _, ok := newOverrides[key]; !ok {
			delete(s.mu.overrides, key)

			// Reset the setting to the value in the settings table (or the default
			// value).
			if sv, ok := s.mu.values[key]; ok && !sv.tombstone {
				s.setLocked(ctx, key, sv.val)
			} else {
				s.setDefaultLocked(ctx, key)
			}
		}
	}
}

func (s *SettingsWatcher) resetUpdater() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.updater = s.settings.MakeUpdater()
}

// SetTestingKnobs is used by tests to set testing knobs.
func (s *SettingsWatcher) SetTestingKnobs(knobs *rangefeedcache.TestingKnobs) {
	s.testingWatcherKnobs = knobs
}

// IsOverridden implements cluster.OverridesInformer.
func (s *SettingsWatcher) IsOverridden(settingName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.mu.overrides[settingName]
	return exists
}

// GetStorageClusterVersion returns the storage cluster version cached in the
// SettingsWatcher. The storage cluster version info in the settings watcher is
// populated by a cluster settings override sent from the system tenant to all
// tenants, anytime the storage cluster version changes (or when a new cluster
// is initialized in version 23.1 or later). In cases where the storage cluster
// version is not initialized, we assume that it's running version 22.2,
// the last version which did not properly initialize this value.
func (s *SettingsWatcher) GetStorageClusterVersion() clusterversion.ClusterVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.storageClusterVersion.Equal(clusterversion.ClusterVersion{Version: roachpb.Version{Major: 0, Minor: 0}}) {
		// If the storage cluster version is not initialized in the
		// settingswatcher, it means that the storage cluster has not yet been
		// upgraded to 23.1. As a result, assume that storage cluster is at
		// version 22.2.
		storageClusterVersion := roachpb.Version{Major: 22, Minor: 2, Internal: 0}
		return clusterversion.ClusterVersion{Version: storageClusterVersion}
	}
	return s.mu.storageClusterVersion
}

// GetClusterVersionFromStorage reads the cluster version from the storage via
// the given transaction.
func (s *SettingsWatcher) GetClusterVersionFromStorage(
	ctx context.Context, txn *kv.Txn,
) (clusterversion.ClusterVersion, error) {
	indexPrefix := s.codec.IndexPrefix(keys.SettingsTableID, uint32(1))
	key := encoding.EncodeUvarintAscending(encoding.EncodeStringAscending(indexPrefix, "version"), uint64(0))
	row, err := txn.Get(ctx, key)
	if err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	if row.Value == nil {
		return clusterversion.ClusterVersion{}, errors.New("got nil value for tenant cluster version row")
	}
	_, val, _, err := s.dec.DecodeRow(roachpb.KeyValue{Key: row.Key, Value: *row.Value}, nil /* alloc */)
	if err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	var version clusterversion.ClusterVersion
	if err := protoutil.Unmarshal([]byte(val.Value), &version); err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	return version, nil
}
