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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

		values    map[string]RawValue
		overrides map[string]RawValue
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
	u := s.settings.MakeUpdater()
	var initialScan = struct {
		ch   chan struct{}
		done bool
		err  error
	}{
		ch: make(chan struct{}),
	}
	noteUpdate := func(update rangefeedcache.Update) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if update.Type == rangefeedcache.CompleteUpdate {
			u.ResetRemaining(ctx)
			if !initialScan.done {
				initialScan.done = true
				close(initialScan.ch)
			}
		}
	}

	s.mu.values = make(map[string]RawValue)

	if s.overridesMonitor != nil {
		s.mu.overrides = make(map[string]RawValue)
		// Initialize the overrides. We want to do this before processing the
		// settings table, otherwise we could see temporary transitions to the value
		// in the table.
		s.updateOverrides(ctx, u)

		// Set up a worker to watch the monitor.
		if err := s.stopper.RunAsyncTask(ctx, "setting-overrides", func(ctx context.Context) {
			overridesCh := s.overridesMonitor.NotifyCh()
			for {
				select {
				case <-overridesCh:
					s.updateOverrides(ctx, u)

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
			(update.Type == rangefeedcache.PartialUpdate && len(update.Events) == 0) {
			return
		}
		eventKVs := rangefeedbuffer.EventsToKVs(update.Events,
			rangefeedbuffer.RangeFeedValueEventToKV)
		switch update.Type {
		case rangefeedcache.CompleteUpdate:
			snapshot = eventKVs
		case rangefeedcache.PartialUpdate:
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
			return s.handleKV(ctx, kv, u)
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
			u = s.settings.MakeUpdater()
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
	ctx context.Context, kv *roachpb.RangeFeedValue, u settings.Updater,
) rangefeedbuffer.Event {
	name, val, tombstone, err := s.dec.DecodeRow(roachpb.KeyValue{
		Key:   kv.Key,
		Value: kv.Value,
	})
	if err != nil {
		log.Warningf(ctx, "failed to decode settings row %v: %v", kv.Key, err)
		return nil
	}

	if !s.codec.ForSystemTenant() {
		setting, ok := settings.Lookup(name, settings.LookupForLocalAccess, s.codec.ForSystemTenant())
		if !ok {
			log.Warningf(ctx, "unknown setting %s, skipping update", log.Safe(name))
			return nil
		}
		if setting.Class() != settings.TenantWritable {
			log.Warningf(ctx, "ignoring read-only setting %s", log.Safe(name))
			return nil
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, hasOverride := s.mu.overrides[name]
	if tombstone {
		// This event corresponds to a deletion.
		delete(s.mu.values, name)
		if !hasOverride {
			s.setDefault(ctx, u, name)
		}
	} else {
		s.mu.values[name] = val
		if !hasOverride {
			s.set(ctx, u, name, val)
		}
	}
	if s.storage != nil {
		return kv
	}
	return nil
}

const versionSettingKey = "version"

// set the current value of a setting.
func (s *SettingsWatcher) set(ctx context.Context, u settings.Updater, key string, val RawValue) {
	// The system tenant (i.e. the KV layer) does not use the SettingsWatcher
	// to propagate cluster version changes (it uses the BumpClusterVersion
	// RPC). However, non-system tenants (i.e. SQL pods) (asynchronously) get
	// word of the new cluster version below.
	if key == versionSettingKey && !s.codec.ForSystemTenant() {
		var v clusterversion.ClusterVersion
		if err := protoutil.Unmarshal([]byte(val.Value), &v); err != nil {
			log.Warningf(ctx, "failed to set cluster version: %v", err)
		} else if err := s.settings.Version.SetActiveVersion(ctx, v); err != nil {
			log.Warningf(ctx, "failed to set cluster version: %v", err)
		} else {
			log.Infof(ctx, "set cluster version to: %v", v)
		}
		return
	}

	if err := u.Set(ctx, key, val.Value, val.Type); err != nil {
		log.Warningf(ctx, "failed to set setting %s to %s: %v", log.Safe(key), val.Value, err)
	}
}

// setDefault sets a setting to its default value.
func (s *SettingsWatcher) setDefault(ctx context.Context, u settings.Updater, key string) {
	setting, ok := settings.Lookup(key, settings.LookupForLocalAccess, s.codec.ForSystemTenant())
	if !ok {
		log.Warningf(ctx, "failed to find setting %s, skipping update", log.Safe(key))
		return
	}
	ws, ok := setting.(settings.NonMaskedSetting)
	if !ok {
		log.Fatalf(ctx, "expected non-masked setting, got %T", s)
	}
	val := RawValue{
		Value: ws.EncodedDefault(),
		Type:  ws.Typ(),
	}
	s.set(ctx, u, key, val)
}

// updateOverrides updates the overrides map and updates any settings
// accordingly.
func (s *SettingsWatcher) updateOverrides(ctx context.Context, u settings.Updater) {
	newOverrides := s.overridesMonitor.Overrides()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, val := range newOverrides {
		if key == versionSettingKey {
			log.Warningf(ctx, "ignoring attempt to override %s", key)
			continue
		}
		if oldVal, hasExisting := s.mu.overrides[key]; hasExisting && oldVal == val {
			// We already have the same override in place; ignore.
			continue
		}
		// A new override was added or an existing override has changed.
		s.mu.overrides[key] = val
		s.set(ctx, u, key, val)
	}

	// Clean up any overrides that were removed.
	for key := range s.mu.overrides {
		if _, ok := newOverrides[key]; !ok {
			delete(s.mu.overrides, key)

			// Reset the setting to the value in the settings table (or the default
			// value).
			if val, ok := s.mu.values[key]; ok {
				s.set(ctx, u, key, val)
			} else {
				s.setDefault(ctx, u, key)
			}
		}
	}
}
