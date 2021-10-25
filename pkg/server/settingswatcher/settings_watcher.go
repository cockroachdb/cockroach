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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
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

	// Running state, access underneath the rangefeed callback.
	updater settings.Updater

	storage Storage

	// State used to store settings values to disk.
	buffer *rangefeedbuffer.Buffer
	g      singleflight.Group
	mu     struct {
		syncutil.Mutex
		frontierSaved  hlc.Timestamp
		frontierToSave hlc.Timestamp
		data           []roachpb.KeyValue
	}
}

// Storage is used to store a snapshot of KVs which can then be used to
// bootstrap settings state.
type Storage interface {
	WriteKVs(ctx context.Context, kvs []roachpb.KeyValue) error
}

// New constructs a new SettingsWatcher.
func New(
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	storage Storage,
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

func (s *SettingsWatcher) onEntry(ctx context.Context, kv *roachpb.RangeFeedValue) {
	k, val, valType, tombstone, err := s.dec.DecodeRow(roachpb.KeyValue{
		Key:   kv.Key,
		Value: kv.Value,
	})
	if err != nil {
		log.Warningf(ctx, "failed to decode settings row %v: %v", kv.Key, err)
	}
	// This event corresponds to a deletion.
	if tombstone {
		s, ok := settings.Lookup(k, settings.LookupForLocalAccess)
		if !ok {
			log.Warningf(ctx, "failed to find setting %s, skipping update",
				log.Safe(k))
			return
		}
		ws, ok := s.(settings.NonMaskedSetting)
		if !ok {
			log.Fatalf(ctx, "expected writable setting, got %T", s)
		}
		val, valType = ws.EncodedDefault(), ws.Typ()
	}

	// The system tenant (i.e. the KV layer) does not use the SettingsWatcher
	// to propagate cluster version changes (it uses the BumpClusterVersion
	// RPC). However, non-system tenants (i.e. SQL pods) (asynchronously) get
	// word of the new cluster version below.
	const versionSettingKey = "version"
	if k == versionSettingKey && !s.codec.ForSystemTenant() {
		var v clusterversion.ClusterVersion
		if err := protoutil.Unmarshal([]byte(val), &v); err != nil {
			log.Warningf(ctx, "failed to set cluster version: %v", err)
		} else if err := s.settings.Version.SetActiveVersion(ctx, v); err != nil {
			log.Warningf(ctx, "failed to set cluster version: %v", err)
		} else {
			log.Infof(ctx, "set cluster version to: %v", v)
		}
	} else if err := s.updater.Set(ctx, k, val, valType); err != nil {
		log.Warningf(ctx, "failed to set setting %s to %s: %v",
			log.Safe(k), val, err)
	}

	if s.buffer != nil {
		if err := s.buffer.Add((*event)(kv)); err != nil {
			// TODO(ajwerner): What do I do here? Stop buffering?
			// I really wish there were no limit.
			log.Warningf(ctx, "failed to buffer setting for storage, "+
				"giving up on storing settings")
			s.buffer = nil
		}
	}
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
	s.updater = s.settings.MakeUpdater()

	var (
		initialScanDoneCh = make(chan struct{})
		initialScanFunc   = func(ctx context.Context) {
			s.updater.ResetRemaining(ctx)
			close(initialScanDoneCh)
		}
		initialScanErr       error
		initialScanErrorFunc = func(
			ctx context.Context, err error,
		) (shouldFail bool) {
			// TODO(ajwerner): Consider if there are other errors which we want to
			// treat as permanent.
			if grpcutil.IsAuthError(err) ||
				// This is a hack around the fact that we do not get properly structured
				// errors out of gRPC. See #56208.
				strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
				initialScanErr = err
				close(initialScanDoneCh)
				shouldFail = true
			}
			return shouldFail
		}
	)
	opts := []rangefeed.Option{
		rangefeed.WithInitialScan(initialScanFunc),
		rangefeed.WithOnInitialScanError(initialScanErrorFunc),
	}
	if s.storage != nil {
		const aNumberThatBetterBeLargeEnough = 1 << 16
		s.buffer = rangefeedbuffer.New(aNumberThatBetterBeLargeEnough)
		opts = append(opts, rangefeed.WithOnFrontierAdvance(func(
			ctx context.Context, timestamp hlc.Timestamp,
		) {
			if !s.setFrontierToSnapshot(timestamp) {
				return
			}
			s.snapshotFrontier(ctx, timestamp)
		}))
	}
	now := s.clock.Now()
	rf, err := s.f.RangeFeed(
		ctx, "settings", []roachpb.Span{settingsTableSpan}, now, s.onEntry, opts...,
	)
	if err != nil {
		return err
	}
	select {
	case <-initialScanDoneCh:
		if initialScanErr != nil {
			return initialScanErr
		}
		s.stopper.AddCloser(rf)
		return nil
	case <-s.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable,
			"failed to retrieve initial cluster settings")
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(),
			"failed to retrieve initial cluster settings")
	}
}

// setFrontierToSnapshot sets the frontier timestamp for which there is
// a pending snapshot.
func (s *SettingsWatcher) setFrontierToSnapshot(timestamp hlc.Timestamp) (forwarded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.frontierToSave.Forward(timestamp)
}

func (s *SettingsWatcher) getFrontierState() (toSnapshop, stored hlc.Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.frontierToSave, s.mu.frontierSaved
}

func (s *SettingsWatcher) snapshotFrontier(ctx context.Context, toSnapshot hlc.Timestamp) {
	// If this returns an error, it's because we are shutting down. Ignore it.
	_ = s.stopper.RunAsyncTask(ctx, "snapshot-settings", func(
		ctx context.Context,
	) {
		for {
			tsInterface, _, err := s.g.Do("", func() (interface{}, error) {
				toSave, saved := s.getFrontierState()
				if toSave.Equal(saved) {
					return nil, nil
				}
				newData := s.copyEventsToData(s.buffer.Flush(ctx, toSave))
				if err := s.storage.WriteKVs(ctx, newData); err != nil {
					return nil, err
				}
				s.mu.Lock()
				defer s.mu.Unlock()
				s.mu.frontierSaved = toSave
				return toSave, nil
			})
			if err != nil {
				return
			}
			if ts, ok := tsInterface.(hlc.Timestamp); ok && toSnapshot.LessEq(ts) {
				return
			}
		}
	})
}

type event roachpb.RangeFeedValue

func (r *event) Timestamp() hlc.Timestamp {
	return r.Value.Timestamp
}

var _ rangefeedbuffer.Event = (*event)(nil)

func (s *SettingsWatcher) copyEventsToData(events []rangefeedbuffer.Event) []roachpb.KeyValue {
	var kvs []roachpb.KeyValue
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		kvs = append(kvs, s.mu.data...)
	}()
	for _, ev := range events {
		ev := ev.(*event)
		kvs = append(kvs, roachpb.KeyValue{Key: ev.Key, Value: ev.Value})
	}
	// Sort the keys data before compacting it away.
	sort.Slice(kvs, func(i, j int) bool {
		cmp := kvs[i].Key.Compare(kvs[j].Key)
		switch {
		case cmp < 0:
			return true
		case cmp > 0:
			return false
		default:
			// Sort larger timestamps earlier.
			return !kvs[i].Value.Timestamp.LessEq(kvs[j].Value.Timestamp)
		}
	})
	// Remove the older entries.
	truncated := kvs[:0]
	for _, kv := range kvs {
		if len(truncated) > 0 &&
			truncated[len(truncated)-1].Key.Equal(kv.Key) {
			continue
		}
		truncated = append(truncated, kv)
	}
	// Remove the tombstones.
	kvs, truncated = truncated, truncated[:0]
	for _, kv := range kvs {
		if !kv.Value.IsPresent() {
			continue
		}
		truncated = append(truncated, kv)
	}
	// Clear the memory in the rest of the slice.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.data = append(make([]roachpb.KeyValue, 0, len(truncated)), kvs...)
	return s.mu.data
}
