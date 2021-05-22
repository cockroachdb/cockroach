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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
}

// New constructs a new SettingsWatcher.
func New(
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
) *SettingsWatcher {
	return &SettingsWatcher{
		clock:    clock,
		codec:    codec,
		settings: settingsToUpdate,
		f:        f,
		stopper:  stopper,
		dec:      MakeRowDecoder(codec),
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
	now := s.clock.Now()
	u := s.settings.MakeUpdater()
	initialScanDone := make(chan struct{})
	var initialScanErr error
	rf, err := s.f.RangeFeed(ctx, "settings", settingsTableSpan, now, func(
		ctx context.Context, kv *roachpb.RangeFeedValue,
	) {
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
			ws, ok := s.(settings.WritableSetting)
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
		} else if err := u.Set(ctx, k, val, valType); err != nil {
			log.Warningf(ctx, "failed to set setting %s to %s: %v",
				log.Safe(k), val, err)
		}
	}, rangefeed.WithInitialScan(func(ctx context.Context) {
		u.ResetRemaining(ctx)
		close(initialScanDone)
	}), rangefeed.WithOnInitialScanError(func(
		ctx context.Context, err error,
	) (shouldFail bool) {
		// TODO(ajwerner): Consider if there are other errors which we want to
		// treat as permanent.
		if grpcutil.IsAuthError(err) ||
			// This is a hack around the fact that we do not get properly structured
			// errors out of gRPC. See #56208.
			strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
			initialScanErr = err
			close(initialScanDone)
			shouldFail = true
		}
		return shouldFail
	}))
	if err != nil {
		return err
	}
	select {
	case <-initialScanDone:
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
