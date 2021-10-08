// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func processSystemConfigKVs(
	ctx context.Context, kvs []roachpb.KeyValue, u settings.Updater, eng storage.Engine,
) error {
	tbl := systemschema.SettingsTable

	codec := keys.TODOSQLCodec
	settingsTablePrefix := codec.TablePrefix(uint32(tbl.GetID()))
	dec := settingswatcher.MakeRowDecoder(codec)

	var settingsKVs []roachpb.KeyValue
	processKV := func(ctx context.Context, kv roachpb.KeyValue, u settings.Updater) error {
		if !bytes.HasPrefix(kv.Key, settingsTablePrefix) {
			return nil
		}
		k, v, t, _, err := dec.DecodeRow(kv)
		if err != nil {
			return err
		}
		settingsKVs = append(settingsKVs, kv)

		if err := u.Set(ctx, k, v, t); err != nil {
			log.Warningf(ctx, "setting %q to %q failed: %+v", k, v, err)
		}
		return nil
	}
	for _, kv := range kvs {
		if err := processKV(ctx, kv, u); err != nil {
			return errors.Wrap(err, `while decoding settings data
this likely indicates the settings table structure or encoding has been altered;
skipping settings updates`)
		}
	}
	u.ResetRemaining(ctx)
	return errors.Wrap(storeCachedSettingsKVs(ctx, eng, settingsKVs), "while storing settings kvs")
}

// RefreshSettings starts a settings-changes listener.
func (s *Server) refreshSettings(initialSettingsKVs []roachpb.KeyValue) error {
	ctx := s.AnnotateCtx(context.Background())
	// If we have initial settings KV pairs loaded from the local engines,
	// apply them before starting the gossip listener.
	if len(initialSettingsKVs) != 0 {
		if err := processSystemConfigKVs(ctx, initialSettingsKVs, s.st.MakeUpdater(), s.engines[0]); err != nil {
			return errors.Wrap(err, "during processing initial settings")
		}
	}
	// Setup updater that listens for changes in settings.
	return s.stopper.RunAsyncTask(ctx, "refresh-settings", func(ctx context.Context) {
		gossipUpdateC := s.gossip.RegisterSystemConfigChannel()
		// No new settings can be defined beyond this point.
		for {
			select {
			case <-gossipUpdateC:
				cfg := s.gossip.GetSystemConfig()
				u := s.st.MakeUpdater()
				if err := processSystemConfigKVs(ctx, cfg.Values, u, s.engines[0]); err != nil {
					log.Warningf(ctx, "error processing config KVs: %+v", err)
				}
			case <-s.stopper.ShouldQuiesce():
				return
			}
		}
	})
}
