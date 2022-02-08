// Copyright 2020 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type settingsCacheWriter struct {
	stopper *stop.Stopper
	eng     storage.Engine

	mu struct {
		syncutil.Mutex
		currentlyWriting bool

		queuedToWrite []roachpb.KeyValue
	}
}

func newSettingsCacheWriter(eng storage.Engine, stopper *stop.Stopper) *settingsCacheWriter {
	return &settingsCacheWriter{
		eng:     eng,
		stopper: stopper,
	}
}

func (s *settingsCacheWriter) SnapshotKVs(ctx context.Context, kvs []roachpb.KeyValue) {
	if !s.queueSnapshot(kvs) {
		return
	}
	if err := s.stopper.RunAsyncTask(ctx, "snapshot-settings-cache", func(
		ctx context.Context,
	) {
		defer s.doneWriting()
		for toWrite, ok := s.getToWrite(); ok; toWrite, ok = s.getToWrite() {
			if err := storeCachedSettingsKVs(ctx, s.eng, toWrite); err != nil {
				log.Warningf(ctx, "failed to write settings snapshot: %v", err)
			}
		}
	}); err != nil {
		s.doneWriting()
	}
}

func (s *settingsCacheWriter) queueSnapshot(kvs []roachpb.KeyValue) (shouldRun bool) {
	s.mu.Lock() // held into the async task
	if s.mu.currentlyWriting {
		s.mu.queuedToWrite = kvs
		s.mu.Unlock()
		return false
	}
	s.mu.currentlyWriting = true
	s.mu.queuedToWrite = kvs
	s.mu.Unlock()
	return true
}

func (s *settingsCacheWriter) doneWriting() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.currentlyWriting = false
}

func (s *settingsCacheWriter) getToWrite() (toWrite []roachpb.KeyValue, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	toWrite, s.mu.queuedToWrite = s.mu.queuedToWrite, nil
	return toWrite, toWrite != nil
}

var _ settingswatcher.Storage = (*settingsCacheWriter)(nil)

// storeCachedSettingsKVs stores or caches node's settings locally.
// This helps in restoring the node restart with the at least the same settings with which it died.
func storeCachedSettingsKVs(ctx context.Context, eng storage.Engine, kvs []roachpb.KeyValue) error {
	batch := eng.NewBatch()
	defer batch.Close()
	for _, kv := range kvs {
		kv.Value.Timestamp = hlc.Timestamp{} // nb: Timestamp is not part of checksum
		if err := storage.MVCCPut(
			ctx, batch, nil, keys.StoreCachedSettingsKey(kv.Key), hlc.Timestamp{}, kv.Value, nil,
		); err != nil {
			return err
		}
	}
	return batch.Commit(false /* sync */)
}

// loadCachedSettingsKVs loads locally stored cached settings.
func loadCachedSettingsKVs(_ context.Context, eng storage.Engine) ([]roachpb.KeyValue, error) {
	var settingsKVs []roachpb.KeyValue
	if err := eng.MVCCIterate(
		keys.LocalStoreCachedSettingsKeyMin,
		keys.LocalStoreCachedSettingsKeyMax,
		storage.MVCCKeyAndIntentsIterKind,
		func(kv storage.MVCCKeyValue) error {
			settingKey, err := keys.DecodeStoreCachedSettingsKey(kv.Key.Key)
			if err != nil {
				return err
			}
			meta := enginepb.MVCCMetadata{}
			if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
				return err
			}
			settingsKVs = append(settingsKVs, roachpb.KeyValue{
				Key:   settingKey,
				Value: roachpb.Value{RawBytes: meta.RawBytes},
			})
			return nil
		},
	); err != nil {
		return nil, err
	}
	return settingsKVs, nil
}

func initializeCachedSettings(
	ctx context.Context, codec keys.SQLCodec, updater settings.Updater, kvs []roachpb.KeyValue,
) error {
	dec := settingswatcher.MakeRowDecoder(codec)
	for _, kv := range kvs {
		settings, val, _, err := dec.DecodeRow(kv)
		if err != nil {
			return errors.Wrap(err, `while decoding settings data
-this likely indicates the settings table structure or encoding has been altered;
-skipping settings updates`)
		}
		if err := updater.Set(ctx, settings, val); err != nil {
			log.Warningf(ctx, "setting %q to %v failed: %+v", settings, val, err)
		}
	}
	updater.ResetRemaining(ctx)
	return nil
}
