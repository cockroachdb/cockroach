// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func runTestLegacySSTIterator(t *testing.T, iter SimpleMVCCIterator, allKVs []MVCCKeyValue) {
	// Drop the first kv so we can test Seek.
	expected := allKVs[1:]

	// Run the test multiple times to check re-Seeking.
	for i := 0; i < 3; i++ {
		var kvs []MVCCKeyValue
		for iter.SeekGE(expected[0].Key); ; iter.Next() {
			ok, err := iter.Valid()
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if !ok {
				break
			}
			kv := MVCCKeyValue{
				Key: MVCCKey{
					Key:       append([]byte(nil), iter.UnsafeKey().Key...),
					Timestamp: iter.UnsafeKey().Timestamp,
				},
				Value: append([]byte(nil), iter.UnsafeValue()...),
			}
			kvs = append(kvs, kv)
		}
		if ok, err := iter.Valid(); err != nil {
			t.Fatalf("%+v", err)
		} else if ok {
			t.Fatalf("expected !ok")
		}

		lastElemKey := expected[len(expected)-1].Key
		seekTo := MVCCKey{Key: lastElemKey.Key.Next()}

		iter.SeekGE(seekTo)
		if ok, err := iter.Valid(); err != nil {
			t.Fatalf("%+v", err)
		} else if ok {
			foundKey := iter.UnsafeKey()
			t.Fatalf("expected !ok seeking to lastEmem.Next(). foundKey %s < seekTo %s: %t",
				foundKey, seekTo, foundKey.Less(seekTo))
		}

		if !reflect.DeepEqual(kvs, expected) {
			t.Fatalf("got %+v but expected %+v", kvs, expected)
		}
	}
}

func TestLegacySSTIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemFile{}
	sst := MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sst.Close()
	var allKVs []MVCCKeyValue
	maxWallTime := 10
	for i := 0; i < maxWallTime; i++ {
		var v MVCCValue
		v.Value.SetBytes([]byte{'a', byte(i)})
		vRaw, err := EncodeMVCCValue(v)
		require.NoError(t, err)

		kv := MVCCKeyValue{
			Key: MVCCKey{
				Key:       []byte{'A' + byte(i)},
				Timestamp: hlc.Timestamp{WallTime: int64(i)},
			},
			Value: vRaw,
		}
		if err := sst.Put(kv.Key, kv.Value); err != nil {
			t.Fatalf("%+v", err)
		}
		allKVs = append(allKVs, kv)
	}

	if err := sst.Finish(); err != nil {
		t.Fatalf("%+v", err)
	}

	t.Run("Disk", func(t *testing.T) {
		tempDir, cleanup := testutils.TempDir(t)
		defer cleanup()

		path := filepath.Join(tempDir, "data.sst")
		if err := os.WriteFile(path, sstFile.Data(), 0600); err != nil {
			t.Fatalf("%+v", err)
		}
		file, err := vfs.Default.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		iter, err := NewLegacySSTIterator(file)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		runTestLegacySSTIterator(t, iter, allKVs)
	})
	t.Run("Mem", func(t *testing.T) {
		iter, err := NewLegacyMemSSTIterator(sstFile.Data(), false)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		runTestLegacySSTIterator(t, iter, allKVs)
	})
	t.Run("AsOf", func(t *testing.T) {
		iter, err := NewLegacyMemSSTIterator(sstFile.Data(), false)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		asOfTimes := []hlc.Timestamp{
			{WallTime: int64(maxWallTime / 2)},
			{WallTime: int64(maxWallTime)},
			{}}
		for _, asOf := range asOfTimes {
			var asOfKVs []MVCCKeyValue
			for _, kv := range allKVs {
				if !asOf.IsEmpty() && asOf.Less(kv.Key.Timestamp) {
					continue
				}
				asOfKVs = append(asOfKVs, kv)
			}
			asOfIter := NewReadAsOfIterator(iter, asOf)
			runTestLegacySSTIterator(t, asOfIter, asOfKVs)
		}
	})
}
