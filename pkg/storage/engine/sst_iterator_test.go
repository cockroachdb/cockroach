// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func runTestSSTIterator(t *testing.T, iter SimpleIterator, allKVs []MVCCKeyValue) {
	// Drop the first kv so we can test Seek.
	expected := allKVs[1:]

	// Run the test multiple times to check re-Seeking.
	for i := 0; i < 3; i++ {
		var kvs []MVCCKeyValue
		for iter.Seek(expected[0].Key); ; iter.Next() {
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

		iter.Seek(seekTo)
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

func TestSSTIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sst, err := MakeRocksDBSstFileWriter()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer sst.Close()
	var allKVs []MVCCKeyValue
	for i := 0; i < 10; i++ {
		kv := MVCCKeyValue{
			Key: MVCCKey{
				Key:       []byte{'A' + byte(i)},
				Timestamp: hlc.Timestamp{WallTime: int64(i)},
			},
			Value: []byte{'a' + byte(i)},
		}
		if err := sst.Put(kv.Key, kv.Value); err != nil {
			t.Fatalf("%+v", err)
		}
		allKVs = append(allKVs, kv)
	}

	data, err := sst.Finish()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	t.Run("Disk", func(t *testing.T) {
		tempDir, cleanup := testutils.TempDir(t)
		defer cleanup()

		path := filepath.Join(tempDir, "data.sst")
		if err := ioutil.WriteFile(path, data, 0600); err != nil {
			t.Fatalf("%+v", err)
		}

		iter, err := NewSSTIterator(path)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		runTestSSTIterator(t, iter, allKVs)
	})
	t.Run("Mem", func(t *testing.T) {
		iter, err := NewMemSSTIterator(data, false)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		runTestSSTIterator(t, iter, allKVs)
	})
}

func TestCockroachComparer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyAMetadata := MVCCKey{
		Key: []byte("a"),
	}
	keyA2 := MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	}
	keyA1 := MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 1},
	}
	keyB2 := MVCCKey{
		Key:       []byte("b"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	}

	if x := MVCCComparer.Compare(EncodeKey(keyAMetadata), EncodeKey(keyA1)); x != -1 {
		t.Errorf("expected key metadata to sort first got: %d", x)
	}
	if x := MVCCComparer.Compare(EncodeKey(keyA2), EncodeKey(keyA1)); x != -1 {
		t.Errorf("expected higher timestamp to sort first got: %d", x)
	}
	if x := MVCCComparer.Compare(EncodeKey(keyA2), EncodeKey(keyB2)); x != -1 {
		t.Errorf("expected lower key to sort first got: %d", x)
	}
}
