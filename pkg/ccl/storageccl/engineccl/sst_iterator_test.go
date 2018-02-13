// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func runTestSSTIterator(t *testing.T, iter engine.SimpleIterator, allKVs []engine.MVCCKeyValue) {
	// Drop the first kv so we can test Seek.
	expected := allKVs[1:]

	// Run the test multiple times to check re-Seeking.
	for i := 0; i < 3; i++ {
		var kvs []engine.MVCCKeyValue
		for iter.Seek(expected[0].Key); ; iter.Next() {
			ok, err := iter.Valid()
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if !ok {
				break
			}
			kv := engine.MVCCKeyValue{
				Key: engine.MVCCKey{
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

		if !reflect.DeepEqual(kvs, expected) {
			t.Fatalf("got %+v but expected %+v", kvs, expected)
		}
	}
}

func TestSSTIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer sst.Close()
	var allKVs []engine.MVCCKeyValue
	for i := byte(0); i < 10; i++ {
		kv := engine.MVCCKeyValue{
			Key: engine.MVCCKey{
				Key:       []byte{i},
				Timestamp: hlc.Timestamp{WallTime: int64(i)},
			},
			Value: []byte{i},
		}
		if err := sst.Add(kv); err != nil {
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

	keyAMetadata := engine.EncodeKey(engine.MVCCKey{
		Key: []byte("a"),
	})
	keyA2 := engine.EncodeKey(engine.MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	})
	keyA1 := engine.EncodeKey(engine.MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 1},
	})
	keyB2 := engine.EncodeKey(engine.MVCCKey{
		Key:       []byte("b"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	})

	c := cockroachComparer{}
	if x := c.Compare(keyAMetadata, keyA1); x != -1 {
		t.Errorf("expected key metadata to sort first got: %d", x)
	}
	if x := c.Compare(keyA2, keyA1); x != -1 {
		t.Errorf("expected higher timestamp to sort first got: %d", x)
	}
	if x := c.Compare(keyA2, keyB2); x != -1 {
		t.Errorf("expected lower key to sort first got: %d", x)
	}
}
