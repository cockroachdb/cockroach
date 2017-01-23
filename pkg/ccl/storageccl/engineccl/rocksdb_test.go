// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package engineccl

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestRocksDBSst(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanup := testutils.TempDir(t, 0)
	defer cleanup()

	kvs := []engine.MVCCKeyValue{
		{Key: engine.MVCCKey{Key: []byte("key1")}, Value: []byte("value1")},
		{Key: engine.MVCCKey{Key: []byte("key2")}, Value: []byte("value2")},
	}

	file := filepath.Join(dir, "sst")

	w := MakeRocksDBSstFileWriter()
	if err := w.Open(file); err != nil {
		t.Fatalf("%+v", err)
	}
	defer func() { _ = w.Close() }()
	for _, kv := range kvs {
		if err := w.Add(kv); err != nil {
			t.Fatalf("%+v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	r, err := MakeRocksDBSstFileReader()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer r.Close()
	if err := r.AddFile(file); err != nil {
		t.Fatalf("%+v", err)
	}

	var results []engine.MVCCKeyValue
	iterateFn := func(kv engine.MVCCKeyValue) (bool, error) {
		results = append(results, kv)
		return false, nil
	}
	startKey, endKey := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}
	if err := r.Iterate(startKey, endKey, iterateFn); err != nil {
		t.Fatalf("%+v", err)
	}

	if !reflect.DeepEqual(kvs, results) {
		t.Fatalf("expected %+v got %+v", kvs, results)
	}
}

func BenchmarkRocksDBSstFileWriter(b *testing.B) {
	dir, err := ioutil.TempDir("", "BenchmarkRocksDBSstFileWriter")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()

	const maxEntries = 100000
	const keyLen = 10
	const valLen = 100
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	kv := engine.MVCCKeyValue{
		Key:   engine.MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts},
		Value: make([]byte, valLen),
	}

	b.ResetTimer()
	sst := MakeRocksDBSstFileWriter()
	if err := sst.Open(filepath.Join(dir, "sst")); err != nil {
		b.Fatal(sst)
	}
	defer func() {
		if err := sst.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	for i := 1; i <= b.N; i++ {
		if i%maxEntries == 0 {
			if err := sst.Close(); err != nil {
				b.Fatal(err)
			}
			sst = MakeRocksDBSstFileWriter()
			if err := sst.Open(filepath.Join(dir, "sst")); err != nil {
				b.Fatal(sst)
			}
		}

		b.StopTimer()
		kv.Key.Key = []byte(fmt.Sprintf("%09d", i))
		copy(kv.Value, kv.Key.Key)
		b.StartTimer()
		if err := sst.Add(kv); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(keyLen + valLen)
}

func BenchmarkRocksDBSstFileReader(b *testing.B) {
	dir, err := ioutil.TempDir("", "BenchmarkRocksDBSstFileReader")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()

	sstPath := filepath.Join(dir, "sst")
	{
		const maxEntries = 100000
		const keyLen = 10
		const valLen = 100
		b.SetBytes(keyLen + valLen)

		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		kv := engine.MVCCKeyValue{
			Key:   engine.MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts},
			Value: make([]byte, valLen),
		}

		sst := MakeRocksDBSstFileWriter()
		if err := sst.Open(sstPath); err != nil {
			b.Fatal(sst)
		}
		var entries = b.N
		if entries > maxEntries {
			entries = maxEntries
		}
		for i := 0; i < entries; i++ {
			kv.Key.Key = []byte(fmt.Sprintf("%09d", i))
			copy(kv.Value, kv.Key.Key)
			if err := sst.Add(kv); err != nil {
				b.Fatal(err)
			}
		}
		if err := sst.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	sst, err := MakeRocksDBSstFileReader()
	if err != nil {
		b.Fatal(err)
	}
	if err := sst.AddFile(sstPath); err != nil {
		b.Fatal(err)
	}
	defer sst.Close()
	count := 0
	iterateFn := func(kv engine.MVCCKeyValue) (bool, error) {
		count++
		if count >= b.N {
			return true, nil
		}
		return false, nil
	}
	for {
		startKey, endKey := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}
		if err := sst.Iterate(startKey, endKey, iterateFn); err != nil {
			b.Fatal(err)
		}
		if count >= b.N {
			break
		}
	}
}
