// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

const testCacheSize = 1 << 30 // 1 GB

func TestMinMemtableBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rocksdb := NewRocksDB(roachpb.Attributes{}, ".", 0, 0, 0, stop.NewStopper())
	const expected = "memtable budget must be at least"
	if err := rocksdb.Open(); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but got %v", expected, err)
	}
}

func TestBatchIterReadOwnWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 1 << 30 // 1 GB

	db, stopper := setupMVCCInMemRocksDB(t, "iter_read_own_write")
	defer stopper.Stop()

	b := db.NewBatch()

	k := MakeMVCCMetadataKey(testKey1)

	before := b.NewIterator(nil)
	defer before.Close()

	nonBatchBefore := db.NewIterator(nil)
	defer nonBatchBefore.Close()

	if err := b.Put(k, []byte("abc")); err != nil {
		t.Fatal(err)
	}

	after := b.NewIterator(nil)
	defer after.Close()

	if after.Seek(k); !after.Valid() {
		t.Fatal("write missing on batch iter created after write")
	}
	if before.Seek(k); !before.Valid() {
		t.Fatal("write missing on batch iter created before write")
	}
	if nonBatchBefore.Seek(k); nonBatchBefore.Valid() {
		t.Fatal("uncommitted write seen by non-batch iter")
	}

	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}

	nonBatchAfter := db.NewIterator(nil)
	defer nonBatchAfter.Close()

	if nonBatchBefore.Seek(k); nonBatchBefore.Valid() {
		t.Fatal("committed write seen by non-batch iter created before commit")
	}
	if nonBatchAfter.Seek(k); !nonBatchAfter.Valid() {
		t.Fatal("committed write missing by non-batch iter created after commit")
	}

	// `Commit` frees the batch, so iterators backed by it should panic.
	func() {
		defer func() {
			if err, expected := recover(), "iterator used after backing engine closed"; err != expected {
				t.Fatalf("Unexpected panic: expected %q, got %q", expected, err)
			}
		}()
		after.Seek(k)
		t.Fatalf(`Seek on batch-backed iter after batched closed should panic.
			iter.engine: %T, iter.engine.Closed: %v, batch.Closed %v`,
			after.(*rocksDBIterator).engine,
			after.(*rocksDBIterator).engine.Closed(),
			b.Closed(),
		)
	}()
}

func makeKey(i int) MVCCKey {
	return MakeMVCCMetadataKey(roachpb.Key(strconv.Itoa(i)))
}

func benchmarkIterOnBatch(b *testing.B, writes int) {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	engine := createTestEngine(stopper)

	for i := 0; i < writes; i++ {
		if err := engine.Put(makeKey(i), []byte(strconv.Itoa(i))); err != nil {
			b.Fatal(err)
		}
	}

	batch := engine.NewBatch()
	defer batch.Close()

	for i := 0; i < writes; i++ {
		if err := batch.Clear(makeKey(i)); err != nil {
			b.Fatal(err)
		}
	}

	r := rand.New(rand.NewSource(5))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := makeKey(r.Intn(writes))
		iter := batch.NewIterator(key.Key)
		iter.Seek(key)
		iter.Close()
	}
}

// TestRocksDBOpenWithVersions verifies the version checking in Open()
// functions correctly.
func TestRocksDBOpenWithVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		hasFile     bool
		ver         Version
		expectedErr string
	}{
		{false, Version{}, ""},
		{true, Version{versionCurrent}, ""},
		{true, Version{versionMinimum}, ""},
		{true, Version{-1}, "incompatible rocksdb data version, current:1, on disk:-1, minimum:0"},
		{true, Version{2}, "incompatible rocksdb data version, current:1, on disk:2, minimum:0"},
	}

	for i, testCase := range testCases {
		err := openRocksDBWithVersion(t, testCase.hasFile, testCase.ver)
		if err == nil && len(testCase.expectedErr) == 0 {
			continue
		}
		if !testutils.IsError(err, testCase.expectedErr) {
			t.Errorf("%d: expected error '%s', actual '%s'", i, testCase.expectedErr, err)
		}
	}
}

// openRocksDBWithVersion attempts to open a rocks db instance, optionally with
// the supplied Version struct.
func openRocksDBWithVersion(t *testing.T, hasVersionFile bool, ver Version) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	dir, err := ioutil.TempDir("", "testing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()

	if hasVersionFile {
		b, err := json.Marshal(ver)
		if err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(getVersionFilename(dir), b, 0644); err != nil {
			t.Fatal(err)
		}
	}

	rocksdb := NewRocksDB(roachpb.Attributes{}, dir, 0, minMemtableBudget, 0, stopper)
	defer rocksdb.Close()
	return rocksdb.Open()
}
