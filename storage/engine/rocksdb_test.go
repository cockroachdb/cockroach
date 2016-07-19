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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

	rocksdb := NewRocksDB(
		roachpb.Attributes{},
		".",
		RocksDBCache{},
		0,
		0,
		DefaultMaxOpenFiles,
		stop.NewStopper(),
	)
	const expected = "memtable budget must be at least"
	if err := rocksdb.Open(); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but got %v", expected, err)
	}
}

func TestBatchIterReadOwnWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, stopper := setupMVCCInMemRocksDB(t, "iter_read_own_write")
	defer stopper.Stop()

	b := db.NewBatch()

	k := MakeMVCCMetadataKey(testKey1)

	before := b.NewIterator(false)
	defer before.Close()

	nonBatchBefore := db.NewIterator(false)
	defer nonBatchBefore.Close()

	if err := b.Put(k, []byte("abc")); err != nil {
		t.Fatal(err)
	}

	// We use a prefix iterator for after in order to workaround the restriction
	// on concurrent use of more than 1 prefix or normal (non-prefix) iterator on
	// a batch.
	after := b.NewIterator(true /* prefix */)
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

	nonBatchAfter := db.NewIterator(false)
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
			after.(*rocksDBIterator).engine.closed(),
			b.closed(),
		)
	}()
}

func TestBatchPrefixIter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, stopper := setupMVCCInMemRocksDB(t, "iter_read_own_write")
	defer stopper.Stop()

	b := db.NewBatch()

	// Set up a batch with: delete("a"), put("b"). We'll then prefix seek for "b"
	// which should succeed and then prefix seek for "a" which should fail. Note
	// that order of operations is important here to stress the C++ code paths.
	if err := b.Clear(mvccKey("a")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(mvccKey("b"), []byte("b")); err != nil {
		t.Fatal(err)
	}

	iter := b.NewIterator(true /* prefix */)
	defer iter.Close()

	if iter.Seek(mvccKey("b")); !iter.Valid() {
		t.Fatalf("expected to find \"b\"")
	}
	if iter.Seek(mvccKey("a")); iter.Valid() {
		t.Fatalf("expected to not find anything, found %s -> %q", iter.Key(), iter.Value())
	}
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
		iter := batch.NewIterator(true)
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

	rocksdb := NewRocksDB(
		roachpb.Attributes{},
		dir,
		RocksDBCache{},
		minMemtableBudget,
		0,
		DefaultMaxOpenFiles,
		stopper,
	)
	return rocksdb.Open()
}

func TestCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, err := ioutil.TempDir("", "testing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()

	var expectedKeys []string
	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()

		db := NewRocksDB(
			roachpb.Attributes{},
			dir,
			RocksDBCache{},
			minMemtableBudget,
			0,
			DefaultMaxOpenFiles,
			stopper,
		)
		if err := db.Open(); err != nil {
			t.Fatal(err)
		}

		// Add 20 keys, creating a checkpoint after the 10th key is added.
		for i := 0; i < 20; i++ {
			if i == 10 {
				if err := db.Checkpoint("checkpoint"); err != nil {
					t.Fatal(err)
				}
			}
			s := fmt.Sprintf("%02d", i)
			if err := db.Put(mvccKey(s), []byte(s)); err != nil {
				t.Fatal(err)
			}
			if i < 10 {
				expectedKeys = append(expectedKeys, s)
			}
		}
	}()

	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()

		dir = filepath.Join(dir, "checkpoint")
		db := NewRocksDB(
			roachpb.Attributes{},
			dir,
			RocksDBCache{},
			minMemtableBudget,
			0,
			DefaultMaxOpenFiles,
			stopper,
		)
		if err := db.Open(); err != nil {
			t.Fatal(err)
		}

		// The checkpoint should only contain the first 10 keys.
		var keys []string
		err := db.Iterate(NilKey, MVCCKeyMax, func(kv MVCCKeyValue) (bool, error) {
			keys = append(keys, string(kv.Key.Key))
			return false, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expectedKeys, keys) {
			t.Fatalf("expected %s, but got %s", expectedKeys, keys)
		}
	}()
}

func TestSSTableInfosString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	info := func(level int, size int64) SSTableInfo {
		return SSTableInfo{
			Level: level,
			Size:  size,
		}
	}
	tables := SSTableInfos{
		info(1, 7<<20),
		info(1, 1<<20),
		info(1, 63<<10),
		info(2, 10<<20),
		info(2, 8<<20),
		info(2, 13<<20),
		info(2, 31<<20),
		info(2, 13<<20),
		info(2, 30<<20),
		info(2, 5<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 9<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 93<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 122<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 129<<20),
		info(3, 24<<20),
		info(3, 18<<20),
	}
	expected := `1 [   8M  3 ]: 7M 1M 63K
2 [ 110M  7 ]: 31M 30M 13M[2] 10M 8M 5M
3 [   2G 19 ]: 129M[14] 122M 93M 24M 18M 9M
`
	sort.Sort(tables)
	s := tables.String()
	if expected != s {
		t.Fatalf("expected\n%s\ngot\n%s", expected, s)
	}
}

func TestReadAmplification(t *testing.T) {
	defer leaktest.AfterTest(t)()

	info := func(level int, size int64) SSTableInfo {
		return SSTableInfo{
			Level: level,
			Size:  size,
		}
	}

	tables1 := SSTableInfos{
		info(0, 0),
		info(0, 0),
		info(0, 0),
		info(1, 0),
	}
	if a, e := tables1.ReadAmplification(), 4; a != e {
		t.Errorf("got %d, expected %d", a, e)
	}

	tables2 := SSTableInfos{
		info(0, 0),
		info(1, 0),
		info(2, 0),
		info(3, 0),
	}
	if a, e := tables2.ReadAmplification(), 4; a != e {
		t.Errorf("got %d, expected %d", a, e)
	}

	tables3 := SSTableInfos{
		info(1, 0),
		info(0, 0),
		info(0, 0),
		info(0, 0),
		info(1, 0),
		info(1, 0),
		info(2, 0),
		info(3, 0),
		info(6, 0),
	}
	if a, e := tables3.ReadAmplification(), 7; a != e {
		t.Errorf("got %d, expected %d", a, e)
	}
}
