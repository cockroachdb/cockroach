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

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const testCacheSize = 1 << 30 // 1 GB

// TestBatchReadLaterWrite demonstrates that reading from a batch is not like
// reading from a snapshot: writes that occur after opening the batch will be
// visible to reads from the batch (whereas using a snapshot, they would not).
func TestBatchReadLaterWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	key := roachpb.Key("a")

	eng := setupMVCCInMemRocksDB(t, "unused")
	defer eng.Close()

	batch := eng.NewBatch()
	defer batch.Close()
	snap := eng.NewSnapshot()
	defer snap.Close()

	v := roachpb.MakeValueFromString("foo")

	if err := MVCCPut(ctx, eng, nil, key, hlc.Timestamp{}, v, nil); err != nil {
		t.Fatal(err)
	}

	// Read from a batch that was opened before the value was written to the
	// underlying engine. The batch will see the write.
	{
		rv, _, err := MVCCGet(ctx, batch, key, hlc.Timestamp{}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rv == nil {
			t.Fatal("value not found")
		}

		if !rv.Equal(&v) {
			t.Fatalf("values not equal: put %v, read %v", v, *rv)
		}
	}

	// Read from a snapshot opened prior to the write. The snapshot won't see the
	// write.
	{
		rv, _, err := MVCCGet(ctx, snap, key, hlc.Timestamp{}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rv != nil {
			t.Fatalf("value unexpectedly found: %v", *rv)
		}
	}
}

func TestBatchIterReadOwnWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db := setupMVCCInMemRocksDB(t, "iter_read_own_write")
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()

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

	after.Seek(k)
	if ok, err := after.Valid(); !ok {
		t.Fatalf("write missing on batch iter created after write, err=%v", err)
	}
	before.Seek(k)
	if ok, err := before.Valid(); !ok {
		t.Fatalf("write missing on batch iter created before write, err=%v", err)
	}
	nonBatchBefore.Seek(k)
	if ok, err := nonBatchBefore.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("uncommitted write seen by non-batch iter")
	}

	if err := b.Commit(false /* sync */); err != nil {
		t.Fatal(err)
	}

	nonBatchAfter := db.NewIterator(false)
	defer nonBatchAfter.Close()

	nonBatchBefore.Seek(k)
	if ok, err := nonBatchBefore.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("committed write seen by non-batch iter created before commit")
	}
	nonBatchAfter.Seek(k)
	if ok, err := nonBatchAfter.Valid(); !ok {
		t.Fatalf("committed write missing by non-batch iter created after commit, err=%v", err)
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

func TestBatchPrefixIter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db := setupMVCCInMemRocksDB(t, "iter_read_own_write")
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()

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

	iter.Seek(mvccKey("b"))
	if ok, err := iter.Valid(); !ok {
		t.Fatalf("expected to find \"b\", err=%v", err)
	}
	iter.Seek(mvccKey("a"))
	if ok, err := iter.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("expected to not find anything, found %s -> %q", iter.Key(), iter.Value())
	}
}

func makeKey(i int) MVCCKey {
	return MakeMVCCMetadataKey(roachpb.Key(strconv.Itoa(i)))
}

func benchmarkIterOnBatch(b *testing.B, writes int) {
	engine := createTestEngine()
	defer engine.Close()

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

func benchmarkIterOnReadWriter(
	b *testing.B, writes int, f func(Engine) ReadWriter, closeReadWriter bool,
) {
	engine := createTestEngine()
	defer engine.Close()

	for i := 0; i < writes; i++ {
		if err := engine.Put(makeKey(i), []byte(strconv.Itoa(i))); err != nil {
			b.Fatal(err)
		}
	}

	readWriter := f(engine)
	if closeReadWriter {
		defer readWriter.Close()
	}

	r := rand.New(rand.NewSource(5))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := makeKey(r.Intn(writes))
		iter := readWriter.NewIterator(true)
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
		{true, Version{-1}, "incompatible rocksdb data version, current:2, on disk:-1, minimum:0"},
		{true, Version{3}, "incompatible rocksdb data version, current:2, on disk:3, minimum:0"},
	}

	for i, testCase := range testCases {
		err := openRocksDBWithVersion(t, testCase.hasFile, testCase.ver)
		if !testutils.IsError(err, testCase.expectedErr) {
			t.Errorf("%d: expected error '%s', actual '%v'", i, testCase.expectedErr, err)
		}
	}
}

// openRocksDBWithVersion attempts to open a rocks db instance, optionally with
// the supplied Version struct.
func openRocksDBWithVersion(t *testing.T, hasVersionFile bool, ver Version) error {
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

	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		RocksDBCache{},
	)
	if err == nil {
		rocksdb.Close()
	}
	return err
}

func TestRocksDBApproximateDiskBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rocksdb.Close()

	rnd, seed := randutil.NewPseudoRand()

	log.Infof(context.Background(), "seed is %d", seed)

	for i := 0; i < 10; i++ {
		ts := hlc.Timestamp{WallTime: rnd.Int63()}
		key := roachpb.Key(randutil.RandBytes(rnd, 1<<10))
		key = append(key, []byte(fmt.Sprintf("#%d", i))...) // make unique
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, 1<<20))
		value.InitChecksum(key)
		if err := MVCCPut(context.Background(), rocksdb, nil, key, ts, value, nil); err != nil {
			t.Fatal(err)
		}
		if err := rocksdb.Flush(); err != nil {
			t.Fatal(err)
		}
		keyOnlySize, err := rocksdb.ApproximateDiskBytes(key, key.Next())
		if err != nil {
			t.Fatal(err)
		}
		const mb = int64(1 << 20)
		if min, max, act := mb/2, 2*mb, int64(keyOnlySize); act < min || act > max {
			t.Fatalf("iteration %d: new kv pair estimated at %s; expected between %s and %s",
				i+1, humanizeutil.IBytes(act), humanizeutil.IBytes(min), humanizeutil.IBytes(max))
		}

		allSize, err := rocksdb.ApproximateDiskBytes(roachpb.KeyMin, roachpb.KeyMax)
		if err != nil {
			t.Fatal(err)
		}

		if min, max, act := int64(i)*mb, int64(i+2)*mb, int64(allSize); act < min || act > max {
			t.Fatalf("iteration %d: total size estimated at %s; expected between %s and %s",
				i+1, humanizeutil.IBytes(act), humanizeutil.IBytes(min), humanizeutil.IBytes(max))
		}

	}
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

func TestInMemIllegalOption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cache := NewRocksDBCache(10 << 20 /* 10mb */)
	defer cache.Release()

	r := &RocksDB{
		cfg: RocksDBConfig{
			MustExist: true,
		},
		// dir: empty dir == "mem" RocksDB instance.
		cache: cache.ref(),
	}
	err := r.open()
	const expErr = `could not open rocksdb instance: Invalid argument: ` +
		`: does not exist \(create_if_missing is false\)`
	if !testutils.IsError(err, expErr) {
		t.Error(err)
	}
}

func TestConcurrentBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testutils.NightlyStress() || util.RaceEnabled {
		t.Skip()
	}

	dir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()

	db, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", dir, err)
	}
	defer db.Close()

	// Prepare 16 4 MB batches containing non-overlapping contents.
	var batches []Batch
	for i := 0; i < 16; i++ {
		batch := db.NewBatch()
		for j := 0; true; j++ {
			key := encoding.EncodeUvarintAscending([]byte("bar"), uint64(i))
			key = encoding.EncodeUvarintAscending(key, uint64(j))
			if err := batch.Put(MakeMVCCMetadataKey(key), nil); err != nil {
				t.Fatal(err)
			}
			if len(batch.Repr()) >= 4<<20 {
				break
			}
		}
		batches = append(batches, batch)
	}

	errChan := make(chan error, len(batches))

	// Concurrently write all the batches.
	for _, batch := range batches {
		go func(batch Batch) {
			errChan <- batch.Commit(false /* sync */)
		}(batch)
	}

	// While the batch writes are in progress, try to write another key.
	time.Sleep(100 * time.Millisecond)
	remainingBatches := len(batches)
	for i := 0; remainingBatches > 0; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				t.Fatal(err)
			}
			remainingBatches--
		default:
		}

		// This write can get delayed excessively if we hit the max memtable count
		// or the L0 stop writes threshold.
		start := timeutil.Now()
		key := encoding.EncodeUvarintAscending([]byte("foo"), uint64(i))
		if err := db.Put(MakeMVCCMetadataKey(key), nil); err != nil {
			t.Fatal(err)
		}
		if elapsed := timeutil.Since(start); elapsed >= 10*time.Second {
			t.Fatalf("write took %0.1fs\n", elapsed.Seconds())
		}
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
	kv := MVCCKeyValue{
		Key:   MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts},
		Value: make([]byte, valLen),
	}

	b.ResetTimer()
	sst, err := MakeRocksDBSstFileWriter()
	if err != nil {
		b.Fatal(sst)
	}
	defer sst.Close()
	for i := 1; i <= b.N; i++ {
		if i%maxEntries == 0 {
			if _, err := sst.Finish(); err != nil {
				b.Fatal(err)
			}
			sst, err = MakeRocksDBSstFileWriter()
			if err != nil {
				b.Fatal(sst)
			}
			defer sst.Close()
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

	var sstContents []byte
	{
		const maxEntries = 100000
		const keyLen = 10
		const valLen = 100
		b.SetBytes(keyLen + valLen)

		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		kv := MVCCKeyValue{
			Key:   MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts},
			Value: make([]byte, valLen),
		}

		sst, err := MakeRocksDBSstFileWriter()
		if err != nil {
			b.Fatal(sst)
		}
		defer sst.Close()
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
		sstContents, err = sst.Finish()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	sst := MakeRocksDBSstFileReader()
	defer sst.Close()

	if err := sst.IngestExternalFile(sstContents); err != nil {
		b.Fatal(err)
	}
	count := 0
	iterateFn := func(kv MVCCKeyValue) (bool, error) {
		count++
		if count >= b.N {
			return true, nil
		}
		return false, nil
	}
	for {
		if err := sst.Iterate(MVCCKey{Key: keys.MinKey}, MVCCKey{Key: keys.MaxKey}, iterateFn); err != nil {
			b.Fatal(err)
		}
		if count >= b.N {
			break
		}
	}
}

func TestRocksDBTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", dir, err)
	}
	defer rocksdb.Close()

	var minTimestamp = hlc.Timestamp{WallTime: 1, Logical: 0}
	var maxTimestamp = hlc.Timestamp{WallTime: 3, Logical: 0}
	times := []hlc.Timestamp{
		{WallTime: 2, Logical: 0},
		minTimestamp,
		maxTimestamp,
		{WallTime: 2, Logical: 0},
	}

	for i, time := range times {
		s := fmt.Sprintf("%02d", i)
		key := MVCCKey{Key: roachpb.Key(s), Timestamp: time}
		if err := rocksdb.Put(key, []byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rocksdb.Flush(); err != nil {
		t.Fatal(err)
	}

	ssts, err := rocksdb.getUserProperties()
	if err != nil {
		t.Fatal(err)
	}
	if len(ssts.Sst) != 1 {
		t.Fatalf("expected 1 sstable got %d", len(ssts.Sst))
	}
	sst := ssts.Sst[0]
	if sst.TsMin == nil || *sst.TsMin != minTimestamp {
		t.Fatalf("got min %v expected %v", sst.TsMin, minTimestamp)
	}
	if sst.TsMax == nil || *sst.TsMax != maxTimestamp {
		t.Fatalf("got max %v expected %v", sst.TsMax, maxTimestamp)
	}

	batch := rocksdb.NewBatch()
	defer batch.Close()

	// Make a time bounded iterator that skips the SSTable containing our writes.
	func() {
		tbi := batch.NewTimeBoundIterator(maxTimestamp.Next(), maxTimestamp.Next().Next())
		defer tbi.Close()
		tbi.Seek(NilKey)

		var count int
		for ; ; tbi.Next() {
			ok, err := tbi.Valid()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			count++
		}

		// Make sure the iterator sees no writes.
		if expCount := 0; expCount != count {
			t.Fatalf("saw %d values in time bounded iterator, but expected %d", count, expCount)
		}
	}()

	// Make a regular iterator. Before #21721, this would accidentally pick up the
	// time bounded iterator instead.
	iter := batch.NewIterator(false)
	defer iter.Close()
	iter.Seek(NilKey)

	var count int
	for ; ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		count++
	}

	// Make sure the iterator sees the writes (i.e. it's not the time bounded iterator).
	if expCount := len(times); expCount != count {
		t.Fatalf("saw %d values in regular iterator, but expected %d", count, expCount)
	}
}

func key(s string) MVCCKey {
	return MakeMVCCMetadataKey([]byte(s))
}

// Regression test for https://github.com/facebook/rocksdb/issues/2752. Range
// deletion tombstones between different snapshot stripes are not stored in
// order, so the first tombstone of each snapshot stripe should be checked as a
// smallest candidate.
func TestRocksDBDeleteRangeBug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	db, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", dir, err)
	}
	defer db.Close()

	if err := db.Put(key("a"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	func() {
		if err := db.ClearRange(key("b"), key("c")); err != nil {
			t.Fatal(err)
		}
		// Hold a snapshot to separate these two delete ranges.
		snap := db.NewSnapshot()
		defer snap.Close()
		if err := db.ClearRange(key("a"), key("b")); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}()

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	iter := db.NewIterator(false)
	iter.Seek(key("a"))
	if ok, _ := iter.Valid(); ok {
		t.Fatalf("unexpected key: %s", iter.Key())
	}
	iter.Close()
}

func createTestSSTableInfos() SSTableInfos {
	ssti := SSTableInfos{
		// Level 0.
		{Level: 0, Size: 20, Start: key("a"), End: key("z")},
		{Level: 0, Size: 15, Start: key("a"), End: key("k")},
		// Level 1.
		{Level: 1, Size: 200, Start: key("a"), End: key("j")},
		{Level: 1, Size: 100, Start: key("k"), End: key("o")},
		{Level: 1, Size: 100, Start: key("r"), End: key("t")},
		// Level 2.
		{Level: 2, Size: 201, Start: key("a"), End: key("c")},
		{Level: 2, Size: 200, Start: key("d"), End: key("f")},
		{Level: 2, Size: 300, Start: key("h"), End: key("r")},
		{Level: 2, Size: 405, Start: key("s"), End: key("z")},
		// Level 3.
		{Level: 3, Size: 667, Start: key("a"), End: key("c")},
		{Level: 3, Size: 230, Start: key("d"), End: key("f")},
		{Level: 3, Size: 332, Start: key("h"), End: key("i")},
		{Level: 3, Size: 923, Start: key("k"), End: key("n")},
		{Level: 3, Size: 143, Start: key("n"), End: key("o")},
		{Level: 3, Size: 621, Start: key("p"), End: key("s")},
		{Level: 3, Size: 411, Start: key("u"), End: key("x")},
		// Level 4.
		{Level: 4, Size: 215, Start: key("a"), End: key("b")},
		{Level: 4, Size: 211, Start: key("b"), End: key("d")},
		{Level: 4, Size: 632, Start: key("e"), End: key("f")},
		{Level: 4, Size: 813, Start: key("f"), End: key("h")},
		{Level: 4, Size: 346, Start: key("h"), End: key("j")},
		{Level: 4, Size: 621, Start: key("j"), End: key("l")},
		{Level: 4, Size: 681, Start: key("m"), End: key("o")},
		{Level: 4, Size: 521, Start: key("o"), End: key("r")},
		{Level: 4, Size: 135, Start: key("r"), End: key("t")},
		{Level: 4, Size: 622, Start: key("t"), End: key("v")},
		{Level: 4, Size: 672, Start: key("x"), End: key("z")},
	}
	sort.Sort(ssti)
	return ssti
}

func TestSSTableInfosByLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ssti := NewSSTableInfosByLevel(createTestSSTableInfos())

	// First, verify that each level is sorted by start key, not size.
	for level, l := range ssti.levels {
		if level == 0 {
			continue
		}
		lastInfo := l[0]
		for _, info := range l[1:] {
			if !lastInfo.Start.Less(info.Start) {
				t.Errorf("sort failed (%s >= %s) for level %d", lastInfo.Start, info.Start, level)
			}
		}
	}
	if a, e := ssti.MaxLevel(), 4; a != e {
		t.Errorf("expected MaxLevel() == %d; got %d", e, a)
	}

	// Next, verify various contiguous overlap scenarios.
	testCases := []struct {
		span        roachpb.Span
		expMaxLevel int
	}{
		// The full a-z span overlaps more than two SSTables at all levels L1-L4
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}, expMaxLevel: 0},
		// The a-j span overlaps the first three SSTables in L2, so max level is L1.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("j")}, expMaxLevel: 1},
		// The k-o span overlaps only two adjacent L4 SSTs: j-l & m-o.
		{span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("o")}, expMaxLevel: 4},
		// The K0-o0 span hits three SSTs in L4: j-l, m-o, & o-r.
		{span: roachpb.Span{Key: roachpb.Key("k0"), EndKey: roachpb.Key("o0")}, expMaxLevel: 3},
		// The k-z span overlaps the last 4 SSTs in L3.
		{span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("z")}, expMaxLevel: 2},
		// The c-c0 span overlaps only the second L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("c0")}, expMaxLevel: 4},
		// The a-f span full overlaps the first three L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")}, expMaxLevel: 3},
		// The a-d0 span only overlaps the first two L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d0")}, expMaxLevel: 4},
		// The a-e span only overlaps the first two L4 SSTs. It only is adjacent to the 3rd.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}, expMaxLevel: 4},
		// The a-d span overlaps fully the first two L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}, expMaxLevel: 4},
		// The a-a0 span overlaps only the first L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("a0")}, expMaxLevel: 4},
		// The 0-1 span doesn't overlap any L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("0"), EndKey: roachpb.Key("1")}, expMaxLevel: 4},
		// The Z-a span doesn't overlap any L4 SSTs, just touches the start of the first.
		{span: roachpb.Span{Key: roachpb.Key("Z"), EndKey: roachpb.Key("a")}, expMaxLevel: 4},
		// The Z-a0 span overlaps only the first L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("Z"), EndKey: roachpb.Key("a0")}, expMaxLevel: 4},
		// The z-z0 span doesn't overlap any L4 SSTs, just touches the end of the last.
		{span: roachpb.Span{Key: roachpb.Key("z"), EndKey: roachpb.Key("z0")}, expMaxLevel: 4},
		// The y-z0 span overlaps the last L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("z0")}, expMaxLevel: 4},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.span.Key, test.span.EndKey), func(t *testing.T) {
			maxLevel := ssti.MaxLevelSpanOverlapsContiguousSSTables(test.span)
			if test.expMaxLevel != maxLevel {
				t.Errorf("expected max level %d; got %d", test.expMaxLevel, maxLevel)
			}
		})
	}
}

func TestRocksDBOptions(t *testing.T) {
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
	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
			RocksDBOptions: "use_fsync=true;" +
				"min_write_buffer_number_to_merge=2;" +
				"block_based_table_factory={block_size=4k}",
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatal(err)
	}
	rocksdb.Close()

	paths, err := filepath.Glob(dir + "/OPTIONS-*")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		data, err := ioutil.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}

		options := []string{
			"use_fsync=true",
			"min_write_buffer_number_to_merge=2",
			"block_size=4096",
		}
		for _, o := range options {
			fullOption := fmt.Sprintf("  %s\n", o)
			if !bytes.Contains(data, []byte(fullOption)) {
				t.Errorf("unable to find %s in %s", o, p)
			}
		}
	}
}
