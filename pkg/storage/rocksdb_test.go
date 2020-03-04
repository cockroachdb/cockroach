// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"context"
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
		rv, _, err := MVCCGet(ctx, batch, key, hlc.Timestamp{}, MVCCGetOptions{})
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
		rv, _, err := MVCCGet(ctx, snap, key, hlc.Timestamp{}, MVCCGetOptions{})
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

	before := b.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
	defer before.Close()

	nonBatchBefore := db.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
	defer nonBatchBefore.Close()

	if err := b.Put(k, []byte("abc")); err != nil {
		t.Fatal(err)
	}

	// We use a prefix iterator for after in order to workaround the restriction
	// on concurrent use of more than 1 prefix or normal (non-prefix) iterator on
	// a batch.
	after := b.NewIterator(IterOptions{Prefix: true})
	defer after.Close()

	after.SeekGE(k)
	if ok, err := after.Valid(); !ok {
		t.Fatalf("write missing on batch iter created after write, err=%v", err)
	}
	before.SeekGE(k)
	if ok, err := before.Valid(); !ok {
		t.Fatalf("write missing on batch iter created before write, err=%v", err)
	}
	nonBatchBefore.SeekGE(k)
	if ok, err := nonBatchBefore.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("uncommitted write seen by non-batch iter")
	}

	if err := b.Commit(false /* sync */); err != nil {
		t.Fatal(err)
	}

	nonBatchAfter := db.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
	defer nonBatchAfter.Close()

	nonBatchBefore.SeekGE(k)
	if ok, err := nonBatchBefore.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("committed write seen by non-batch iter created before commit")
	}
	nonBatchAfter.SeekGE(k)
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
		after.SeekGE(k)
		t.Fatalf(`Seek on batch-backed iter after batched closed should panic.
			iter.engine: %T, iter.engine.Closed: %v, batch.Closed %v`,
			after.(*rocksDBIterator).reader,
			after.(*rocksDBIterator).reader.Closed(),
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

	iter := b.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()

	iter.SeekGE(mvccKey("b"))
	if ok, err := iter.Valid(); !ok {
		t.Fatalf("expected to find \"b\", err=%v", err)
	}
	iter.SeekGE(mvccKey("a"))
	if ok, err := iter.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("expected to not find anything, found %s -> %q", iter.Key(), iter.Value())
	}
}

func TestIterBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db := setupMVCCInMemRocksDB(t, "iter_bounds")
	defer db.Close()

	if err := db.Put(mvccKey("0"), []byte("val")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(mvccKey("a"), []byte("val")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(mvccKey("b"), []byte("val")); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name         string
		createEngine func() Reader
	}{
		{"batch", func() Reader { return db.NewBatch() }},
		{"readonly", func() Reader { return db.NewReadOnly() }},
		{"snapshot", func() Reader { return db.NewSnapshot() }},
		{"engine", func() Reader { return db }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.createEngine()
			defer e.Close()

			if _, ok := e.(*rocksDBBatch); !ok { // batches do not support reverse iteration
				// Test that a new iterator's lower bound is applied.
				func() {
					iter := e.NewIterator(IterOptions{LowerBound: roachpb.Key("b")})
					defer iter.Close()
					iter.SeekLT(mvccKey("c"))
					if ok, err := iter.Valid(); err != nil {
						t.Fatal(err)
					} else if !ok {
						t.Fatalf("expected iterator to be valid, but was invalid")
					}
					iter.SeekLT(mvccKey("b"))
					if ok, err := iter.Valid(); err != nil {
						t.Fatal(err)
					} else if ok {
						t.Fatalf("expected iterator to be invalid, but was valid")
					}
					iter.SeekLT(mvccKey("a"))
					if ok, err := iter.Valid(); err != nil {
						t.Fatal(err)
					} else if ok {
						t.Fatalf("expected iterator to be invalid, but was valid")
					}
				}()

				// Test that the cached iterator, if the underlying engine implementation
				// caches iterators, can take on a new lower bound.
				func() {
					iter := e.NewIterator(IterOptions{LowerBound: roachpb.Key("a")})
					defer iter.Close()

					iter.SeekLT(mvccKey("b"))
					if ok, err := iter.Valid(); !ok {
						t.Fatal(err)
					}
					if !mvccKey("a").Equal(iter.Key()) {
						t.Fatalf("expected key a, but got %q", iter.Key())
					}
					iter.Prev()
					if ok, err := iter.Valid(); err != nil {
						t.Fatal(err)
					} else if ok {
						t.Fatalf("expected iterator to be invalid, but was valid")
					}
				}()
			}

			// Test that a new iterator's upper bound is applied.
			func() {
				iter := e.NewIterator(IterOptions{UpperBound: roachpb.Key("a")})
				defer iter.Close()
				iter.SeekGE(mvccKey("a"))
				if ok, err := iter.Valid(); err != nil {
					t.Fatal(err)
				} else if ok {
					t.Fatalf("expected iterator to be invalid, but was valid")
				}
			}()

			// Test that the cached iterator, if the underlying engine implementation
			// caches iterators, can take on a new upper bound.
			func() {
				iter := e.NewIterator(IterOptions{UpperBound: roachpb.Key("b")})
				defer iter.Close()

				iter.SeekGE(mvccKey("a"))
				if ok, err := iter.Valid(); !ok {
					t.Fatal(err)
				}
				if !mvccKey("a").Equal(iter.Key()) {
					t.Fatalf("expected key a, but got %q", iter.Key())
				}
				iter.Next()
				if ok, err := iter.Valid(); err != nil {
					t.Fatal(err)
				} else if ok {
					t.Fatalf("expected iterator to be invalid, but was valid")
				}
			}()

			// Perform additional tests if the engine supports writes.
			w, isReadWriter := e.(ReadWriter)
			if _, isSecretlyReadOnly := e.(*rocksDBReadOnly); !isReadWriter || isSecretlyReadOnly {
				return
			}
			if err := w.Put(mvccKey("c"), []byte("val")); err != nil {
				t.Fatal(err)
			}
			func() {
				iter := w.NewIterator(IterOptions{UpperBound: roachpb.Key("c")})
				defer iter.Close()
				iter.SeekGE(mvccKey("c"))
				if ok, err := iter.Valid(); err != nil {
					t.Fatal(err)
				} else if ok {
					t.Fatalf("expected iterator to be invalid, but was valid")
				}
			}()
		})
	}
}

func makeKey(i int) MVCCKey {
	return MakeMVCCMetadataKey(roachpb.Key(strconv.Itoa(i)))
}

func benchmarkIterOnBatch(ctx context.Context, b *testing.B, writes int) {
	engine := createTestRocksDBEngine()
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
		iter := batch.NewIterator(IterOptions{Prefix: true})
		iter.SeekGE(key)
		iter.Close()
	}
}

func benchmarkIterOnReadWriter(
	b *testing.B, writes int, f func(Engine) ReadWriter, closeReadWriter bool,
) {
	engine := createTestRocksDBEngine()
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
		iter := readWriter.NewIterator(IterOptions{Prefix: true})
		iter.SeekGE(key)
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
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
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
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
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
			StorageConfig: base.StorageConfig{
				MustExist: true,
			},
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
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %+v", dir, err)
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
			const targetSize = 4 << 20
			if targetSize < maxBatchGroupSize {
				t.Fatalf("target size (%d) should be larger than the max batch group size (%d)",
					targetSize, maxBatchGroupSize)
			}
			if batch.Len() >= targetSize {
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

// TestRocksDBSstFileWriterTruncate ensures that sum of the chunks created by
// calling Truncate on a RocksDBSstFileWriter is equivalent to an SST built
// without ever calling Truncate.
func TestRocksDBSstFileWriterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Truncate will be used on this writer.
	sst1, err := MakeRocksDBSstFileWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer sst1.Close()

	// Truncate will not be used on this writer.
	sst2, err := MakeRocksDBSstFileWriter()
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()

	const keyLen = 10
	const valLen = 950
	ts := hlc.Timestamp{WallTime: 1}
	key := MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts}
	value := make([]byte, valLen)

	var resBuf1, resBuf2 []byte
	const entries = 100000
	const truncateChunk = entries / 10
	for i := 0; i < entries; i++ {
		key.Key = []byte(fmt.Sprintf("%09d", i))
		copy(value, key.Key)

		if err := sst1.Put(key, value); err != nil {
			t.Fatal(err)
		}
		if err := sst2.Put(key, value); err != nil {
			t.Fatal(err)
		}

		if i > 0 && i%truncateChunk == 0 {
			sst1Chunk, err := sst1.Truncate()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("iteration %d, truncate chunk\tlen=%d", i, len(sst1Chunk))

			// Even though we added keys, it is not guaranteed strictly by the
			// contract of Truncate that a byte slice will be returned. This is
			// because the keys may be in un-flushed blocks. This test had been tuned
			// such that every other batch chunk is always large enough to require at
			// least one block to be flushed.
			empty := len(sst1Chunk) == 0
			if i%(2*truncateChunk) == 0 {
				if empty {
					t.Fatalf("expected non-empty SST chunk during iteration %d", i)
				}
				resBuf1 = append(resBuf1, sst1Chunk...)
			} else {
				if !empty {
					t.Fatalf("expected empty SST chunk during iteration %d", i)
				}
			}
		}
	}

	sst1FinishBuf, err := sst1.Finish()
	if err != nil {
		t.Fatal(err)
	}
	resBuf1 = append(resBuf1, sst1FinishBuf...)
	t.Logf("truncated sst final chunk\t\tlen=%d", len(sst1FinishBuf))

	resBuf2, err = sst2.Finish()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("non-truncated sst final chunk\tlen=%d", len(resBuf2))

	if !bytes.Equal(resBuf1, resBuf2) {
		t.Errorf("expected SST made up of truncate chunks (len=%d) to be equivalent to SST that "+
			"was not (len=%d)", len(sst1FinishBuf), len(resBuf2))
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
		if err := sst.Put(kv.Key, kv.Value); err != nil {
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
			if err := sst.Put(kv.Key, kv.Value); err != nil {
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
		if err := sst.Iterate(keys.MinKey, keys.MaxKey, iterateFn); err != nil {
			b.Fatal(err)
		}
		if count >= b.N {
			break
		}
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
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %+v", dir, err)
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

	iter := db.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
	iter.SeekGE(key("a"))
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
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
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

// Verify that range tombstones do not result in sstables that cover an
// exessively large portion of the key space.
func TestRocksDBDeleteRangeCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db := setupMVCCInMemRocksDB(t, "delrange")
	defer db.Close()

	// Disable automatic compactions which interfere with test expectations
	// below.
	if err := db.(*RocksDB).disableAutoCompaction(); err != nil {
		t.Fatal(err)
	}

	makeKey := func(prefix string, i int) roachpb.Key {
		return roachpb.Key(fmt.Sprintf("%s%09d", prefix, i))
	}

	rnd, _ := randutil.NewPseudoRand()

	// Create sstables in L6 that are half the L6 target size. Any smaller and
	// RocksDB might choose to compact them.
	const targetSize = 64 << 20
	const numEntries = 10000
	const keySize = 10
	const valueSize = (targetSize / numEntries) - keySize

	for _, p := range "abc" {
		sst, err := MakeRocksDBSstFileWriter()
		if err != nil {
			t.Fatal(sst)
		}
		defer sst.Close()

		for i := 0; i < numEntries; i++ {
			if err := sst.Put(MVCCKey{Key: makeKey(string(p), i)}, randutil.RandBytes(rnd, valueSize)); err != nil {
				t.Fatal(err)
			}
		}

		sstContents, err := sst.Finish()
		if err != nil {
			t.Fatal(err)
		}

		filename := fmt.Sprintf("ingest")
		if err := db.WriteFile(filename, sstContents); err != nil {
			t.Fatal(err)
		}

		if err := db.IngestExternalFiles(context.Background(), []string{filename}); err != nil {
			t.Fatal(err)
		}
		if testing.Verbose() {
			fmt.Printf("ingested %s\n", string(p))
		}
	}

	getSSTables := func() string {
		ssts := db.GetSSTables()
		sort.Slice(ssts, func(i, j int) bool {
			a, b := ssts[i], ssts[j]
			if a.Level < b.Level {
				return true
			}
			if a.Level > b.Level {
				return false
			}
			return a.Start.Less(b.Start)
		})
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n")
		for i := range ssts {
			fmt.Fprintf(&buf, "%d: %s - %s\n",
				ssts[i].Level, ssts[i].Start.Key, ssts[i].End.Key)
		}
		return buf.String()
	}

	verifySSTables := func(expected string) {
		actual := getSSTables()
		if expected != actual {
			t.Fatalf("expected%sgot%s", expected, actual)
		}
		if testing.Verbose() {
			fmt.Printf("%s", actual)
		}
	}

	// After setup there should be 3 sstables.
	verifySSTables(`
6: "a000000000" - "a000009999"
6: "b000000000" - "b000009999"
6: "c000000000" - "c000009999"
`)

	// Generate a batch which writes to the very first key, and then deletes the
	// range of keys covered by the last sstable.
	batch := db.NewBatch()
	if err := batch.Put(MakeMVCCMetadataKey(makeKey("a", 0)), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := batch.ClearRange(MakeMVCCMetadataKey(makeKey("c", 0)),
		MakeMVCCMetadataKey(makeKey("c", numEntries))); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// After flushing, there is a single additional L0 table that covers the
	// entire key range.
	verifySSTables(`
0: "a000000000" - "c000010000"
6: "a000000000" - "a000009999"
6: "b000000000" - "b000009999"
6: "c000000000" - "c000009999"
`)

	// Compacting the key range covering the last sstable should result in that
	// sstable being deleted. Prior to the hack in dbClearRange, all of the
	// sstables would be compacted resulting in 2 L6 sstables with different
	// boundaries than the ones below.
	_ = db.CompactRange(makeKey("c", 0), makeKey("c", numEntries), false)
	verifySSTables(`
5: "a000000000" - "a000000000"
6: "a000000000" - "a000009999"
6: "b000000000" - "b000009999"
`)
}

func BenchmarkRocksDBDeleteRangeIterate(b *testing.B) {
	for _, entries := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			for _, deleted := range []int{entries, entries - 1} {
				b.Run(fmt.Sprintf("deleted=%d", deleted), func(b *testing.B) {
					db := setupMVCCInMemRocksDB(b, "unused")
					defer db.Close()

					makeKey := func(i int) roachpb.Key {
						return roachpb.Key(fmt.Sprintf("%09d", i))
					}

					// Create an SST with N entries and ingest it. This is a fast way to get a
					// lot of entries into RocksDB.
					{
						sst, err := MakeRocksDBSstFileWriter()
						if err != nil {
							b.Fatal(sst)
						}
						defer sst.Close()

						for i := 0; i < entries; i++ {
							if err := sst.Put(MVCCKey{Key: makeKey(i)}, nil); err != nil {
								b.Fatal(err)
							}
						}

						sstContents, err := sst.Finish()
						if err != nil {
							b.Fatal(err)
						}

						filename := fmt.Sprintf("ingest")
						if err := db.WriteFile(filename, sstContents); err != nil {
							b.Fatal(err)
						}

						err = db.IngestExternalFiles(context.Background(), []string{filename})
						if err != nil {
							b.Fatal(err)
						}
					}

					// Create a range tombstone that deletes most (or all) of those entries.
					from := makeKey(0)
					to := makeKey(deleted)
					if err := db.ClearRange(MakeMVCCMetadataKey(from), MakeMVCCMetadataKey(to)); err != nil {
						b.Fatal(err)
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						iter := db.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
						iter.SeekGE(MakeMVCCMetadataKey(from))
						ok, err := iter.Valid()
						if err != nil {
							b.Fatal(err)
						}
						if deleted < entries {
							if !ok {
								b.Fatal("key not found")
							}
						} else if ok {
							b.Fatal("unexpected key found")
						}
						iter.Close()
					}
				})
			}
		})
	}
}

func TestMakeBatchGroup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Assume every newly instantiated batch has size 12 (header only).
	testCases := []struct {
		maxSize   int
		groupSize []int
		leader    []bool
		groups    []int
	}{
		{1, []int{12, 12, 12}, []bool{true, true, true}, []int{1, 1, 1}},
		{23, []int{12, 12, 12}, []bool{true, true, true}, []int{1, 1, 1}},
		{24, []int{12, 24, 12}, []bool{true, false, true}, []int{2, 1}},
		{35, []int{12, 24, 12}, []bool{true, false, true}, []int{2, 1}},
		{36, []int{12, 24, 36}, []bool{true, false, false}, []int{3}},
		{
			48,
			[]int{12, 24, 36, 48, 12},
			[]bool{true, false, false, false, true},
			[]int{4, 1},
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var pending []*rocksDBBatch
			var groupSize int
			for i := range c.groupSize {
				b := &rocksDBBatch{}
				var leader bool
				pending, groupSize, leader = makeBatchGroup(pending, b, groupSize, c.maxSize)
				if c.groupSize[i] != groupSize {
					t.Fatalf("expected group size %d, but found %d", c.groupSize[i], groupSize)
				}
				if c.leader[i] != leader {
					t.Fatalf("expected leader %t, but found %t", c.leader[i], leader)
				}
			}
			var groups []int
			for len(pending) > 0 {
				var group []*rocksDBBatch
				group, pending = nextBatchGroup(pending)
				groups = append(groups, len(group))
			}
			if !reflect.DeepEqual(c.groups, groups) {
				t.Fatalf("expected %d, but found %d", c.groups, groups)
			}
		})
	}
}

// Verify that RocksDBSstFileWriter works with time bounded iterators.
func TestSstFileWriterTimeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	db := setupMVCCInMemRocksDB(t, "sstwriter-timebound")
	defer db.Close()

	for walltime := int64(1); walltime < 5; walltime++ {
		sst, err := MakeRocksDBSstFileWriter()
		if err != nil {
			t.Fatal(sst)
		}
		defer sst.Close()
		if err := sst.Put(
			MVCCKey{Key: []byte("key"), Timestamp: hlc.Timestamp{WallTime: walltime}},
			[]byte("value"),
		); err != nil {
			t.Fatal(err)
		}
		sstContents, err := sst.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.WriteFile(`ingest`, sstContents); err != nil {
			t.Fatal(err)
		}
		if err := db.IngestExternalFiles(ctx, []string{`ingest`}); err != nil {
			t.Fatal(err)
		}
	}

	it := db.NewIterator(IterOptions{
		UpperBound:       keys.MaxKey,
		MinTimestampHint: hlc.Timestamp{WallTime: 2},
		MaxTimestampHint: hlc.Timestamp{WallTime: 3},
		WithStats:        true,
	})
	defer it.Close()
	for it.SeekGE(MVCCKey{Key: keys.MinKey}); ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	if s := it.Stats(); s.TimeBoundNumSSTs != 2 {
		t.Errorf(`expected 2 sstables got %d`, s.TimeBoundNumSSTs)
	}
}

// TestRocksDBWALFileEmptyBatch verifies that committing an empty batch does
// not write an entry to RocksDB's write-ahead log.
func TestRocksDBWALFileEmptyBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	// NB: The in-mem RocksDB instance doesn't support syncing the WAL which is
	// necessary for this test.
	e, err := NewRocksDB(
		RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
		},
		RocksDBCache{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Commit a batch with one key.
	b := e.NewBatch()
	defer b.Close()
	if err := b.Put(mvccKey("foo"), []byte{'b', 'a', 'r'}); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true /* sync */); err != nil {
		t.Fatal(err)
	}

	// Verify that RocksDB has created a non-empty WAL.
	walsBefore, err := e.GetSortedWALFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(walsBefore) != 1 {
		t.Fatalf("expected exactly one WAL file, but got %d", len(walsBefore))
	}
	if walsBefore[0].Size == 0 {
		t.Fatalf("expected non-empty WAL file")
	}

	// Commit an empty batch.
	b = e.NewBatch()
	defer b.Close()
	if err := b.Commit(true /* sync */); err != nil {
		t.Fatal(err)
	}

	// Verify that the WAL has not changed in size.
	walsAfter, err := e.GetSortedWALFiles()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(walsBefore, walsAfter) {
		t.Fatalf("expected wal files %#v after committing empty batch, but got %#v",
			walsBefore, walsAfter)
	}

	// Regression test a bug that would accidentally make Commit a no-op (via an
	// errant fast-path) when a batch contained only LogData.
	testutils.RunTrueAndFalse(t, "distinct", func(t *testing.T, distinct bool) {
		walsBefore, err := e.GetSortedWALFiles()
		if err != nil {
			t.Fatal(err)
		}
		if len(walsBefore) != 1 {
			t.Fatalf("expected one WAL file, got %d", len(walsBefore))
		}

		batch := e.NewBatch()
		defer batch.Close()

		var rw ReadWriter = batch
		if distinct {
			// NB: we can't actually close this distinct batch because it auto-
			// closes when the batch commits.
			rw = batch.Distinct()
		}

		if err := rw.LogData([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if batch.Empty() {
			t.Error("batch is not empty")
		}

		if err := batch.Commit(true /* sync */); err != nil {
			t.Fatal(err)
		}

		// Verify that the WAL has grown.
		walsAfter, err := e.GetSortedWALFiles()
		if err != nil {
			t.Fatal(err)
		}

		if len(walsAfter) != 1 {
			t.Fatalf("expected one WAL file, got %+v", walsAfter)
		}

		if after, before := walsAfter[0].Size, walsBefore[0].Size; after <= before {
			t.Fatalf("wal size was expected to increase, got %d -> %d", before, after)
		}
	})
}
