// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func makeIntTableKVs(numKeys, valueSize, maxRevisions int) []engine.MVCCKeyValue {
	prefix := encoding.EncodeUvarintAscending(keys.MakeTablePrefix(uint32(100)), uint64(1))
	kvs := make([]engine.MVCCKeyValue, numKeys)
	r, _ := randutil.NewPseudoRand()

	var k int
	for i := 0; i < numKeys; {
		k += 1 + rand.Intn(100)
		key := encoding.EncodeVarintAscending(append([]byte{}, prefix...), int64(k))
		buf := make([]byte, valueSize)
		randutil.ReadTestdataBytes(r, buf)
		revisions := 1 + r.Intn(maxRevisions)

		ts := int64(maxRevisions * 100)
		for j := 0; j < revisions && i < numKeys; j++ {
			ts -= 1 + r.Int63n(99)
			kvs[i].Key.Key = key
			kvs[i].Key.Timestamp.WallTime = ts
			kvs[i].Key.Timestamp.Logical = r.Int31()
			kvs[i].Value = roachpb.MakeValueFromString(string(buf)).RawBytes
			i++
		}
	}
	return kvs
}

func makeRocksSST(t testing.TB, kvs []engine.MVCCKeyValue) []byte {
	w, err := engine.MakeRocksDBSstFileWriter()
	require.NoError(t, err)
	defer w.Close()

	for i := range kvs {
		if err := w.Put(kvs[i].Key, kvs[i].Value); err != nil {
			t.Fatal(err)
		}
	}
	sst, err := w.Finish()
	require.NoError(t, err)
	return sst
}

func makePebbleSST(t testing.TB, kvs []engine.MVCCKeyValue) []byte {
	w := engine.MakeSSTWriter()
	defer w.Close()

	for i := range kvs {
		if err := w.Put(kvs[i].Key, kvs[i].Value); err != nil {
			t.Fatal(err)
		}
	}
	sst, err := w.Finish()
	require.NoError(t, err)
	return sst
}

// TestPebbleWritesSameSSTs tests that using pebble to write some SST produces
// the same file -- byte-for-byte -- as using our Rocks-based writer. This is is
// done not because we don't trust pebble to write the correct SST, but more
// because we otherwise don't have a great way to be sure we've configured it to
// to the same thing we configured RocksDB to do w.r.t. all the block size, key
// filtering, property collecting, etc settings. Getting these settings wrong
// could easily produce an SST with the same K/V content but subtle and hard to
// debug differences in runtime performance (which also could prove elusive when
// it could compacted away at any time and replaced with a Rocks-written one).
//
// This test may need to be removed if/when Pebble's SSTs diverge from Rocks'.
// That is probably OK: it is mostly intended to increase our confidence during
// the transition that we're not introducing a regression. Once pebble-written
// SSTs are the norm, comparing to ones written using the Rocks writer (which
// didn't actually share a configuration with the serving RocksDB, so they were
// already different from actual runtime-written SSTs) will no longer be a
// concen (though we will likely want testing of things like prop collectors).
func TestPebbleWritesSameSSTs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, _ := randutil.NewPseudoRand()
	const numKeys, valueSize, revisions = 5000, 100, 100

	kvs := makeIntTableKVs(numKeys, valueSize, revisions)
	sstRocks := makeRocksSST(t, kvs)
	sstPebble := makePebbleSST(t, kvs)

	itRocks, err := engine.NewMemSSTIterator(sstRocks, false)
	require.NoError(t, err)
	itPebble, err := engine.NewMemSSTIterator(sstPebble, false)
	require.NoError(t, err)

	itPebble.Seek(engine.NilKey)
	for itRocks.Seek(engine.NilKey); ; {
		okRocks, err := itRocks.Valid()
		if err != nil {
			t.Fatal(err)
		}
		okPebble, err := itPebble.Valid()
		if err != nil {
			t.Fatal(err)
		}
		if !okRocks {
			break
		}
		if !okPebble {
			t.Fatal("expected valid")
		}
		require.Equal(t, itRocks.UnsafeKey(), itPebble.UnsafeKey())
		require.Equal(t, itRocks.UnsafeValue(), itPebble.UnsafeValue())

		if r.Intn(5) == 0 {
			itRocks.NextKey()
			itPebble.NextKey()
		} else {
			itRocks.Next()
			itPebble.Next()
		}
	}
	require.Equal(t, string(sstRocks), string(sstPebble))
}

// TestSSTFileWriterTruncate ensures that sum of the chunks created by
// calling Truncate on a RocksDBSstFileWriter is equivalent to an SST built
// without ever calling Truncate.
func TestSSTFileWriterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Truncate will be used on this writer.
	sst1 := engine.MakeSSTWriter()
	defer sst1.Close()

	// Truncate will not be used on this writer.
	sst2 := engine.MakeSSTWriter()
	defer sst2.Close()

	const keyLen = 10
	const valLen = 950
	ts := hlc.Timestamp{WallTime: 1}
	key := engine.MVCCKey{Key: roachpb.Key(make([]byte, keyLen)), Timestamp: ts}
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

			if len(sst1Chunk) == 0 {
				t.Fatalf("expected non-empty SST chunk during iteration %d", i)
			}
			resBuf1 = append(resBuf1, sst1Chunk...)
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

func BenchmarkWriteRocksSSTable(b *testing.B) {
	b.StopTimer()
	// Writing the SST 10 times keeps size needed for ~10s benchtime under 1gb.
	const valueSize, revisions, ssts = 100, 100, 10
	kvs := makeIntTableKVs(b.N, valueSize, revisions)
	approxUserDataSizePerKV := kvs[b.N/2].Key.EncodedSize() + valueSize
	b.SetBytes(int64(approxUserDataSizePerKV * ssts))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < ssts; i++ {
		_ = makeRocksSST(b, kvs)
	}
	b.StopTimer()
}

func BenchmarkWriteSSTable(b *testing.B) {
	b.StopTimer()
	// Writing the SST 10 times keeps size needed for ~10s benchtime under 1gb.
	const valueSize, revisions, ssts = 100, 100, 10
	kvs := makeIntTableKVs(b.N, valueSize, revisions)
	approxUserDataSizePerKV := kvs[b.N/2].Key.EncodedSize() + valueSize
	b.SetBytes(int64(approxUserDataSizePerKV * ssts))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < ssts; i++ {
		_ = makePebbleSST(b, kvs)
	}
	b.StopTimer()
}
