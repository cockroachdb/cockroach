// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk_test

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/bulk"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func makeIntTableKVs(t testing.TB, numKeys, valueSize, maxRevisions int) []engine.MVCCKeyValue {
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
	w := bulk.MakeSSTWriter()
	defer w.Close()

	for i := range kvs {
		if err := w.Add(kvs[i]); err != nil {
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

	kvs := makeIntTableKVs(t, numKeys, valueSize, revisions)
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

func BenchmarkWriteSSTable(b *testing.B) {
	b.StopTimer()
	// Writing the SST 10 times keeps size needed for ~10s benchtime under 1gb.
	const valueSize, revisions, ssts = 100, 100, 10
	kvs := makeIntTableKVs(b, b.N, valueSize, revisions)
	approxUserDataSizePerKV := kvs[b.N/2].Key.EncodedSize() + valueSize
	b.SetBytes(int64(approxUserDataSizePerKV * ssts))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < ssts; i++ {
		_ = makePebbleSST(b, kvs)
	}
}
