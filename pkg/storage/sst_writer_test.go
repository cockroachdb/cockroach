// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func makeIntTableKVs(numKeys, valueSize, maxRevisions int) []MVCCKeyValue {
	prefix := encoding.EncodeUvarintAscending(keys.SystemSQLCodec.TablePrefix(uint32(100)), uint64(1))
	kvs := make([]MVCCKeyValue, numKeys)
	r, _ := randutil.NewTestRand()

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

func makePebbleSST(t testing.TB, kvs []MVCCKeyValue, ingestion bool) []byte {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	f := &MemObject{}
	var w SSTWriter
	if ingestion {
		w = MakeIngestionSSTWriter(ctx, st, f)
	} else {
		w = MakeTransportSSTWriter(ctx, st, &f.Buffer)
	}
	defer w.Close()

	for i := range kvs {
		if err := w.Put(kvs[i].Key, kvs[i].Value); err != nil {
			t.Fatal(err)
		}
	}
	err := w.Finish()
	require.NoError(t, err)
	return f.Data()
}

func TestMakeIngestionWriterOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type want struct {
		format             sstable.TableFormat
		disableValueBlocks bool
	}
	testCases := []struct {
		name string
		st   *cluster.Settings
		want want
	}{
		{
			name: "with virtual sstables",
			st: func() *cluster.Settings {
				st := cluster.MakeTestingClusterSettings()
				IngestionValueBlocksEnabled.Override(context.Background(), &st.SV, true)
				ColumnarBlocksEnabled.Override(context.Background(), &st.SV, false)
				return st
			}(),
			want: want{
				format:             sstable.TableFormatPebblev4,
				disableValueBlocks: false,
			},
		},
		{
			name: "disable value blocks",
			st: func() *cluster.Settings {
				st := cluster.MakeTestingClusterSettings()
				IngestionValueBlocksEnabled.Override(context.Background(), &st.SV, false)
				ColumnarBlocksEnabled.Override(context.Background(), &st.SV, false)
				return st
			}(),
			want: want{
				format:             sstable.TableFormatPebblev4,
				disableValueBlocks: true,
			},
		},
		{
			name: "enable columnar blocks",
			st: func() *cluster.Settings {
				st := cluster.MakeTestingClusterSettings()
				IngestionValueBlocksEnabled.Override(context.Background(), &st.SV, false)
				ColumnarBlocksEnabled.Override(context.Background(), &st.SV, true)
				return st
			}(),
			want: want{
				format:             sstable.TableFormatPebblev5,
				disableValueBlocks: true,
			},
		},
		{
			name: "enable columnar blocks with value blocks",
			st: func() *cluster.Settings {
				st := cluster.MakeTestingClusterSettings()
				IngestionValueBlocksEnabled.Override(context.Background(), &st.SV, true)
				ColumnarBlocksEnabled.Override(context.Background(), &st.SV, true)
				return st
			}(),
			want: want{
				format:             sstable.TableFormatPebblev5,
				disableValueBlocks: false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			opts := MakeIngestionWriterOptions(ctx, tc.st)
			require.Equal(t, tc.want.format, opts.TableFormat)
			require.Equal(t, tc.want.disableValueBlocks, opts.DisableValueBlocks)
		})
	}
}

func TestSSTWriterRangeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemObject{}
	sst := MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sst.Close()

	require.NoError(t, sst.Put(pointKey("a", 1), stringValueRaw("foo")))
	require.EqualValues(t, 9, sst.DataSize)

	require.NoError(t, sst.PutMVCCRangeKey(rangeKey("a", "e", 2), tombstoneLocalTS(1)))
	require.EqualValues(t, 20, sst.DataSize)

	require.NoError(t, sst.PutEngineRangeKey(roachpb.Key("f"), roachpb.Key("g"),
		wallTSRaw(2), tombstoneLocalTSRaw(1)))
	require.EqualValues(t, 31, sst.DataSize)

	require.NoError(t, sst.Finish())

	iter, err := NewMemSSTIterator(sstFile.Bytes(), false /* verify */, IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer iter.Close()

	require.Equal(t, []interface{}{
		rangeKV("a", "e", 2, tombstoneLocalTS(1)),
		pointKV("a", 1, "foo"),
		rangeKV("f", "g", 2, tombstoneLocalTS(1)),
	}, scanIter(t, iter))
}

func TestSSTWriterOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// makeCompressionWriterOpt returns a new SSTWriterOption that uses the
	// cluster setting that has been set to the given algorithm as the basis for
	// determining which compression algorithm is used when the SSTWriterOption
	// runs over an sstable.WriterOptions.
	makeCompressionWriterOpt := func(alg compressionAlgorithm) SSTWriterOption {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		CompressionAlgorithmStorage.Override(ctx, &st.SV, alg)
		return WithCompressionFromClusterSetting(ctx, st, CompressionAlgorithmStorage)
	}

	tcs := []struct {
		name      string
		writerOpt SSTWriterOption
		wantFunc  func(*testing.T, *sstable.WriterOptions)
	}{
		{
			"disable value blocks",
			WithValueBlocksDisabled,
			func(t *testing.T, opts *sstable.WriterOptions) {
				require.True(t, opts.DisableValueBlocks)
			},
		},
		{
			"with snappy compression",
			makeCompressionWriterOpt(compressionAlgorithmSnappy),
			func(t *testing.T, opts *sstable.WriterOptions) {
				require.Equal(t, block.SnappyCompression, opts.Compression)
			},
		},
		{
			"with zstd compression",
			makeCompressionWriterOpt(compressionAlgorithmZstd),
			func(t *testing.T, opts *sstable.WriterOptions) {
				require.Equal(t, block.ZstdCompression, opts.Compression)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			opts := &sstable.WriterOptions{}
			tc.writerOpt(opts)
			tc.wantFunc(t, opts)
		})
	}
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
		_ = makePebbleSST(b, kvs, true /* ingestion */)
	}
	b.StopTimer()
}
