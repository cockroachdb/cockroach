// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

func makeIntTableKVs(numKeys, valueSize, maxRevisions int) []storage.MVCCKeyValue {
	prefix := encoding.EncodeUvarintAscending(keys.SystemSQLCodec.TablePrefix(uint32(100)), uint64(1))
	kvs := make([]storage.MVCCKeyValue, numKeys)
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

func makePebbleSST(t testing.TB, kvs []storage.MVCCKeyValue, ingestion bool) []byte {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	f := &storage.MemFile{}
	var w storage.SSTWriter
	if ingestion {
		w = storage.MakeIngestionSSTWriter(ctx, st, f)
	} else {
		w = storage.MakeBackupSSTWriter(ctx, st, f)
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

	testCases := []struct {
		name string
		st   *cluster.Settings
		want sstable.TableFormat
	}{
		{
			name: "before feature gate",
			st: cluster.MakeTestingClusterSettingsWithVersions(
				clusterversion.ByKey(clusterversion.EnablePebbleFormatVersionBlockProperties-1),
				clusterversion.TestingBinaryMinSupportedVersion,
				true,
			),
			want: sstable.TableFormatRocksDBv2,
		},
		{
			name: "at feature gate",
			st: cluster.MakeTestingClusterSettingsWithVersions(
				clusterversion.ByKey(clusterversion.EnablePebbleFormatVersionBlockProperties),
				clusterversion.TestingBinaryMinSupportedVersion,
				true,
			),
			want: sstable.TableFormatPebblev1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			opts := storage.MakeIngestionWriterOptions(ctx, tc.st)
			require.Equal(t, tc.want, opts.TableFormat)
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
