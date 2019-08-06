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

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
		require.NoError(t, w.Add(kvs[i]))
	}
	sst, err := w.Finish()
	require.NoError(t, err)
	return sst
}
