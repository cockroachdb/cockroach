// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storageutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// MakeSST builds a binary in-memory SST from the given KVs, which can be both
// MVCCKeyValue or MVCCRangeKeyValue. It returns the binary SST data as well as
// the start and end (exclusive) keys of the SST.
func MakeSST(
	t *testing.T, st *cluster.Settings, kvs []interface{},
) ([]byte, roachpb.Key, roachpb.Key) {
	t.Helper()

	sstFile := &storage.MemObject{}
	writer := storage.MakeIngestionSSTWriter(context.Background(), st, sstFile)
	defer writer.Close()

	start, end := keys.MaxKey, keys.MinKey
	for _, kvI := range kvs {
		var s, e roachpb.Key
		switch kv := kvI.(type) {
		case storage.MVCCKeyValue:
			if kv.Key.Timestamp.IsEmpty() {
				v, err := protoutil.Marshal(&enginepb.MVCCMetadata{RawBytes: kv.Value})
				require.NoError(t, err)
				require.NoError(t, writer.PutUnversioned(kv.Key.Key, v))
			} else {
				require.NoError(t, writer.PutRawMVCC(kv.Key, kv.Value))
			}
			s, e = kv.Key.Key, kv.Key.Key.Next()

		case storage.MVCCRangeKeyValue:
			v, err := storage.DecodeMVCCValue(kv.Value)
			require.NoError(t, err)
			require.NoError(t, writer.PutMVCCRangeKey(kv.RangeKey, v))
			s, e = kv.RangeKey.StartKey, kv.RangeKey.EndKey

		default:
			t.Fatalf("invalid KV type %T", kv)
		}
		if s.Compare(start) < 0 {
			start = s
		}
		if e.Compare(end) > 0 {
			end = e
		}
	}

	require.NoError(t, writer.Finish())
	writer.Close()

	return sstFile.Data(), start, end
}
