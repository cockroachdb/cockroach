// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb_test

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func BenchmarkScanDecodeKeyValue(b *testing.B) {
	key := roachpb.Key("blah blah blah")
	ts := hlc.Timestamp{WallTime: int64(1000000)}
	value := []byte("foo foo foo")
	rep := make([]byte, 8)
	keyBytes := storage.EncodeKey(storage.MVCCKey{Key: key, Timestamp: ts})
	binary.LittleEndian.PutUint64(rep, uint64(len(keyBytes)<<32)|uint64(len(value)))
	rep = append(rep, keyBytes...)
	rep = append(rep, value...)
	b.Run("getTs=true", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var err error
			_, _, _, _, err = enginepb.ScanDecodeKeyValue(rep)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("getTs=false", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var err error
			_, _, _, err = enginepb.ScanDecodeKeyValueNoTS(rep)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
