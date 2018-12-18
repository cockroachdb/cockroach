// Copyright 2018 The Cockroach Authors.
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

package enginepb_test

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func BenchmarkScanDecodeKeyValue(b *testing.B) {
	key := roachpb.Key("blah blah blah")
	ts := hlc.Timestamp{WallTime: int64(1000000)}
	value := []byte("foo foo foo")
	rep := make([]byte, 8)
	keyBytes := engine.EncodeKey(engine.MVCCKey{Key: key, Timestamp: ts})
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
