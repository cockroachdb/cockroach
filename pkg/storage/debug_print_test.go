// Copyright 2019 The Cockroach Authors.
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

package storage

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStringifyWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var wb storagepb.WriteBatch
	swb := stringifyWriteBatch(wb)
	if str, expStr := swb.String(), "failed to stringify write batch (): batch repr too small: 0 < 12"; str != expStr {
		t.Errorf("expected %q for stringified write batch; got %q", expStr, str)
	}

	builder := engine.RocksDBBatchBuilder{}
	builder.Put(engine.MVCCKey{
		Key:       roachpb.Key("/db1"),
		Timestamp: hlc.Timestamp{WallTime: math.MaxInt64},
	}, []byte("test value"))
	wb.Data = builder.Finish()
	swb = stringifyWriteBatch(wb)
	if str, expStr := swb.String(), "Put: 9223372036.854775807,0 \"/db1\" (0x2f646231007fffffffffffffff09): \"test value\"\n"; str != expStr {
		t.Errorf("expected %q for stringified write batch; got %q", expStr, str)
	}
}
