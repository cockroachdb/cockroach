// Copyright 2019 The Cockroach Authors.
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
