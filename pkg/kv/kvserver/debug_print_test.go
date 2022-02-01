// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestStringifyWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var wb kvserverpb.WriteBatch
	swb := stringifyWriteBatch(wb)
	if str, expStr := swb.String(), "failed to stringify write batch (): batch repr too small: 0 < 12"; str != expStr {
		t.Errorf("expected %q for stringified write batch; got %q", expStr, str)
	}

	batch := pebble.Batch{}
	require.NoError(t, batch.Set(storage.EncodeMVCCKey(storage.MVCCKey{
		Key:       roachpb.Key("/db1"),
		Timestamp: hlc.Timestamp{WallTime: math.MaxInt64},
	}), []byte("test value"), nil /* WriteOptions */))
	wb.Data = batch.Repr()
	swb = stringifyWriteBatch(wb)
	if str, expStr := swb.String(), "Put: 9223372036.854775807,0 \"/db1\" (0x2f646231007fffffffffffffff09): \"test value\"\n"; str != expStr {
		t.Errorf("expected %q for stringified write batch; got %q", expStr, str)
	}

	var err error
	batch = pebble.Batch{}
	encodedKey, err := hex.DecodeString("017a6b12c089f704918df70bee8800010003623a9318c0384d07a6f22b858594df6012")
	require.NoError(t, err)
	err = batch.SingleDelete(encodedKey, nil)
	require.NoError(t, err)
	wb.Data = batch.Repr()
	swb = stringifyWriteBatch(wb)
	if str, expStr := swb.String(), "Single Delete: /Local/Lock/Intent/Table/56/1/1169/5/3054/0 "+
		"03623a9318c0384d07a6f22b858594df60 (0x017a6b12c089f704918df70bee8800010003623a9318c0384d07a6f22b858594df6012): \n"; str != expStr {
		t.Errorf("expected %q for stringified write batch; got %q", expStr, str)
	}
}
