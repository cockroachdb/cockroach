// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	k := func(s string, ts hlc.Timestamp) storage.MVCCKey {
		return storage.MVCCKey{Key: []byte(s), Timestamp: ts}
	}
	var missing roachpb.Value
	v := func(s string, ts hlc.Timestamp) roachpb.Value {
		v := roachpb.MakeValueFromString(s)
		v.Timestamp = ts
		return v
	}
	ts := func(i int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(i)}
	}

	e, err := MakeEngine()
	require.NoError(t, err)
	defer e.Close()
	assert.Equal(t, missing, e.Get(roachpb.Key(`a`), ts(1)))
	e.Put(k(`a`, ts(1)), roachpb.MakeValueFromString(`a-1`).RawBytes)
	e.Put(k(`a`, ts(2)), roachpb.MakeValueFromString(`a-2`).RawBytes)
	e.Put(k(`b`, ts(2)), roachpb.MakeValueFromString(`b-2`).RawBytes)
	assert.Equal(t, v(`a-2`, ts(2)), e.Get(roachpb.Key(`a`), ts(3)))
	assert.Equal(t, v(`a-2`, ts(2)), e.Get(roachpb.Key(`a`), ts(2)))
	assert.Equal(t, v(`a-1`, ts(1)), e.Get(roachpb.Key(`a`), ts(2).Prev()))
	assert.Equal(t, v(`a-1`, ts(1)), e.Get(roachpb.Key(`a`), ts(1)))
	assert.Equal(t, missing, e.Get(roachpb.Key(`a`), ts(1).Prev()))
	assert.Equal(t, v(`b-2`, ts(2)), e.Get(roachpb.Key(`b`), ts(3)))
	assert.Equal(t, v(`b-2`, ts(2)), e.Get(roachpb.Key(`b`), ts(2)))
	assert.Equal(t, missing, e.Get(roachpb.Key(`b`), ts(1)))

	assert.Equal(t, strings.TrimSpace(`
"a" 0.000000002,0 -> /BYTES/a-2
"a" 0.000000001,0 -> /BYTES/a-1
"b" 0.000000002,0 -> /BYTES/b-2
	`), e.DebugPrint(""))
}
