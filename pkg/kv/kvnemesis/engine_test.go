// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
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
	e.Put(k(`b`, ts(3)), nil)
	e.Put(k(`c`, ts(4)), nil)
	e.Put(k(`d`, ts(4)), roachpb.MakeValueFromString(`d-4`).RawBytes)
	e.Put(k(`e`, ts(4)), roachpb.MakeValueFromString(`e-4`).RawBytes)
	e.Put(k(`d`, ts(5)), nil)
	e.DeleteRange(roachpb.Key("f"), roachpb.Key("g"), ts(7), nil)
	assert.Equal(t, v(`a-2`, ts(2)), e.Get(roachpb.Key(`a`), ts(3)))
	assert.Equal(t, v(`a-2`, ts(2)), e.Get(roachpb.Key(`a`), ts(2)))
	assert.Equal(t, v(`a-1`, ts(1)), e.Get(roachpb.Key(`a`), ts(2).Prev()))
	assert.Equal(t, v(`a-1`, ts(1)), e.Get(roachpb.Key(`a`), ts(1)))
	assert.Equal(t, missing, e.Get(roachpb.Key(`a`), ts(1).Prev()))
	assert.Equal(t, v(`b-2`, ts(2)), e.Get(roachpb.Key(`b`), ts(3).Prev()))
	assert.Equal(t, v(`b-2`, ts(2)), e.Get(roachpb.Key(`b`), ts(2)))
	assert.Equal(t, missing, e.Get(roachpb.Key(`b`), ts(1)))
	assert.Equal(t, missing, e.Get(roachpb.Key(`b`), ts(4)))
	assert.Equal(t, v(`d-4`, ts(4)), e.Get(roachpb.Key(`d`), ts(4)))
	assert.Equal(t, missing, e.Get(roachpb.Key(`d`), ts(5)))
	assert.Equal(t, v(`e-4`, ts(4)), e.Get(roachpb.Key(`e`), ts(5)))

	_, _ = k, v
	echotest.Require(t, e.DebugPrint(""), datapathutils.TestDataPath(t, t.Name(), "output.txt"))
}
