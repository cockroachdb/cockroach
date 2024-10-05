// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type floatAlias float32
type boolAlias bool
type stringAlias string

func TestGolangQueryArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Each test case pairs an arbitrary value and tree.Datum which has the same
	// type
	testCases := []struct {
		value        interface{}
		expectedType *types.T
	}{
		// Null type.
		{nil, types.Unknown},
		{[]byte(nil), types.Unknown},

		// Bool type.
		{true, types.Bool},

		// Primitive Integer types.
		{int(1), types.Int},
		{int8(1), types.Int},
		{int16(1), types.Int},
		{int32(1), types.Int},
		{int64(1), types.Int},
		{uint(1), types.Int},
		{uint8(1), types.Int},
		{uint16(1), types.Int},
		{uint32(1), types.Int},
		{uint64(1), types.Int},

		// Primitive Float types.
		{float32(1.0), types.Float},
		{float64(1.0), types.Float},

		// Decimal type.
		{apd.New(55, 1), types.Decimal},

		// String type.
		{"test", types.String},

		// Bytes type.
		{[]byte("abc"), types.Bytes},

		// Interval and timestamp.
		{time.Duration(1), types.Interval},
		{timeutil.Now(), types.Timestamp},

		// Primitive type aliases.
		{roachpb.NodeID(1), types.Int},
		{descpb.ID(1), types.Int},
		{floatAlias(1), types.Float},
		{boolAlias(true), types.Bool},
		{stringAlias("string"), types.String},

		// Byte slice aliases.
		{roachpb.Key("key"), types.Bytes},
		{roachpb.RKey("key"), types.Bytes},

		// Bit array.
		{bitarray.MakeBitArrayFromInt64(8, 58, 7), types.VarBit},
	}

	for i, tcase := range testCases {
		datums, err := golangFillQueryArguments(tcase.value)
		require.NoError(t, err)
		if len(datums) != 1 {
			t.Fatalf("expected 1 datum, got: %d", len(datums))
		}
		d := datums[0]
		if a, e := d.ResolvedType(), tcase.expectedType; !a.Equal(e) {
			t.Errorf("case %d failed: expected type %s, got %s", i, e.String(), a.String())
		}
	}
}
