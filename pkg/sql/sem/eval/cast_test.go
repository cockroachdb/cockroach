// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/internal/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCastsFromUnknown verifies that there is a cast from Unknown defined for
// all types.
func TestCastsFromUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, typ := range types.OidToType {
		_, ok := cast.LookupCast(types.Unknown, typ, false /* intervalStyleEnabled */, false /* dateStyleEnabled */)
		if !ok {
			t.Errorf("cast from Unknown to %s does not exist", typ.String())
		}
	}
}

func TestTupleCastVolatility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		from, to []*types.T
		exp      string
	}{
		{
			from: nil,
			to:   nil,
			exp:  "leak-proof",
		},
		{
			from: nil,
			to:   []*types.T{types.Int},
			exp:  "error",
		},
		{
			from: []*types.T{types.Int},
			to:   []*types.T{types.Int},
			exp:  "immutable",
		},
		{
			from: []*types.T{types.Int, types.Int},
			to:   []*types.T{types.Any},
			exp:  "stable",
		},
		{
			from: []*types.T{types.TimestampTZ},
			to:   []*types.T{types.Date},
			exp:  "stable",
		},
		{
			from: []*types.T{types.Int, types.TimestampTZ},
			to:   []*types.T{types.Int, types.Date},
			exp:  "stable",
		},
	}

	for _, tc := range testCases {
		from := *types.EmptyTuple
		from.InternalType.TupleContents = tc.from
		to := *types.EmptyTuple
		to.InternalType.TupleContents = tc.to
		v, ok := tree.LookupCastVolatility(&from, &to, nil /* sessionData */)
		res := "error"
		if ok {
			res = v.String()
		}
		if res != tc.exp {
			t.Errorf("from: %s  to: %s  expected: %s  got: %s", &from, &to, tc.exp, res)
		}
	}
}

func TestCastStringToRegClassTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in       string
		expected tree.TableName
	}{
		{"a", tree.MakeUnqualifiedTableName("a")},
		{`a"`, tree.MakeUnqualifiedTableName(`a"`)},
		{`"a""".bB."cD" `, tree.MakeTableNameWithSchema(`a"`, "bb", "cD")},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := castStringToRegClassTableName(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.expected, out)
		})
	}

	errorTestCases := []struct {
		in            string
		expectedError string
	}{
		{"a.b.c.d", "too many components: a.b.c.d"},
		{"", `invalid table name: `},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.in, func(t *testing.T) {
			_, err := castStringToRegClassTableName(tc.in)
			require.EqualError(t, err, tc.expectedError)
		})
	}

}
