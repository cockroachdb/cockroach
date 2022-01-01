// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func genColumnType() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		columnType := randgen.RandColumnType(genParams.Rng)
		return gopter.NewGenResult(columnType, gopter.NoShrinker)
	}
}

func genDatum() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(randgen.RandDatum(genParams.Rng, randgen.RandColumnType(genParams.Rng),
			false), gopter.NoShrinker)
	}
}

func genDatumWithType(columnType interface{}) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		datum := randgen.RandDatum(genParams.Rng, columnType.(*types.T), false)
		return gopter.NewGenResult(datum, gopter.NoShrinker)
	}
}

func TestEncodeTableValue(t *testing.T) {
	a := &tree.DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)
	var scratch []byte
	properties.Property("roundtrip", prop.ForAll(
		func(d tree.Datum) string {
			b, err := rowenc.EncodeTableValue(nil, 0, d, scratch)
			if err != nil {
				return "error: " + err.Error()
			}
			newD, leftoverBytes, err := rowenc.DecodeTableValue(a, d.ResolvedType(), b)
			if len(leftoverBytes) > 0 {
				return "Leftover bytes"
			}
			if err != nil {
				return "error: " + err.Error()
			}
			if newD.Compare(ctx, d) != 0 {
				return "unequal"
			}
			return ""
		},
		genDatum(),
	))
	properties.TestingRun(t)
}

func TestMarshalColumnValueRoundtrip(t *testing.T) {
	a := &tree.DatumAlloc{}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10000
	properties := gopter.NewProperties(parameters)

	properties.Property("roundtrip",
		prop.ForAll(
			func(typ *types.T) string {
				d, ok := genDatumWithType(typ).Sample()
				if !ok {
					return "error generating datum"
				}
				datum := d.(tree.Datum)
				value, err := rowenc.MarshalColumnTypeValue("testcol", typ, datum)
				if err != nil {
					return "error marshaling: " + err.Error()
				}
				outDatum, err := rowenc.UnmarshalColumnValue(a, typ, value)
				if err != nil {
					return "error unmarshaling: " + err.Error()
				}
				if datum.Compare(ctx, outDatum) != 0 {
					return fmt.Sprintf("datum didn't roundtrip.\ninput: %v\noutput: %v", datum, outDatum)
				}
				return ""
			},
			genColumnType(),
		),
	)
	properties.TestingRun(t)
}

// TestDecodeTableValueOutOfRangeTimestamp deliberately tests out of range timestamps
// can still be decoded from disk. See #46973.
func TestDecodeTableValueOutOfRangeTimestamp(t *testing.T) {
	for _, d := range []tree.Datum{
		&tree.DTimestamp{Time: timeutil.Unix(-9223372036854775808, 0).In(time.UTC)},
		&tree.DTimestampTZ{Time: timeutil.Unix(-9223372036854775808, 0).In(time.UTC)},
	} {
		t.Run(d.String(), func(t *testing.T) {
			var b []byte
			colID := descpb.ColumnID(1)
			encoded, err := rowenc.EncodeTableValue(b, colID, d, []byte{})
			require.NoError(t, err)
			a := &tree.DatumAlloc{}
			decoded, _, err := rowenc.DecodeTableValue(a, d.ResolvedType(), encoded)
			require.NoError(t, err)
			require.Equal(t, d, decoded)
		})
	}
}

// This test ensures that decoding a tuple value with a specific, labeled tuple
// type preserves the labels.
func TestDecodeTupleValueWithType(t *testing.T) {
	tupleType := types.MakeLabeledTuple([]*types.T{types.Int, types.String}, []string{"a", "b"})
	datum := tree.NewDTuple(tupleType, tree.NewDInt(tree.DInt(1)), tree.NewDString("foo"))
	buf, err := rowenc.EncodeTableValue(nil, descpb.NoColumnID, datum, nil)
	if err != nil {
		t.Fatal(err)
	}
	da := tree.DatumAlloc{}
	var decoded tree.Datum
	decoded, _, err = rowenc.DecodeTableValue(&da, tupleType, buf)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, decoded, datum)
}
