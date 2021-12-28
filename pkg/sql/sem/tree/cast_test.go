// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// TestCastsVolatilityMatchesPostgres checks that our defined casts match
// Postgres' casts for Volatility.
//
// Dump command below:
// COPY (
//   SELECT c.castsource, c.casttarget, p.provolatile, p.proleakproof
//   FROM pg_cast c JOIN pg_proc p ON (c.castfunc = p.oid)
// ) TO STDOUT WITH CSV DELIMITER '|' HEADER;
func TestCastsVolatilityMatchesPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	csvPath := filepath.Join("testdata", "pg_cast_dump.csv")
	f, err := os.Open(csvPath)
	require.NoError(t, err)

	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '|'

	// Read header row
	_, err = reader.Read()
	require.NoError(t, err)

	type pgCast struct {
		from, to   oid.Oid
		volatility Volatility
	}
	var pgCasts []pgCast

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Len(t, line, 4)

		fromOid, err := strconv.Atoi(line[0])
		require.NoError(t, err)

		toOid, err := strconv.Atoi(line[1])
		require.NoError(t, err)

		provolatile := line[2]
		require.Len(t, provolatile, 1)
		proleakproof := line[3]
		require.Len(t, proleakproof, 1)

		v, err := VolatilityFromPostgres(provolatile, proleakproof[0] == 't')
		require.NoError(t, err)

		pgCasts = append(pgCasts, pgCast{
			from:       oid.Oid(fromOid),
			to:         oid.Oid(toOid),
			volatility: v,
		})
	}

	for src := range castMap {
		for tgt, c := range castMap[src] {
			// Find the corresponding pg cast.
			found := false
			for _, pgCast := range pgCasts {
				if src == pgCast.from && tgt == pgCast.to {
					found = true
					if c.volatility != pgCast.volatility {
						t.Errorf("cast %s::%s has volatility %s; corresponding pg cast has volatility %s",
							oidStr(src), oidStr(tgt), c.volatility, pgCast.volatility,
						)

					}
				}
			}
			if !found && testing.Verbose() {
				t.Logf("cast %s::%s has no corresponding pg cast", oidStr(src), oidStr(tgt))
			}
		}
	}
}

	}
}

// TestCastsFromUnknown verifies that there is a cast from Unknown defined for
// all type families.
func TestCastsFromUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	typs := []*types.T{
		types.MakeBit(2),
		types.MakeArray(types.MakeBit(2)),
		types.Bool,
		types.BoolArray,
		types.MakeChar(2),
		types.MakeArray(types.MakeChar(2)),
		types.Bytes,
		types.BytesArray,
		types.QChar,
		types.MakeArray(types.QChar),
		types.Date,
		types.DateArray,
		types.AnyEnum,
		types.MakeArray(types.AnyEnum),
		types.Float4,
		types.MakeArray(types.Float4),
		types.Float,
		types.FloatArray,
		types.Geography,
		types.MakeArray(types.Geography),
		types.Geometry,
		types.MakeArray(types.Geometry),
		types.INet,
		types.INetArray,
		types.Int2,
		types.MakeArray(types.Int2),
		types.Int4,
		types.MakeArray(types.Int4),
		types.Int,
		types.IntArray,
		types.Interval,
		types.IntervalArray,
		types.Jsonb,
		types.MakeArray(types.Jsonb),
		types.Name,
		types.MakeArray(types.Name),
		types.Decimal,
		types.DecimalArray,
		types.String,
		types.StringArray,
		types.Time,
		types.TimeArray,
		types.Timestamp,
		types.TimestampArray,
		types.TimestampTZ,
		types.TimestampTZArray,
		types.TimeTZ,
		types.TimeTZArray,
		types.Uuid,
		types.UUIDArray,
		types.VarBit,
		types.VarBitArray,
		types.VarChar,
		types.MakeArray(types.VarChar),
		types.Void,
	}

	for _, typ := range typs {
		_, ok := lookupCast(types.Unknown, typ, false /* intervalStyleEnabled */, false /* dateStyleEnabled */)
		if !ok {
			t.Errorf("cast from unknown to %s does not exist", typ.Name())
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
		v, ok := LookupCastVolatility(&from, &to, nil /* sessionData */)
		res := "error"
		if ok {
			res = v.String()
		}
		if res != tc.exp {
			t.Errorf("from: %s  to: %s  expected: %s  got: %s", &from, &to, tc.exp, res)
		}
	}
}

func oidStr(o oid.Oid) string {
	res, ok := oidext.TypeName(o)
	if !ok {
		res = fmt.Sprintf("%d", o)
	}
	return res
}
