// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cast

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// TestCastsMatchPostgres checks that the Volatility and Context of our
// defined casts match Postgres' casts.
//
// The command for generating pg_cast_dump.csv from psql is below. We ignore
// types that we do not support, and we ignore geospatial types because they are
// an extension of Postgres and have no official OIDs.
//
//	\copy (
//	  WITH ignored_types AS (
//	    SELECT t::regtype::oid t
//	    FROM (VALUES
//	      ('geography'),
//	      ('geometry'),
//	      ('box2d'),
//	      ('box3d'),
//	      ('tstzmultirange'),
//	      ('int4multirange'),
//	      ('int8multirange'),
//	      ('tstzmultirange'),
//	      ('tsmultirange'),
//	      ('datemultirange'),
//	      ('nummultirange')
//	    ) AS types(t)
//	  )
//	  SELECT
//	    c.castsource,
//	    c.casttarget,
//	    p.provolatile,
//	    p.proleakproof,
//	    c.castcontext,
//	    substring(version(), 'PostgreSQL (\d+\.\d+)') pg_version
//	  FROM pg_cast c JOIN pg_proc p ON (c.castfunc = p.oid)
//	  WHERE
//	    c.castsource NOT IN (SELECT t FROM ignored_types)
//	    AND c.casttarget NOT IN (SELECT t FROM ignored_types)
//	  ORDER BY 1, 2
//	) TO pg_cast_dump.csv WITH CSV DELIMITER '|' HEADER;
func TestCastsMatchPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	csvPath := datapathutils.TestDataPath(t, "pg_cast_dump.csv")
	f, err := os.Open(csvPath)
	require.NoError(t, err)

	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '|'

	// Read header row
	_, err = reader.Read()
	require.NoError(t, err)

	type pgCastKey struct {
		from, to oid.Oid
	}

	type pgCastValue struct {
		volatility volatility.V
		context    Context
	}

	pgCastMap := make(map[pgCastKey]pgCastValue)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Len(t, line, 6)

		fromOid, err := strconv.Atoi(line[0])
		require.NoError(t, err)

		toOid, err := strconv.Atoi(line[1])
		require.NoError(t, err)

		provolatile := line[2]
		require.Len(t, provolatile, 1)
		proleakproof := line[3]
		require.Len(t, proleakproof, 1)
		castcontext := line[4]
		require.Len(t, castcontext, 1)

		v, err := volatility.FromPostgres(provolatile, proleakproof[0] == 't')
		require.NoError(t, err)

		c, err := castContextFromPostgres(castcontext)
		require.NoError(t, err)

		pgCastMap[pgCastKey{oid.Oid(fromOid), oid.Oid(toOid)}] = pgCastValue{v, c}
	}

	for src := range castMap {
		for tgt, c := range castMap[src] {
			// Find the corresponding pg cast.
			pgCast, ok := pgCastMap[pgCastKey{src, tgt}]
			if !ok && testing.Verbose() {
				t.Logf("cast %s::%s has no corresponding pg cast", oidStr(src), oidStr(tgt))
			}
			if ok && c.Volatility != pgCast.volatility {
				t.Errorf("cast %s::%s has Volatility %s; corresponding pg cast has Volatility %s",
					oidStr(src), oidStr(tgt), c.Volatility, pgCast.volatility,
				)
			}
			if ok && c.MaxContext != pgCast.context {
				t.Errorf("cast %s::%s has MaxContext %s; corresponding pg cast has context %s",
					oidStr(src), oidStr(tgt), c.MaxContext, pgCast.context,
				)
			}
		}
	}
}

// castContextFromPostgres returns a Context that matches the castcontext
// setting in Postgres's pg_cast table.
func castContextFromPostgres(castcontext string) (Context, error) {
	switch castcontext {
	case "e":
		return ContextExplicit, nil
	case "a":
		return ContextAssignment, nil
	case "i":
		return ContextImplicit, nil
	default:
		return 0, errors.AssertionFailedf("invalid castcontext %s", castcontext)
	}
}

func oidStr(o oid.Oid) string {
	res, ok := oidext.TypeName(o)
	if !ok {
		res = fmt.Sprintf("%d", o)
	}
	return res
}

// TestCastsFromUnknown verifies that there is a cast from Unknown defined for
// all types.
func TestCastsFromUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, typ := range types.OidToType {
		_, ok := LookupCast(types.Unknown, typ)
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
			exp:  "leakproof",
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
			to:   []*types.T{types.AnyElement},
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
		v, ok := LookupCastVolatility(&from, &to)
		res := "error"
		if ok {
			res = v.String()
		}
		if res != tc.exp {
			t.Errorf("from: %s  to: %s  expected: %s  got: %s", &from, &to, tc.exp, res)
		}
	}
}
