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
	csvPath := filepath.Join("testdata", "pg_cast_provolatile_dump.csv")
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

	oidToFamily := func(o oid.Oid) (_ types.Family, ok bool) {
		t, ok := types.OidToType[o]
		if !ok {
			return 0, false
		}
		return t.Family(), true
	}

	oidStr := func(o oid.Oid) string {
		res, ok := oidext.TypeName(o)
		if !ok {
			res = fmt.Sprintf("%d", o)
		}
		return res
	}

	for _, c := range validCasts {
		if c.volatility == 0 {
			t.Errorf("cast %s::%s has no volatility set", c.from.Name(), c.to.Name())

		}
		if c.ignoreVolatilityCheck {
			continue
		}

		// Look through all pg casts and find any where the Oids map to these
		// families.
		found := false
		for i := range pgCasts {
			fromFamily, fromOk := oidToFamily(pgCasts[i].from)
			toFamily, toOk := oidToFamily(pgCasts[i].to)
			if fromOk && toOk && fromFamily == c.from && toFamily == c.to {
				found = true
				if c.volatility != pgCasts[i].volatility {
					t.Errorf("cast %s::%s has volatility %s; corresponding pg cast %s::%s has volatility %s",
						c.from.Name(), c.to.Name(), c.volatility,
						oidStr(pgCasts[i].from), oidStr(pgCasts[i].to), pgCasts[i].volatility,
					)
				}
			}
		}
		if !found && testing.Verbose() {
			t.Logf("cast %s::%s has no corresponding pg cast", c.from.Name(), c.to.Name())
		}
	}
}

// TestCastsFromUnknown verifies that there is a cast from Unknown defined for
// all type families.
func TestCastsFromUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for v := range types.Family_name {
		switch fam := types.Family(v); fam {
		case types.UnknownFamily, types.AnyFamily:
			// These type families are exceptions.

		default:
			cast := lookupCast(types.UnknownFamily, fam)
			if cast == nil {
				t.Errorf("cast from Unknown to %s does not exist", fam)
			}
		}
	}
}
