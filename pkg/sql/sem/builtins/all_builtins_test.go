// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverloadsHaveVolatility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for name, builtin := range builtins {
		for idx, overload := range builtin.overloads {
			assert.NotEqual(
				t,
				tree.Volatility(0),
				overload.Volatility,
				"function %s at overload idx %d has no Volatility set",
				name,
				idx,
			)
		}
	}
}

// TestOverloadsVolatilityMatchesPostgres that our overloads match Postgres'
// overloads for Volatility.
// Dump command below:
// COPY (SELECT proname, args, rettype, provolatile, proleakproof FROM (
//   SELECT
//     lhs.oid, proname, pg2.typname as rettype, ARRAY_AGG(pg1.typname) as args, provolatile, proleakproof
//     FROM
//     (select oid, proname, unnest(proargtypes) as typ, proargnames, prorettype, provolatile, proleakproof from pg_proc) AS lhs
//     JOIN pg_type AS pg1 ON (lhs.typ = pg1.oid)
//     JOIN pg_type AS pg2 ON (lhs.prorettype = pg2.oid) GROUP BY lhs.oid, proname, pg2.typname, provolatile, proleakproof) a
//     ORDER BY proname, args
// ) TO '/tmp/pg_proc_provolatile_dump.csv' WITH CSV DELIMITER '|' HEADER;
func TestOverloadsVolatilityMatchesPostgres(t *testing.T) {
	defer leaktest.AfterTest(t)()
	csvPath := filepath.Join("testdata", "pg_proc_provolatile_dump.csv")
	f, err := os.Open(csvPath)
	require.NoError(t, err)

	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '|'

	// Read header row
	_, err = reader.Read()
	require.NoError(t, err)

	type pgOverload struct {
		families   []types.Family
		volatility tree.Volatility
	}

	// Maps proname -> equivalent pg overloads.
	foundVolatilities := map[string][]pgOverload{}
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		proname := line[0]
		provolatile := line[3]
		require.Len(t, provolatile, 1)
		proleakproof := line[4]
		require.Len(t, proleakproof, 1)
		proargs := line[1]
		families := []types.Family{}
		// Remove start and end '{' and '}' characters.
		badType := false
		for _, typname := range strings.Split(proargs[1:len(proargs)-1], ",") {
			typ, _, _ := types.TypeForNonKeywordTypeName(typname)
			if typ == nil {
				badType = true
				break
			}
			families = append(families, typ.Family())
		}
		if badType {
			continue
		}
		v, err := tree.VolatilityFromPostgres(provolatile, proleakproof[0] == 't')
		require.NoError(t, err)
		foundVolatilities[proname] = append(
			foundVolatilities[proname],
			pgOverload{
				volatility: v,
				families:   families,
			},
		)
	}

	// findOverloadVolatility checks if the volatility is found in the
	// foundVolatilities mapping and returns the volatility and true if found.
	findOverloadVolatility := func(name string, overload tree.Overload) (tree.Volatility, bool) {
		v, ok := foundVolatilities[name]
		if !ok {
			return tree.Volatility(0), false
		}
		for _, postgresOverload := range v {
			if len(postgresOverload.families) != overload.Types.Length() {
				continue
			}
			matches := true
			for i, postgresFamily := range postgresOverload.families {
				if postgresFamily != overload.Types.GetAt(i).Family() {
					matches = false
					break
				}
			}
			if matches {
				return postgresOverload.volatility, true
			}
		}
		return tree.Volatility(0), false
	}

	// Check each builtin against Postgres.
	for name, builtin := range builtins {
		for idx, overload := range builtin.overloads {
			if overload.IgnoreVolatilityCheck {
				continue
			}
			postgresVolatility, found := findOverloadVolatility(name, overload)
			if !found {
				continue
			}
			assert.Equal(
				t,
				postgresVolatility,
				overload.Volatility,
				`overload %s at idx %d has volatility %s not which does not match postgres %s`,
				name,
				idx,
				overload.Volatility,
				postgresVolatility,
			)
		}
	}
}
