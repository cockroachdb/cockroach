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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestGeoBuiltinsInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for k, builtin := range geoBuiltins {
		t.Run(k, func(t *testing.T) {
			for i, overload := range builtin.overloads {
				t.Run(strconv.Itoa(i+1), func(t *testing.T) {
					infoFirstLine := strings.Trim(strings.Split(overload.Info, "\n\n")[0], "\t\n ")
					require.True(t, infoFirstLine[len(infoFirstLine)-1] == '.', "first line of info must end with a `.` character")
					require.True(t, unicode.IsUpper(rune(infoFirstLine[0])), "first character of info start with an uppercase letter.")
				})
			}
		})
	}
}

// TestGeoBuiltinsPointEmptyArgs tests POINT EMPTY arguments do not cause panics.
func TestGeoBuiltinsPointEmptyArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	emptyGeometry, err := tree.ParseDGeometry("POINT EMPTY")
	require.NoError(t, err)
	emptyGeography, err := tree.ParseDGeography("POINT EMPTY")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(0))
	for k, builtin := range geoBuiltins {
		t.Run(k, func(t *testing.T) {
			for i, overload := range builtin.overloads {
				t.Run("overload_"+strconv.Itoa(i+1), func(t *testing.T) {
					for overloadIdx := 0; overloadIdx < overload.Types.Length(); overloadIdx++ {
						switch overload.Types.GetAt(overloadIdx).Family() {
						case types.GeometryFamily, types.GeographyFamily:
							t.Run("idx_"+strconv.Itoa(overloadIdx), func(t *testing.T) {
								var datums tree.Datums
								for i := 0; i < overload.Types.Length(); i++ {
									if i == overloadIdx {
										switch overload.Types.GetAt(i).Family() {
										case types.GeometryFamily:
											datums = append(datums, emptyGeometry)
										case types.GeographyFamily:
											datums = append(datums, emptyGeography)
										default:
											panic("unexpected condition")
										}
									} else {
										datums = append(datums, randgen.RandDatum(rng, overload.Types.GetAt(i), false))
									}
								}
								var call strings.Builder
								call.WriteString(k)
								call.WriteByte('(')
								for i, arg := range datums {
									if i > 0 {
										call.WriteString(", ")
									}
									call.WriteString(arg.String())
								}
								call.WriteByte(')')
								t.Logf("calling: %s", call.String())
								if overload.Fn != nil {
									_, _ = overload.Fn.(eval.FnOverload)(&eval.Context{}, datums)
								} else if overload.Generator != nil {
									_, _ = overload.Generator.(eval.GeneratorOverload)(&eval.Context{}, datums)
								} else if overload.GeneratorWithExprs != nil {
									exprs := make(tree.Exprs, len(datums))
									for i := range datums {
										exprs[i] = datums[i]
									}
									_, _ = overload.GeneratorWithExprs.(eval.GeneratorWithExprsOverload)(&eval.Context{}, exprs)
								}
							})
						}
					}
				})
			}
		})
	}
}

func mustParseDFloat(t testing.TB, in string) tree.Datum {
	d, err := tree.ParseDFloat(in)
	require.NoError(t, err)

	return d
}

func mustParseDGeometry(t testing.TB, in string, srid int) tree.Datum {
	d, err := tree.ParseDGeometry(in)
	require.NoError(t, err)

	withSRID, err := d.CloneWithSRID(geopb.SRID(srid))
	require.NoError(t, err)

	return tree.NewDGeometry(withSRID)
}

type multiArgsBuiltinFixture struct {
	Title       string
	Builtin     tree.Overload
	Args        tree.Datums
	Expected    tree.Datum
	ExpectError bool
}

func TestGeoBuiltinsSTMakeEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// polygonGeometry, err := tree.ParseDGeometry("POLYGON((170 50,170 72,-130 72,-130 50,170 50))")
	for _, toPin := range []multiArgsBuiltinFixture{
		{
			Title:   "st_makeenvelope happy path, srid",
			Builtin: geoBuiltins["st_makeenvelope"].overloads[0],
			Args: tree.Datums{
				mustParseDFloat(t, "30.01"),
				mustParseDFloat(t, "50.01"),
				mustParseDFloat(t, "72.01"),
				mustParseDFloat(t, "52.01"),
				tree.NewDInt(tree.DInt(4326)),
			},
			Expected: mustParseDGeometry(t, "POLYGON((30.01 50.01,30.01 52.01,72.01 52.01,72.01 50.01,30.01 50.01))", 4326),
		},
		{
			Title:   "st_makeenvelope happy path, unknown srid",
			Builtin: geoBuiltins["st_makeenvelope"].overloads[1],
			Args: tree.Datums{
				mustParseDFloat(t, "30.01"),
				mustParseDFloat(t, "50.01"),
				mustParseDFloat(t, "72.01"),
				mustParseDFloat(t, "52.01"),
			},
			Expected: mustParseDGeometry(t, "POLYGON((30.01 50.01,30.01 52.01,72.01 52.01,72.01 50.01,30.01 50.01))", 0),
		},
		{
			Title:   "st_makeenvelope degenerate bounds, unknown srid",
			Builtin: geoBuiltins["st_makeenvelope"].overloads[1],
			Args: tree.Datums{
				mustParseDFloat(t, "30"),
				mustParseDFloat(t, "50"),
				mustParseDFloat(t, "30"),
				mustParseDFloat(t, "50"),
			},
			Expected: mustParseDGeometry(t, "POLYGON((30 50,30 50,30 50,30 50,30 50))", 0),
		},
	} {
		testcase := toPin

		t.Run(testcase.Title, func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING

			overload := testcase.Builtin
			datums := testcase.Args
			res, err := overload.Fn(&tree.EvalContext{}, datums)
			if testcase.ExpectError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.EqualValues(t, testcase.Expected, res)
		})
	}
}
