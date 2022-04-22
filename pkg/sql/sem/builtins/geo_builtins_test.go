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

	"github.com/cockroachdb/cockroach/pkg/geo"
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

type singleArgBuiltinFixture struct {
	Title       string
	Builtin     tree.Overload
	Arg         tree.Datum
	Expected    tree.Datum
	ExpectError bool
}

func TestGeoBuiltinsCoordsMinMax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	polygonGeometry, err := tree.ParseDGeometry("POLYGON((170 50,170 72,-130 72,-130 50,170 50))")
	require.NoError(t, err)
	pointGeometry, err := tree.ParseDGeometry("POINT(130.7319444 33.74972222)")
	require.NoError(t, err)
	box2D, err := tree.ParseDBox2D("BOX(1 2,5 6)")
	require.NoError(t, err)

	// empty geometry, but not NULL
	emptyGeometry, err := tree.ParseDGeometry("POLYGON EMPTY")
	require.NoError(t, err)
	emptyBox2D := tree.NewDBox2D(geo.CartesianBoundingBox{}) // not going to be considered empty: can't have an empty box

	// NOTE: geospatial builtins do not support a NULL argument for now

	for _, toPin := range []singleArgBuiltinFixture{
		{
			Title:    "st_xmin(polygon)",
			Builtin:  geoBuiltins["st_xmin"].overloads[0],
			Arg:      polygonGeometry,
			Expected: mustParseDFloat(t, "-130"),
		},
		{
			Title:    "st_xmax(polygon)",
			Builtin:  geoBuiltins["st_xmax"].overloads[0],
			Arg:      polygonGeometry,
			Expected: mustParseDFloat(t, "170"),
		},
		{
			Title:    "st_ymin(polygon)",
			Builtin:  geoBuiltins["st_ymin"].overloads[0],
			Arg:      polygonGeometry,
			Expected: mustParseDFloat(t, "50"),
		},
		{
			Title:    "st_ymax(polygon)",
			Builtin:  geoBuiltins["st_ymax"].overloads[0],
			Arg:      polygonGeometry,
			Expected: mustParseDFloat(t, "72"),
		},
		{
			Title:    "st_zmin(polygon)",
			Builtin:  geoBuiltins["st_zmin"].overloads[0],
			Arg:      polygonGeometry,
			Expected: tree.NewDFloat(0),
		},
		{
			Title:    "st_zmax(polygon)",
			Builtin:  geoBuiltins["st_zmax"].overloads[0],
			Arg:      polygonGeometry,
			Expected: tree.NewDFloat(0),
		},
		{
			Title:    "st_xmin(point)",
			Builtin:  geoBuiltins["st_xmin"].overloads[0],
			Arg:      pointGeometry,
			Expected: mustParseDFloat(t, "130.7319444"),
		},
		{
			Title:    "st_xmax(point)",
			Builtin:  geoBuiltins["st_xmax"].overloads[0],
			Arg:      pointGeometry,
			Expected: mustParseDFloat(t, "130.7319444"),
		},
		{
			Title:    "st_ymin(point)",
			Builtin:  geoBuiltins["st_ymin"].overloads[0],
			Arg:      pointGeometry,
			Expected: mustParseDFloat(t, "33.74972222"),
		},
		{
			Title:    "st_ymax(point)",
			Builtin:  geoBuiltins["st_ymax"].overloads[0],
			Arg:      pointGeometry,
			Expected: mustParseDFloat(t, "33.74972222"),
		},
		{
			Title:    "st_zmin(point)",
			Builtin:  geoBuiltins["st_zmin"].overloads[0],
			Arg:      pointGeometry,
			Expected: tree.NewDFloat(0),
		},
		{
			Title:    "st_zmax(point)",
			Builtin:  geoBuiltins["st_zmax"].overloads[0],
			Arg:      pointGeometry,
			Expected: tree.NewDFloat(0),
		},
		{
			Title:    "st_xmin(box2D)",
			Builtin:  geoBuiltins["st_xmin"].overloads[1],
			Arg:      box2D,
			Expected: mustParseDFloat(t, "1"),
		},
		{
			Title:    "st_xmax(box2D)",
			Builtin:  geoBuiltins["st_xmax"].overloads[1],
			Arg:      box2D,
			Expected: mustParseDFloat(t, "5"),
		},
		{
			Title:    "st_ymin(box2D)",
			Builtin:  geoBuiltins["st_ymin"].overloads[1],
			Arg:      box2D,
			Expected: mustParseDFloat(t, "2"),
		},
		{
			Title:    "st_ymax(box2D)",
			Builtin:  geoBuiltins["st_ymax"].overloads[1],
			Arg:      box2D,
			Expected: mustParseDFloat(t, "6"),
		},
		{
			Title:    "st_zmin(box2D)",
			Builtin:  geoBuiltins["st_zmin"].overloads[1],
			Arg:      box2D,
			Expected: tree.NewDFloat(0),
		},
		{
			Title:    "st_zmax(box2D)",
			Builtin:  geoBuiltins["st_zmax"].overloads[1],
			Arg:      box2D,
			Expected: tree.NewDFloat(0),
		},
		{
			Title:    "st_xmin(empty geometry)",
			Builtin:  geoBuiltins["st_xmin"].overloads[0],
			Arg:      emptyGeometry,
			Expected: tree.DNull,
		},
		{
			Title:    "st_xmax(empty geometry)",
			Builtin:  geoBuiltins["st_xmax"].overloads[0],
			Arg:      emptyGeometry,
			Expected: tree.DNull,
		},
		{
			Title:    "st_ymin(empty geometry)",
			Builtin:  geoBuiltins["st_ymin"].overloads[0],
			Arg:      emptyGeometry,
			Expected: tree.DNull,
		},
		{
			Title:    "st_ymax(empty geometry)",
			Builtin:  geoBuiltins["st_ymax"].overloads[0],
			Arg:      emptyGeometry,
			Expected: tree.DNull,
		},
		{
			Title:    "st_zmin(empty geometry)",
			Builtin:  geoBuiltins["st_zmin"].overloads[0],
			Arg:      emptyGeometry,
			Expected: tree.DNull,
		},
		{
			Title:    "st_zmax(empty geometry)",
			Builtin:  geoBuiltins["st_zmax"].overloads[0],
			Arg:      emptyGeometry,
			Expected: tree.DNull,
		},
		{
			Title:    "st_xmin(empty box2D)",
			Builtin:  geoBuiltins["st_xmin"].overloads[1],
			Arg:      emptyBox2D,
			Expected: tree.NewDFloat(0), // can't have an empty box
		},
		{
			Title:    "st_xmax(empty box2D)",
			Builtin:  geoBuiltins["st_xmax"].overloads[1],
			Arg:      emptyBox2D,
			Expected: tree.NewDFloat(0), // can't have an empty box
		},
		{
			Title:    "st_ymin(empty box2D)",
			Builtin:  geoBuiltins["st_ymin"].overloads[1],
			Arg:      emptyBox2D,
			Expected: tree.NewDFloat(0), // can't have an empty box
		},
		{
			Title:    "st_ymax(empty box2D)",
			Builtin:  geoBuiltins["st_ymax"].overloads[1],
			Arg:      emptyBox2D,
			Expected: tree.NewDFloat(0), // can't have an empty box
		},
		{
			Title:    "st_zmin(empty box2D)",
			Builtin:  geoBuiltins["st_zmin"].overloads[1],
			Arg:      emptyBox2D,
			Expected: tree.NewDFloat(0), // can't have an empty box
		},
		{
			Title:    "st_zmax(empty box2D)",
			Builtin:  geoBuiltins["st_zmax"].overloads[1],
			Arg:      emptyBox2D,
			Expected: tree.NewDFloat(0), // can't have an empty box
		},
	} {
		testcase := toPin

		t.Run(testcase.Title, func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING

			overload := testcase.Builtin
			datums := tree.Datums{testcase.Arg}
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

func mustParseDFloat(t testing.TB, in string) tree.Datum {
	d, err := tree.ParseDFloat(in)
	require.NoError(t, err)

	return d
}
