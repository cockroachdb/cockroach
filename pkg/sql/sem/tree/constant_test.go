// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"context"
	"go/constant"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
)

// TestAvailTypesAreSets verifies that all of the constant "available type"
// slices don't have duplicate OIDs.
func TestAvailTypesAreSets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		availTypes []*types.T
	}{
		{tree.NumValAvailInteger},
		{tree.NumValAvailDecimalNoFraction},
		{tree.NumValAvailDecimalWithFraction},
		{tree.StrValAvailAllParsable},
		{tree.StrValAvailBytes},
	}

	for i, test := range testCases {
		seen := make(map[oid.Oid]struct{})
		for _, newType := range test.availTypes {
			if _, ok := seen[newType.Oid()]; ok {
				t.Errorf("%d: found duplicate type: %v", i, newType)
			}
			seen[newType.Oid()] = struct{}{}
		}
	}
}

// TestNumericConstantVerifyAndResolveAvailableTypes verifies that test NumVals will
// all return expected available type sets, and that attempting to resolve the NumVals
// as each of these types will all succeed with an expected tree.Datum result.
func TestNumericConstantVerifyAndResolveAvailableTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	wantInt := tree.NumValAvailInteger
	wantDecButCanBeInt := tree.NumValAvailDecimalNoFraction
	wantDec := tree.NumValAvailDecimalWithFraction

	testCases := []struct {
		str   string
		avail []*types.T
	}{
		{"1", wantInt},
		{"0", wantInt},
		{"-1", wantInt},
		{"9223372036854775807", wantInt},
		{"1.0", wantDecButCanBeInt},
		{"-1234.0000", wantDecButCanBeInt},
		{"1e10", wantDecButCanBeInt},
		{"1E10", wantDecButCanBeInt},
		{"1.1", wantDec},
		{"1e-10", wantDec},
		{"1E-10", wantDec},
		{"-1231.131", wantDec},
		{"876543234567898765436787654321", wantDec},
	}

	for i, test := range testCases {
		tok := token.INT
		if strings.ContainsAny(test.str, ".eE") {
			tok = token.FLOAT
		}

		str := test.str
		neg := false
		if str[0] == '-' {
			neg = true
			str = str[1:]
		}

		val := constant.MakeFromLiteral(str, tok, 0)
		if val.Kind() == constant.Unknown {
			t.Fatalf("%d: could not parse value string %q", i, test.str)
		}

		// Check available types.
		c := tree.NewNumVal(val, str, neg)
		avail := c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %v, found %v",
				i, test.avail, c.ExactString(), avail)
		}

		// Make sure it can be resolved as each of those types.
		for _, availType := range avail {
			ctx := context.Background()
			semaCtx := tree.MakeSemaContext()
			if res, err := c.ResolveAsType(ctx, &semaCtx, availType); err != nil {
				t.Errorf("%d: expected resolving %v as available type %s would succeed, found %v",
					i, c.ExactString(), availType, err)
			} else {
				resErr := func(parsed, resolved interface{}) {
					t.Errorf("%d: expected resolving %v as available type %s would produce a tree.Datum"+
						" with the value %v, found %v",
						i, c, availType, parsed, resolved)
				}
				switch typ := res.(type) {
				case *tree.DInt:
					var i int64
					var err error
					if tok == token.INT {
						if i, err = strconv.ParseInt(test.str, 10, 64); err != nil {
							t.Fatal(err)
						}
					} else {
						var f float64
						if f, err = strconv.ParseFloat(test.str, 64); err != nil {
							t.Fatal(err)
						}
						i = int64(f)
					}
					if resI := int64(*typ); i != resI {
						resErr(i, resI)
					}
				case *tree.DFloat:
					f, err := strconv.ParseFloat(test.str, 64)
					if err != nil {
						t.Fatal(err)
					}
					if resF := float64(*typ); f != resF {
						resErr(f, resF)
					}
				case *tree.DDecimal:
					d := new(apd.Decimal)
					if !strings.ContainsAny(test.str, "eE") {
						if _, _, err := d.SetString(test.str); err != nil {
							t.Fatalf("could not set %q on decimal", test.str)
						}
					} else {
						_, _, err = d.SetString(test.str)
						if err != nil {
							t.Fatal(err)
						}
					}
					resD := &typ.Decimal
					if d.Cmp(resD) != 0 {
						resErr(d, resD)
					}
				}
			}
		}
	}
}

// TestStringConstantVerifyAvailableTypes verifies that test StrVals will all
// return expected available type sets, and that attempting to resolve the StrVals
// as each of these types will either succeed or return a parse error.
func TestStringConstantVerifyAvailableTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	wantStringButCanBeAll := tree.StrValAvailAllParsable
	wantBytes := tree.StrValAvailBytes

	testCases := []struct {
		c     *tree.StrVal
		avail []*types.T
	}{
		{tree.NewStrVal("abc 世界"), wantStringButCanBeAll},
		{tree.NewStrVal("t"), wantStringButCanBeAll},
		{tree.NewStrVal("2010-09-28"), wantStringButCanBeAll},
		{tree.NewStrVal("2010-09-28 12:00:00.1"), wantStringButCanBeAll},
		{tree.NewStrVal("PT12H2M"), wantStringButCanBeAll},
		{tree.NewBytesStrVal("abc 世界"), wantBytes},
		{tree.NewBytesStrVal("t"), wantBytes},
		{tree.NewBytesStrVal("2010-09-28"), wantBytes},
		{tree.NewBytesStrVal("2010-09-28 12:00:00.1"), wantBytes},
		{tree.NewBytesStrVal("PT12H2M"), wantBytes},
		{tree.NewBytesStrVal(string([]byte{0xff, 0xfe, 0xfd})), wantBytes},
	}

	for i, test := range testCases {
		// Check that the expected available types are returned.
		avail := test.c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %+v, found %v",
				i, test.avail, test.c, avail)
		}

		// Make sure it can be resolved as each of those types or throws a parsing error.
		for _, availType := range avail {

			// The enum value in c.AvailableTypes() is AnyEnum, so we will not be able to
			// resolve that exact type. In actual execution, the constant would be resolved
			// as a hydrated enum type instead.
			if availType.Family() == types.EnumFamily {
				continue
			}

			semaCtx := tree.MakeSemaContext()
			if _, err := test.c.ResolveAsType(context.Background(), &semaCtx, availType); err != nil {
				if !strings.Contains(err.Error(), "could not parse") {
					// Parsing errors are permitted for this test, as proper tree.StrVal parsing
					// is tested in TestStringConstantTypeResolution. Any other error should
					// throw a failure.
					t.Errorf("%d: expected resolving %v as available type %s would either succeed"+
						" or throw a parsing error, found %v",
						i, test.c, availType, err)
				}
			}
		}
	}
}

func mustParseDInt(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDInt(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDFloat(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDFloat(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDDecimal(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDDecimal(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDBool(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDBool(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDDate(t *testing.T, s string) tree.Datum {
	d, _, err := tree.ParseDDate(nil, s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDTime(t *testing.T, s string) tree.Datum {
	d, _, err := tree.ParseDTime(nil, s, time.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDTimeTZ(t *testing.T, s string) tree.Datum {
	d, _, err := tree.ParseDTimeTZ(nil, s, time.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDTimestamp(t *testing.T, s string) tree.Datum {
	d, _, err := tree.ParseDTimestamp(nil, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDTimestampTZ(t *testing.T, s string) tree.Datum {
	d, _, err := tree.ParseDTimestampTZ(nil, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDInterval(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDInterval(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDJSON(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDJSON(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDUuid(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDUuidFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDBox2D(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDBox2D(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDGeography(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDGeography(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDGeometry(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDGeometry(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDINet(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDIPAddrFromINetString(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDVarBit(t *testing.T, s string) tree.Datum {
	d, err := tree.ParseDBitArray(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDArrayOfType(typ *types.T) func(t *testing.T, s string) tree.Datum {
	return func(t *testing.T, s string) tree.Datum {
		evalContext := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
		d, _, err := tree.ParseDArrayFromString(&evalContext, s, typ)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
}

var parseFuncs = map[*types.T]func(*testing.T, string) tree.Datum{
	types.String:           func(t *testing.T, s string) tree.Datum { return tree.NewDString(s) },
	types.Bytes:            func(t *testing.T, s string) tree.Datum { return tree.NewDBytes(tree.DBytes(s)) },
	types.Int:              mustParseDInt,
	types.Float:            mustParseDFloat,
	types.Decimal:          mustParseDDecimal,
	types.Bool:             mustParseDBool,
	types.Date:             mustParseDDate,
	types.Time:             mustParseDTime,
	types.TimeTZ:           mustParseDTimeTZ,
	types.Timestamp:        mustParseDTimestamp,
	types.TimestampTZ:      mustParseDTimestampTZ,
	types.Interval:         mustParseDInterval,
	types.Jsonb:            mustParseDJSON,
	types.Uuid:             mustParseDUuid,
	types.Box2D:            mustParseDBox2D,
	types.Geography:        mustParseDGeography,
	types.Geometry:         mustParseDGeometry,
	types.INet:             mustParseDINet,
	types.VarBit:           mustParseDVarBit,
	types.DecimalArray:     mustParseDArrayOfType(types.Decimal),
	types.FloatArray:       mustParseDArrayOfType(types.Float),
	types.IntArray:         mustParseDArrayOfType(types.Int),
	types.StringArray:      mustParseDArrayOfType(types.String),
	types.BoolArray:        mustParseDArrayOfType(types.Bool),
	types.UUIDArray:        mustParseDArrayOfType(types.Uuid),
	types.DateArray:        mustParseDArrayOfType(types.Date),
	types.TimeArray:        mustParseDArrayOfType(types.Time),
	types.TimeTZArray:      mustParseDArrayOfType(types.TimeTZ),
	types.TimestampArray:   mustParseDArrayOfType(types.Timestamp),
	types.TimestampTZArray: mustParseDArrayOfType(types.TimestampTZ),
	types.IntervalArray:    mustParseDArrayOfType(types.Interval),
	types.INetArray:        mustParseDArrayOfType(types.INet),
	types.VarBitArray:      mustParseDArrayOfType(types.VarBit),
}

func typeSet(tys ...*types.T) map[*types.T]struct{} {
	set := make(map[*types.T]struct{}, len(tys))
	for _, t := range tys {
		set[t] = struct{}{}
	}
	return set
}

// TestStringConstantResolveAvailableTypes verifies that test StrVals can all be
// resolved successfully into an expected set of tree.Datum types. The test will make sure
// the correct set of tree.Datum types are resolvable, and that the resolved tree.Datum match
// the expected results which come from running the string literal through a
// corresponding parseFunc (above).
func TestStringConstantResolveAvailableTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		c            *tree.StrVal
		parseOptions map[*types.T]struct{}
	}{
		{
			c:            tree.NewStrVal("abc 世界"),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewStrVal("true"),
			parseOptions: typeSet(types.String, types.Bytes, types.Bool, types.Jsonb),
		},
		{
			c:            tree.NewStrVal("2010-09-28"),
			parseOptions: typeSet(types.String, types.Bytes, types.Date, types.Timestamp, types.TimestampTZ),
		},
		{
			c:            tree.NewStrVal("2010-09-28 12:00:00.1"),
			parseOptions: typeSet(types.String, types.Bytes, types.Time, types.TimeTZ, types.Timestamp, types.TimestampTZ, types.Date),
		},
		{
			c:            tree.NewStrVal("2006-07-08T00:00:00.000000123Z"),
			parseOptions: typeSet(types.String, types.Bytes, types.Time, types.TimeTZ, types.Timestamp, types.TimestampTZ, types.Date),
		},
		{
			c:            tree.NewStrVal("PT12H2M"),
			parseOptions: typeSet(types.String, types.Bytes, types.Interval),
		},
		{
			c:            tree.NewBytesStrVal("abc 世界"),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewBytesStrVal("true"),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewBytesStrVal("2010-09-28"),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewBytesStrVal("2010-09-28 12:00:00.1"),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewBytesStrVal("PT12H2M"),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewStrVal("box(0 0, 1 1)"),
			parseOptions: typeSet(types.String, types.Bytes, types.Box2D),
		},
		{
			c:            tree.NewStrVal("POINT(-100.59 42.94)"),
			parseOptions: typeSet(types.String, types.Bytes, types.Geography, types.Geometry),
		},
		{
			c:            tree.NewStrVal("192.168.100.128/25"),
			parseOptions: typeSet(types.String, types.Bytes, types.INet),
		},
		{
			c: tree.NewStrVal("111000110101"),
			parseOptions: typeSet(
				types.String,
				types.Bytes,
				types.VarBit,
				types.Int,
				types.Float,
				types.Decimal,
				types.Interval,
				types.Jsonb),
		},
		{
			c:            tree.NewStrVal(`{"a": 1}`),
			parseOptions: typeSet(types.String, types.Bytes, types.Jsonb),
		},
		{
			c: tree.NewStrVal(`{1,2}`),
			parseOptions: typeSet(
				types.String,
				types.Bytes,
				types.StringArray,
				types.IntArray,
				types.FloatArray,
				types.DecimalArray,
				types.IntervalArray),
		},
		{
			c: tree.NewStrVal(`{1.5,2.0}`),
			parseOptions: typeSet(
				types.String,
				types.Bytes,
				types.StringArray,
				types.FloatArray,
				types.DecimalArray,
				types.IntervalArray),
		},
		{
			c:            tree.NewStrVal(`{a,b}`),
			parseOptions: typeSet(types.String, types.Bytes, types.StringArray),
		},
		{
			c:            tree.NewBytesStrVal(string([]byte{0xff, 0xfe, 0xfd})),
			parseOptions: typeSet(types.String, types.Bytes),
		},
		{
			c:            tree.NewStrVal(`18e7b17e-4ead-4e27-bfd5-bb6d11261bb6`),
			parseOptions: typeSet(types.String, types.Bytes, types.Uuid),
		},
		{
			c:            tree.NewStrVal(`{18e7b17e-4ead-4e27-bfd5-bb6d11261bb6, 18e7b17e-4ead-4e27-bfd5-bb6d11261bb7}`),
			parseOptions: typeSet(types.String, types.Bytes, types.StringArray, types.UUIDArray),
		},
		{
			c:            tree.NewStrVal("{true, false}"),
			parseOptions: typeSet(types.String, types.Bytes, types.StringArray, types.BoolArray),
		},
		{
			c:            tree.NewStrVal("{2010-09-28, 2010-09-29}"),
			parseOptions: typeSet(types.String, types.Bytes, types.StringArray, types.DateArray, types.TimestampArray, types.TimestampTZArray),
		},
		{
			c: tree.NewStrVal("{2010-09-28 12:00:00.1, 2010-09-29 12:00:00.1}"),
			parseOptions: typeSet(
				types.String,
				types.Bytes,
				types.StringArray,
				types.TimeArray,
				types.TimeTZArray,
				types.TimestampArray,
				types.TimestampTZArray,
				types.DateArray),
		},
		{
			c: tree.NewStrVal("{2006-07-08T00:00:00.000000123Z, 2006-07-10T00:00:00.000000123Z}"),
			parseOptions: typeSet(
				types.String,
				types.Bytes,
				types.StringArray,
				types.TimeArray,
				types.TimeTZArray,
				types.TimestampArray,
				types.TimestampTZArray,
				types.DateArray),
		},
		{
			c:            tree.NewStrVal("{PT12H2M, -23:00:00}"),
			parseOptions: typeSet(types.String, types.Bytes, types.StringArray, types.IntervalArray),
		},
		{
			c:            tree.NewStrVal("{192.168.100.128, ::ffff:10.4.3.2}"),
			parseOptions: typeSet(types.String, types.Bytes, types.StringArray, types.INetArray),
		},
		{
			c: tree.NewStrVal("{0101, 11}"),
			parseOptions: typeSet(
				types.String,
				types.Bytes,
				types.StringArray,
				types.IntArray,
				types.FloatArray,
				types.DecimalArray,
				types.IntervalArray,
				types.VarBitArray),
		},
	}

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	for i, test := range testCases {
		parseableCount := 0

		// Make sure it can be resolved as each of those types or throws a parsing error.
		for _, availType := range test.c.AvailableTypes() {

			// The enum value in c.AvailableTypes() is AnyEnum, so we will not be able to
			// resolve that exact type. In actual execution, the constant would be resolved
			// as a hydrated enum type instead.
			if availType.Family() == types.EnumFamily {
				continue
			}

			semaCtx := tree.MakeSemaContext()
			typedExpr, err := test.c.ResolveAsType(context.Background(), &semaCtx, availType)
			var res tree.Datum
			if err == nil {
				res, err = typedExpr.Eval(evalCtx)
			}
			if err != nil {
				if !strings.Contains(err.Error(), "could not parse") &&
					!strings.Contains(err.Error(), "parsing") &&
					!strings.Contains(err.Error(), "out of range") &&
					!strings.Contains(err.Error(), "exceeds supported") {
					// Parsing errors are permitted for this test, but the number of correctly
					// parseable types will be verified. Any other error should throw a failure.
					t.Errorf("%d: expected resolving %v as available type %s would either succeed"+
						" or throw a parsing error, found %v",
						i, test.c, availType, err)
				}
				continue
			}
			parseableCount++

			if _, isExpected := test.parseOptions[availType]; !isExpected {
				t.Errorf("%d: type %s not expected to be resolvable from the tree.StrVal %v, found %v",
					i, availType, test.c, res)
			} else {
				expectedDatum := parseFuncs[availType](t, test.c.RawString())
				if res.Compare(evalCtx, expectedDatum) != 0 {
					t.Errorf("%d: type %s expected to be resolved from the tree.StrVal %v to tree.Datum %v"+
						", found %v",
						i, availType, test.c, expectedDatum, res)
				}
			}
		}

		// Make sure the expected number of types can be resolved from the tree.StrVal.
		if expCount := len(test.parseOptions); parseableCount != expCount {
			t.Errorf("%d: expected %d successfully resolvable types for the tree.StrVal %v, found %d",
				i, expCount, test.c, parseableCount)
		}
	}
}
