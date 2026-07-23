// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

const randomTupleIterations = 1000
const randomTupleMaxLength = 10
const randomTupleStringMaxLength = 1000

var emptyTuple = types.MakeTuple([]*types.T{})
var tupleOfStringAndInt = types.MakeTuple([]*types.T{types.String, types.Int})
var tupleOfOneInt = types.MakeTuple([]*types.T{types.Int})
var tupleOfTwoInts = types.MakeTuple([]*types.T{types.Int, types.Int})
var tupleOfTwoStrings = types.MakeTuple([]*types.T{types.String, types.String})
var tupleOfTuples = types.MakeTuple([]*types.T{tupleOfTwoInts, tupleOfTwoStrings})
var tupleComplex = types.MakeTuple([]*types.T{types.String, types.TimeTZ, types.Jsonb})
var tupleOfTSVectorTSQuery = types.MakeTuple([]*types.T{types.TSVector, types.TSQuery})

func TestParseTuple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	timeTZString := "15:43:50.707903-02:31:00"
	timeTZ, _, err := timetz.ParseTimeTZ(timeutil.Now(), pgdate.DefaultDateStyle(), timeTZString, time.Microsecond)
	require.NoError(t, err)

	escapedJsonString := `{"")N}."": [false], ""UA%t"": [""foobar""]}`
	jsonVal, err := jsonb.ParseJSON(`{")N}.": [false], "UA%t": ["foobar"]}`)
	require.NoError(t, err)

	tsv, err := tree.ParseDTSVector("a b")
	require.NoError(t, err)
	tsq, err := tree.ParseDTSQuery("c")
	require.NoError(t, err)

	testData := []struct {
		str      string
		typ      *types.T
		expected *tree.DTuple
	}{
		{
			`(3,4)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.NewDString("3"), tree.NewDInt(4)),
		},
		{`(1, 2)`, tupleOfTwoInts, tree.NewDTuple(tupleOfStringAndInt, tree.NewDInt(1), tree.NewDInt(2))},
		{`( 1,2)`, tupleOfTwoInts, tree.NewDTuple(tupleOfStringAndInt, tree.NewDInt(1), tree.NewDInt(2))},
		{`(1 ,2)`, tupleOfTwoInts, tree.NewDTuple(tupleOfStringAndInt, tree.NewDInt(1), tree.NewDInt(2))},
		{`(1,2 )`, tupleOfTwoInts, tree.NewDTuple(tupleOfStringAndInt, tree.NewDInt(1), tree.NewDInt(2))},
		{`(null,)`, tupleOfStringAndInt, tree.NewDTuple(tupleOfStringAndInt, tree.NewDString("null"), tree.DNull)},
		{`()`, emptyTuple, tree.NewDTuple(emptyTuple)},
		{`()`, tupleOfOneInt, tree.NewDTuple(tupleOfOneInt, tree.DNull)},
		{
			`(3,)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.NewDString("3"), tree.DNull),
		},
		{
			`(,2)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.DNull, tree.NewDInt(2)),
		},
		{
			`(,)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.DNull, tree.DNull),
		},
		{
			`(  cat , dog  )`,
			tupleOfTwoStrings,
			tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("  cat "), tree.NewDString(" dog  ")),
		},
		{
			`(  cat , "dog " )`,
			tupleOfTwoStrings,
			tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("  cat "), tree.NewDString(" dog  ")),
		},
		{
			`(  cat , " "dog " " )`,
			tupleOfTwoStrings,
			tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("  cat "), tree.NewDString("  dog   ")),
		},
		{
			`(   " cat "   , "d"o"g"  )`,
			tupleOfTwoStrings,
			tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("    cat    "), tree.NewDString(" dog  ")),
		},
		{
			`(  cat \" ," dog \" " )`,
			tupleOfTwoStrings,
			tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("  cat \" "), tree.NewDString(" dog \"  ")),
		},
		{
			`(  cat "" ," dog "" " )`,
			tupleOfTwoStrings,
			tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("  cat  "), tree.NewDString(" dog \"  ")),
		},
		{
			`(  3 ,)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.NewDString("  3 "), tree.DNull),
		},
		{
			`("(1,2)",)`,
			tupleOfTuples,
			tree.NewDTuple(
				tupleOfTuples,
				tree.NewDTuple(tupleOfTwoInts, tree.NewDInt(1), tree.NewDInt(2)),
				tree.DNull,
			),
		},
		{
			`("(1,2)","(3,4)")`,
			tupleOfTuples,
			tree.NewDTuple(
				tupleOfTuples,
				tree.NewDTuple(tupleOfTwoInts, tree.NewDInt(1), tree.NewDInt(2)),
				tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("3"), tree.NewDString("4")),
			),
		},
		{
			`("(1,2) ","(3,4)")`,
			tupleOfTuples,
			tree.NewDTuple(
				tupleOfTuples,
				tree.NewDTuple(tupleOfTwoInts, tree.NewDInt(1), tree.NewDInt(2)),
				tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("3"), tree.NewDString("4")),
			),
		},
		{
			`("(1,2)", "(3,4)")`,
			tupleOfTuples,
			tree.NewDTuple(
				tupleOfTuples,
				tree.NewDTuple(tupleOfTwoInts, tree.NewDInt(1), tree.NewDInt(2)),
				tree.NewDTuple(tupleOfTwoStrings, tree.NewDString("3"), tree.NewDString("4")),
			),
		},
		{
			`("(1,2)", " (,) ")`,
			tupleOfTuples,
			tree.NewDTuple(
				tupleOfTuples,
				tree.NewDTuple(tupleOfTwoInts, tree.NewDInt(1), tree.NewDInt(2)),
				tree.NewDTuple(tupleOfTwoStrings, tree.DNull, tree.DNull),
			),
		},
		{
			`("\n",4)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.NewDString("n"), tree.NewDInt(4)),
		},
		{
			`("\\n",4)`,
			tupleOfStringAndInt,
			tree.NewDTuple(tupleOfStringAndInt, tree.NewDString("\\n"), tree.NewDInt(4)),
		},
		{
			`(a b, c)`,
			tupleOfTSVectorTSQuery,
			tree.NewDTuple(tupleOfTSVectorTSQuery, tsv, tsq),
		},
		{
			`("a b", c)`,
			tupleOfTSVectorTSQuery,
			tree.NewDTuple(tupleOfTSVectorTSQuery, tsv, tsq),
		},
		{
			fmt.Sprintf(`(0s{mlRk#, %s, "%s")`, timeTZString, escapedJsonString),
			tupleComplex,
			tree.NewDTuple(
				tupleComplex,
				tree.NewDString("0s{mlRk#"),
				tree.NewDTimeTZ(timeTZ),
				tree.NewDJSON(jsonVal),
			),
		},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			evalContext := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			actual, _, err := tree.ParseDTupleFromString(evalContext, td.str, td.typ)
			if err != nil {
				t.Fatalf("tuple %s: got error %s, expected %s", td.str, err.Error(), td.expected)
			}
			if cmp, err := actual.Compare(context.Background(), evalContext, td.expected); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatalf("tuple %s: got %s, expected %s", td.str, actual, td.expected)
			}
		})
	}
}

func TestParseTupleRandomStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < randomTupleIterations; i++ {
		numElems := rng.Intn(randomTupleMaxLength)
		tup := make([][]byte, numElems)
		tupContents := []*types.T{}
		for tupIdx := range tup {
			len := rng.Intn(randomTupleStringMaxLength)
			str := make([]byte, len)
			for strIdx := 0; strIdx < len; strIdx++ {
				str[strIdx] = byte(rng.Intn(256))
			}
			tup[tupIdx] = str
			tupContents = append(tupContents, types.String)
		}

		var buf bytes.Buffer
		buf.WriteByte('(')
		for j, b := range tup {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('"')
			// The input format for this doesn't support regular escaping, any
			// character can be preceded by a backslash to encode it directly (this
			// means that there's no printable way to encode non-printing characters,
			// users must use `e` prefixed strings).
			for _, c := range b {
				if c == '"' || c == '\\' || rng.Intn(10) == 0 {
					buf.WriteByte('\\')
				}
				buf.WriteByte(c)
			}
			buf.WriteByte('"')
		}
		buf.WriteByte(')')

		parsed, _, err := tree.ParseDTupleFromString(
			eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings()), buf.String(), types.MakeTuple(tupContents))
		if err != nil {
			t.Fatalf(`got error: "%s" for elem "%s"`, err, buf.String())
		}
		for tupIdx := range tup {
			value := tree.MustBeDString(parsed.D[tupIdx])
			if string(value) != string(tup[tupIdx]) {
				t.Fatalf(`iteration %d: tuple "%s", got %#v, expected %#v`, i, buf.String(), value, string(tup[tupIdx]))
			}
		}
	}
}

func TestParseTupleRandomDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < randomTupleIterations; i++ {
		numElems := 1 + rng.Intn(randomTupleMaxLength)
		tupContents := []*types.T{}
		for j := 0; j < numElems; j++ {
			tupContents = append(tupContents, randgen.RandTypeFromSlice(rng, tree.StrValAvailAllParsable))
		}
		// The inner tuple contents can be NULL, but the tuple itself cannot be,
		// since we can't parse a null tuple from a string.
		tup := randgen.RandDatum(rng, types.MakeTuple(tupContents), true /* nullOk */)
		if tup == tree.DNull {
			continue
		}
		conv := sessiondatapb.DataConversionConfig{
			ExtraFloatDigits: 1,
		}
		tupString := tree.AsStringWithFlags(tup, tree.FmtPgwireText, tree.FmtDataConversionConfig(conv), tree.FmtLocation(time.UTC))

		evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		parsed, _, err := tree.ParseDTupleFromString(
			evalCtx, tupString, types.MakeTuple(tupContents))
		if err != nil {
			t.Fatalf(
				"got error: %s\n tuple: %s\n tupleString: %s\n types: %s",
				err,
				tree.MustBeDTuple(tup).D,
				tupString,
				tupContents,
			)
		}
		if cmp, err := tup.Compare(context.Background(), evalCtx, parsed); err != nil {
			t.Fatal(err)
		} else if cmp != 0 {
			t.Fatalf(`iteration %d: tuple "%s", got %#v, expected %#v`, i, tupString, parsed, tup)
		}
	}
}

func TestParseTupleError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		str           string
		typ           *types.T
		expectedError string
	}{
		{``, tupleOfStringAndInt, `record must be enclosed in ( and )`},
		{`1`, tupleOfStringAndInt, `record must be enclosed in ( and )`},
		{`1,2`, tupleOfTwoInts, `record must be enclosed in ( and )`},
		{`(1,2`, tupleOfTwoInts, `malformed record literal`},
		{`(1,2,`, tupleOfTwoInts, `malformed record literal`},
		{`(`, tupleOfTwoInts, `malformed record literal`},
		{`()()`, tupleOfTwoInts, `malformed record literal`},
		{`() ()`, tupleOfTwoInts, `malformed record literal`},
		{`((1,2), (,))`, tupleOfTuples, `malformed record literal`},
		{`(, (2,3))`, tupleOfTuples, `record must be enclosed in ( and )`},
		{`(1,hello)`, tupleOfTwoInts, `strconv.ParseInt: parsing "hello": invalid syntax`},
		{`(1,"2)`, tupleOfTwoInts, `malformed record literal`},
		{`(hello,he"lo)`, tupleOfTwoStrings, `malformed record literal`},
		{
			`(  cat , "dog  )`,
			tupleOfTwoStrings,
			// Quotes to be balanced.
			`malformed record literal`,
		},
		{string([]byte{200}), tupleOfTwoInts, `could not parse "\xc8" as type tuple{int, int}: record must be enclosed in ( and )`},
		{string([]byte{'(', 'a', 200}), tupleOfTwoStrings, `malformed record literal`},

		{`(3,4,5)`, tupleOfTwoInts, `malformed record literal`},
		{`(3,4,)`, tupleOfTwoInts, `malformed record literal`},
		{`(3)`, tupleOfTwoInts, `malformed record literal`},
		{`(3,4`, tupleOfTwoInts, `malformed record literal`},
		{`(3,null)`, tupleOfTwoInts, `strconv.ParseInt: parsing "null": invalid syntax`},
		{`(3,,4)`, tupleOfTwoInts, `malformed record literal`},
	}

	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			_, _, err := tree.ParseDTupleFromString(
				eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings()), td.str, td.typ)
			if err == nil {
				t.Fatalf("expected %#v to error with message %#v", td.str, td.expectedError)
			}
			if !strings.HasSuffix(err.Error(), td.expectedError) {
				t.Fatalf("tuple %s: got error %s, expected suffix %s", td.str, err.Error(), td.expectedError)
			}
		})
	}
}
