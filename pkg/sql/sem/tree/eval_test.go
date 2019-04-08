// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree_test

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

func TestEval(t *testing.T) {
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer ctx.Stop(context.Background())

	walk := func(t *testing.T, getExpr func(tree.TypedExpr) (tree.TypedExpr, error)) {
		datadriven.Walk(t, filepath.Join("testdata", "eval"), func(t *testing.T, path string) {
			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				if d.Cmd != "eval" {
					t.Fatalf("unsupported command %s", d.Cmd)
				}
				expr, err := parser.ParseExpr(d.Input)
				if err != nil {
					t.Fatalf("%s: %v", d.Input, err)
				}
				// expr.TypeCheck to avoid constant folding.
				typedExpr, err := expr.TypeCheck(nil, types.Any)
				if err != nil {
					t.Fatalf("%s: %v", d.Input, err)
				}
				t.Logf("Type checked expression: %s", typedExpr)

				e, err := getExpr(typedExpr)
				if err != nil {
					t.Fatalf("%s: %v", typedExpr, err)
				}
				r, err := e.Eval(ctx)
				if err != nil {
					t.Fatalf("%s: %v", e, err)
				}
				return r.String() + "\n"
			})
		})
	}

	t.Run("opt", func(t *testing.T) {
		walk(t, func(e tree.TypedExpr) (tree.TypedExpr, error) {
			return optBuildScalar(ctx, e)
		})
	})

	t.Run("no-opt", func(t *testing.T) {
		walk(t, func(e tree.TypedExpr) (tree.TypedExpr, error) {
			return ctx.NormalizeExpr(e)
		})
	})
}

func optBuildScalar(evalCtx *tree.EvalContext, e tree.TypedExpr) (tree.TypedExpr, error) {
	var o xform.Optimizer
	o.Init(evalCtx)
	b := optbuilder.NewScalar(context.TODO(), &tree.SemaContext{}, evalCtx, o.Factory())
	b.AllowUnsupportedExpr = true
	if err := b.Build(e); err != nil {
		return nil, err
	}

	bld := execbuilder.New(nil /* factory */, o.Memo(), o.Memo().RootExpr(), evalCtx)
	ivh := tree.MakeIndexedVarHelper(nil /* container */, 0)

	expr, err := bld.BuildScalar(&ivh)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func TestTimeConversion(t *testing.T) {
	tests := []struct {
		start     string
		format    string
		tm        string
		revformat string
		reverse   string
	}{
		// %a %A %b %B (+ %Y)
		{`Wed Oct 05 2016`, `%a %b %d %Y`, `2016-10-05 00:00:00+00:00`, ``, ``},
		{`Wednesday October 05 2016`, `%A %B %d %Y`, `2016-10-05 00:00:00+00:00`, ``, ``},
		// %c
		{`Wed Oct 5 01:02:03 2016`, `%c`, `2016-10-05 01:02:03+00:00`, ``, ``},
		// %C %d (+ %m %y)
		{`20 06 10 12`, `%C %y %m %d`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %D
		{`10/12/06`, `%D`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %e (+ %Y %m)
		{`2006 10  3`, `%Y %m %e`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %f (+ %c)
		{`Wed Oct 5 01:02:03 2016 .123`, `%c .%f`, `2016-10-05 01:02:03.123+00:00`, `.%f`, `.123000000`},
		{`Wed Oct 5 01:02:03 2016 .123456`, `%c .%f`, `2016-10-05 01:02:03.123456+00:00`, `.%f`, `.123456000`},
		{`Wed Oct 5 01:02:03 2016 .123456789`, `%c .%f`, `2016-10-05 01:02:03.123457+00:00`, `.%f`, `.123457000`},
		{`Wed Oct 5 01:02:03 2016 .999999999`, `%c .%f`, `2016-10-05 01:02:04+00:00`, `.%f`, `.000000000`},
		// %F
		{`2006-10-03`, `%F`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %h (+ %Y %d)
		{`2006 Oct 03`, `%Y %h %d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %H (+ %S %M)
		{`20061012 01:03:02`, `%Y%m%d %H:%S:%M`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %I (+ %Y %m %d)
		{`20161012 11`, `%Y%m%d %I`, `2016-10-12 11:00:00+00:00`, ``, ``},
		// %j (+ %Y)
		{`2016 286`, `%Y %j`, `2016-10-12 00:00:00+00:00`, ``, ``},
		// %k (+ %Y %m %d)
		{`20061012 23`, `%Y%m%d %k`, `2006-10-12 23:00:00+00:00`, ``, ``},
		// %l (+ %Y %m %d %p)
		{`20061012  5 PM`, `%Y%m%d %l %p`, `2006-10-12 17:00:00+00:00`, ``, ``},
		// %n (+ %Y %m %d)
		{"2006\n10\n03", `%Y%n%m%n%d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %p cannot be parsed before hour specifiers, so be sure that
		// they appear in this order.
		{`20161012 11 PM`, `%Y%m%d %I %p`, `2016-10-12 23:00:00+00:00`, ``, ``},
		{`20161012 11 AM`, `%Y%m%d %I %p`, `2016-10-12 11:00:00+00:00`, ``, ``},
		// %r
		{`20161012 11:02:03 PM`, `%Y%m%d %r`, `2016-10-12 23:02:03+00:00`, ``, ``},
		// %R
		{`20161012 11:02`, `%Y%m%d %R`, `2016-10-12 11:02:00+00:00`, ``, ``},
		// %s
		{`1491920586`, `%s`, `2017-04-11 14:23:06+00:00`, ``, ``},
		// %t (+ %Y %m %d)
		{"2006\t10\t03", `%Y%t%m%t%d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %T (+ %Y %m %d)
		{`20061012 01:02:03`, `%Y%m%d %T`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %U %u (+ %Y)
		{`2018 10 4`, `%Y %U %u`, `2018-03-15 00:00:00+00:00`, ``, ``},
		// %W %w (+ %Y)
		{`2018 10 4`, `%Y %W %w`, `2018-03-08 00:00:00+00:00`, ``, ``},
		// %x
		{`10/12/06`, `%x`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %X
		{`20061012 01:02:03`, `%Y%m%d %X`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %y (+ %m %d)
		{`000101`, `%y%m%d`, `2000-01-01 00:00:00+00:00`, ``, ``},
		{`680101`, `%y%m%d`, `2068-01-01 00:00:00+00:00`, ``, ``},
		{`690101`, `%y%m%d`, `1969-01-01 00:00:00+00:00`, ``, ``},
		{`990101`, `%y%m%d`, `1999-01-01 00:00:00+00:00`, ``, ``},
		// %Y
		{`19000101`, `%Y%m%d`, `1900-01-01 00:00:00+00:00`, ``, ``},
		{`20000101`, `%Y%m%d`, `2000-01-01 00:00:00+00:00`, ``, ``},
		{`30000101`, `%Y%m%d`, `3000-01-01 00:00:00+00:00`, ``, ``},
		// %z causes the time zone to adjust the time when parsing, but the time zone information
		// is not retained when printing the timestamp out back.
		{`20160101 13:00 +0655`, `%Y%m%d %H:%M %z`, `2016-01-01 06:05:00+00:00`, `%Y%m%d %H:%M %z`, `20160101 06:05 +0000`},
	}

	for _, test := range tests {
		ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer ctx.Mon.Stop(context.Background())
		exprStr := fmt.Sprintf("experimental_strptime('%s', '%s')", test.start, test.format)
		expr, err := parser.ParseExpr(exprStr)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		typedExpr, err := expr.TypeCheck(nil, types.Timestamp)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		r, err := typedExpr.Eval(ctx)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		ts, ok := r.(*tree.DTimestampTZ)
		if !ok {
			t.Errorf("%s: result not a timestamp: %s", exprStr, r)
			continue
		}

		tmS := ts.String()
		tmS = tmS[1 : len(tmS)-1] // strip the quote delimiters
		if tmS != test.tm {
			t.Errorf("%s: got %q, expected %q", exprStr, tmS, test.tm)
			continue
		}

		revfmt := test.format
		if test.revformat != "" {
			revfmt = test.revformat
		}

		ref := test.start
		if test.reverse != "" {
			ref = test.reverse
		}

		exprStr = fmt.Sprintf("experimental_strftime('%s'::timestamp, '%s')", tmS, revfmt)
		expr, err = parser.ParseExpr(exprStr)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		typedExpr, err = expr.TypeCheck(nil, types.Timestamp)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		r, err = typedExpr.Eval(ctx)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		rs, ok := r.(*tree.DString)
		if !ok {
			t.Errorf("%s: result not a string: %s", exprStr, r)
			continue
		}
		revS := string(*rs)
		if ref != revS {
			t.Errorf("%s: got %q, expected %q", exprStr, revS, ref)
		}
	}
}

func TestEvalError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`1 % 0`, `zero modulus`},
		{`1 / 0`, `division by zero`},
		{`1 // 0`, `division by zero`},
		{`1.5 / 0`, `division by zero`},
		{`'11h2m'::interval / 0`, `division by zero`},
		{`'11h2m'::interval / 0.0::float`, `division by zero`},
		{`'???'::bool`,
			`could not parse "???" as type bool`},
		{`'foo'::int`,
			`could not parse "foo" as type int: strconv.ParseInt: parsing "foo": invalid syntax`},
		{`'3\r2'::int`,
			`could not parse "3\\r2" as type int: strconv.ParseInt: parsing "3\\r2": invalid syntax`},
		{`'bar'::float`,
			`could not parse "bar" as type float: strconv.ParseFloat: parsing "bar": invalid syntax`},
		{`'baz'::decimal`,
			`could not parse "baz" as type decimal`},
		{`'2010-09-28 12:00:00.1q'::date`,
			`could not parse "2010-09-28 12:00:00.1q" as type date`},
		{`'12:00:00q'::time`, `could not parse "12:00:00q" as type time`},
		{`'2010-09-28 12:00.1 MST'::timestamp`,
			`unimplemented: timestamp abbreviations not supported`},
		{`'abcd'::interval`,
			`could not parse "abcd" as type interval: interval: missing unit`},
		{`'1- 2:3:4 9'::interval`,
			`could not parse "1- 2:3:4 9" as type interval: invalid input syntax for type interval 1- 2:3:4 9`},
		{`e'\\xdedf0d36174'::BYTES`, `could not parse "\\xdedf0d36174" as type bytes: encoding/hex: odd length hex string`},
		{`ARRAY[NULL, ARRAY[1, 2]]`, `multidimensional arrays must have array expressions with matching dimensions`},
		{`ARRAY[ARRAY[1, 2], NULL]`, `multidimensional arrays must have array expressions with matching dimensions`},
		{`ARRAY[ARRAY[1, 2], ARRAY[1]]`, `multidimensional arrays must have array expressions with matching dimensions`},
		// TODO(pmattis): Check for overflow.
		// {`~0 + 1`, `0`},
		{`9223372036854775807::int + 1::int`, `integer out of range`},
		{`-9223372036854775807::int + -2::int`, `integer out of range`},
		{`-9223372036854775807::int + -9223372036854775807::int`, `integer out of range`},
		{`9223372036854775807::int + 9223372036854775807::int`, `integer out of range`},
		{`9223372036854775807::int - -1::int`, `integer out of range`},
		{`-9223372036854775807::int - 2::int`, `integer out of range`},
		{`-9223372036854775807::int - 9223372036854775807::int`, `integer out of range`},
		{`9223372036854775807::int - -9223372036854775807::int`, `integer out of range`},
		{`4611686018427387904::int * 2::int`, `integer out of range`},
		{`4611686018427387904::int * 2::int`, `integer out of range`},
		{`(-9223372036854775807:::int - 1) * -1:::int`, `integer out of range`},
		{`123 ^ 100`, `integer out of range`},
		{`power(123, 100)`, `integer out of range`},
		// Although these next two tests are valid integers, a float cannot represent
		// them exactly, and so rounds them to a larger number that is out of bounds
		// for an int. Thus, they should fail during this conversion.
		{`9223372036854775807::float::int`, `integer out of range`},
		{`-9223372036854775808::float::int`, `integer out of range`},
		// The two smallest floats that cannot be converted to an int.
		{`9223372036854775296::float::int`, `integer out of range`},
		{`-9223372036854775296::float::int`, `integer out of range`},
		{`1e500::decimal::int`, `integer out of range`},
		{`1e500::decimal::float`, `float out of range`},
		{`1e300::decimal::float::int`, `integer out of range`},
		{`'Inf'::decimal::int`, `integer out of range`},
		{`'NaN'::decimal::int`, `integer out of range`},
		{`'Inf'::float::int`, `integer out of range`},
		{`'NaN'::float::int`, `integer out of range`},
		{`'1.1'::int`, `could not parse "1.1" as type int`},
		{`IFERROR(1/0, 123, 'unknown')`, `division by zero`},
		{`ISERROR(1/0, 'unknown')`, `division by zero`},
		{`like_escape('___', '\___', 'abc')`, `invalid escape string`},
		{`like_escape('abc', 'abc', 'a日')`, `invalid escape string`},
		{`like_escape('abc', 'abc', '漢日')`, `invalid escape string`},
		{`like_escape('__', '_', '_')`, `LIKE pattern must not end with escape character`},
		{`like_escape('%%', '%', '%')`, `LIKE pattern must not end with escape character`},
		{`like_escape('__', '___', '_')`, `LIKE pattern must not end with escape character`},
		{`like_escape('%%', '%%%', '%')`, `LIKE pattern must not end with escape character`},
		{`like_escape('abc', 'ab%', '%')`, `LIKE pattern must not end with escape character`},
		{`like_escape('abc', '%b%', '%')`, `LIKE pattern must not end with escape character`},
		{`like_escape('abc', 'ab_', '_')`, `LIKE pattern must not end with escape character`},
		{`like_escape('abc', '%b_', '_')`, `LIKE pattern must not end with escape character`},
		{`like_escape('abc', '%b漢', '漢')`, `LIKE pattern must not end with escape character`},
		{`similar_to_escape('abc', '-a-b-c', '-')`, `error parsing regexp: invalid escape sequence`},
		{`similar_to_escape('a(b)c', '%((_)_', '(')`, `error parsing regexp: unexpected )`},
		{`convert_from('\xaaaa'::bytea, 'woo')`, `convert_from(): invalid source encoding name "woo"`},
		{`convert_from('\xaaaa'::bytea, 'utf8')`, `convert_from(): invalid byte sequence for encoding "UTF8"`},
		{`convert_to('abc', 'woo')`, `convert_to(): invalid destination encoding name "woo"`},
		{`convert_to('漢', 'latin1')`, `convert_to(): character '漢' has no representation in encoding "LATIN1"`},
		{`'123'::BIT`, `could not parse string as bit array: "2" is not a valid binary digit`},
		{`B'1001' & B'101'`, `cannot AND bit strings of different sizes`},
		{`B'1001' | B'101'`, `cannot OR bit strings of different sizes`},
		{`B'1001' # B'101'`, `cannot XOR bit strings of different sizes`},
	}
	for _, d := range testData {
		expr, err := parser.ParseExpr(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		typedExpr, err := tree.TypeCheck(expr, nil, types.Any)
		if err == nil {
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			_, err = typedExpr.Eval(evalCtx)
		}
		if !testutils.IsError(err, strings.Replace(regexp.QuoteMeta(d.expected), `\.\*`, `.*`, -1)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
