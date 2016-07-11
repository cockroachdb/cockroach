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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import "testing"

func TestNormalizeExpr(t *testing.T) {
	defer mockQualifiedNameTypes(map[string]Datum{
		"a": TypeInt,
		"b": TypeInt,
		"c": TypeInt,
		"d": TypeBool,
		"s": TypeString,
	})()
	testData := []struct {
		expr     string
		expected string
	}{
		{`(a)`, `a`},
		{`((((a))))`, `a`},
		{`CAST(NULL AS INTEGER)`, `NULL`},
		{`+a`, `a`},
		{`-(-a)`, `a`},
		{`-+-a`, `a`},
		{`-(a-b)`, `b - a`},
		{`-0`, `0`},
		{`-NULL`, `NULL`},
		{`-1`, `-1`},
		{`a+0`, `a`},
		{`0+a`, `a`},
		{`a+(2-2)`, `a`},
		{`a-0`, `a`},
		{`a*1`, `a`},
		{`1*a`, `a`},
		{`a+NULL`, `NULL`},
		{`a/1`, `CAST(a AS DECIMAL)`},
		{`0/a`, `0 / a`},
		{`0/1`, `0`},
		{`12 BETWEEN 24 AND 36`, `false`},
		{`12 BETWEEN 10 AND 20`, `true`},
		{`10 BETWEEN a AND 20`, `a <= 10`},
		{`a BETWEEN b AND c`, `(a >= b) AND (a <= c)`},
		{`a NOT BETWEEN b AND c`, `(a < b) OR (a > c)`},
		{`a BETWEEN NULL AND c`, `NULL`},
		{`NULL BETWEEN 1 AND 2`, `NULL`},
		{`1+1`, `2`},
		{`(1+1,2+2,3+3)`, `(2, 4, 6)`},
		{`a+(1+1)`, `a + 2`},
		{`1+1+a`, `2 + a`},
		{`1+NULL`, `NULL`},
		{`1+(1+NULL)`, `NULL`},
		{`a=1+1`, `a = 2`},
		{`a=1+(2*3)-4`, `a = 3`},
		{`true OR d`, `true`},
		{`false OR d`, `d`},
		{`NULL OR d`, `NULL OR d`},
		{`d OR true`, `true`},
		{`d OR false`, `d`},
		{`d OR NULL`, `d OR NULL`},
		{`true AND d`, `d`},
		{`false AND d`, `false`},
		{`NULL AND d`, `NULL AND d`},
		{`d AND true`, `d`},
		{`d AND false`, `false`},
		{`d AND NULL`, `d AND NULL`},
		{`1 IN (1, 2, 3)`, `true`},
		{`1 IN (3, 2, 1)`, `true`},
		{`a IN (3, 2, 1)`, `a IN (1, 2, 3)`},
		{`1 IN (1, 2, a)`, `1 IN (1, 2, a)`},
		{`NULL IN (1, 2, 3)`, `NULL`},
		{`NULL LIKE 'a'`, `NULL`},
		{`NULL NOT LIKE 'a'`, `NULL`},
		{`NULL ILIKE 'a'`, `NULL`},
		{`NULL NOT ILIKE 'a'`, `NULL`},
		{`NULL SIMILAR TO 'a'`, `NULL`},
		{`NULL NOT SIMILAR TO 'a'`, `NULL`},
		{`NULL ~ 'a'`, `NULL`},
		{`NULL !~ 'a'`, `NULL`},
		{`NULL ~* 'a'`, `NULL`},
		{`NULL !~* 'a'`, `NULL`},
		{`a<1`, `a < 1`},
		{`1>a`, `a < 1`},
		{`a<NULL`, `NULL`},
		{`(a+1)=2`, `a = 1`},
		{`(a-1)>=2`, `a >= 3`},
		{`(1+a)<=2`, `a <= 1`},
		{`(1-a)>2`, `a < -1`},
		{`2<(a+1)`, `a > 1`},
		{`2>(a-1)`, `a < 3`},
		{`2<(1+a)`, `a > 1`},
		{`2>(1-a)`, `a > -1`},
		{`(a+(1+1))=2`, `a = 0`},
		{`((a+1)+1)=2`, `a = 0`},
		{`a+1+1=2`, `a = 0`},
		{`1+1>=(b+c)`, `(b + c) <= 2`},
		{`b+c<=1+1`, `(b + c) <= 2`},
		{`a/2=1`, `a = 2`},
		{`1=a/2`, `a = 2`},
		{`s=lower('FOO')`, `s = 'foo'`},
		{`lower(s)='foo'`, `lower(s) = 'foo'`},
		{`random()`, `random()`},
		{`9223372036854775808`, `9223372036854775808`},
		{`-9223372036854775808`, `-9223372036854775808`},
		{`(1, 2, 3) = (1, 2, 3)`, `true`},
		{`(1, 2, 3) IN ((1, 2, 3), (4, 5, 6))`, `true`},
		{`(1, 'one')`, `(1, 'one')`},
		{`ANNOTATE_TYPE(1, float)`, `1.0`},
		// TODO(nvanbenschoten) introduce a shorthand type annotation notation.
		// {`1!float`, `1.0`},
		{`IF((true AND a < 0), (0 + a)::decimal, 2 / (1 - 1))`, `IF(a < 0, CAST(a AS DECIMAL), 2 / 0)`},
		{`IF((true OR a < 0), (0 + a)::decimal, 2 / (1 - 1))`, `CAST(a AS DECIMAL)`},
		{`COALESCE(NULL, (NULL < 3), a = 2 - 1, d)`, `COALESCE(a = 1, d)`},
		{`COALESCE(NULL, a)`, `a`},
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		typedExpr, err := expr.TypeCheck(nil, NoTypePreference)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		rOrig := typedExpr.String()
		ctx := &EvalContext{}
		r, err := ctx.NormalizeExpr(typedExpr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// Normalizing again should be a no-op.
		r2, err := ctx.NormalizeExpr(r)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// The original expression should be unchanged.
		if rStr := typedExpr.String(); rOrig != rStr {
			t.Fatalf("Original expression `%s` changed to `%s`", rOrig, rStr)
		}
	}
}
