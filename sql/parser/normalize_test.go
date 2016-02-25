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

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
)

func TestNormalizeExpr(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`(a)`, `a`},
		{`((((a))))`, `a`},
		{`ROW(a)`, `(a)`},
		{`a BETWEEN b AND c`, `a >= b AND a <= c`},
		{`a NOT BETWEEN b AND c`, `a < b OR a > c`},
		{`1+1`, `2`},
		{`(1+1,2+2,3+3)`, `(2, 4, 6)`},
		{`a+(1+1)`, `a + 2`},
		{`1+1+a`, `2 + a`},
		{`a=1+1`, `a = 2`},
		{`a=1+(2*3)-4`, `a = 3`},
		{`true OR a`, `true`},
		{`false OR a`, `a`},
		{`NULL OR a`, `NULL OR a`},
		{`a OR true`, `true`},
		{`a OR false`, `a`},
		{`a OR NULL`, `a OR NULL`},
		{`true AND a`, `a`},
		{`false AND a`, `false`},
		{`NULL AND a`, `NULL AND a`},
		{`a AND true`, `a`},
		{`a AND false`, `false`},
		{`a AND NULL`, `a AND NULL`},
		{`1 IN (1, 2, 3)`, `true`},
		{`1 IN (3, 2, 1)`, `true`},
		{`a IN (3, 2, 1)`, `a IN (1, 2, 3)`},
		{`1 IN (1, 2, a)`, `1 IN (1, 2, a)`},
		{`a<1`, `a < 1`},
		{`1>a`, `a < 1`},
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
		{`1+1>=(b+c)`, `b + c <= 2`},
		{`b+c<=1+1`, `b + c <= 2`},
		{`a/2=1`, `a = 2`},
		{`1=a/2`, `a = 2`},
		{`a=lower('FOO')`, `a = 'foo'`},
		{`lower(a)='foo'`, `lower(a) = 'foo'`},
		{`random()`, `random()`},
		{`notARealMethod()`, `notARealMethod()`},
		{`-9223372036854775808`, `-9223372036854775808`},
		{`(SELECT 1)`, `(SELECT 1)`},
		{`(1, 2, 3) = (SELECT 1, 2, 3)`, `(1, 2, 3) = (SELECT 1, 2, 3)`},
		{`(1, 2, 3) IN (SELECT 1, 2, 3)`, `(1, 2, 3) IN (SELECT 1, 2, 3)`},
		{`(1, 'one')`, `(1, 'one')`},
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		rOrig := expr.String()
		r, err := defaultContext.NormalizeExpr(expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// Normalizing again should be a no-op.
		r2, err := defaultContext.NormalizeExpr(r)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// The original expression should be unchanged.
		if rStr := expr.String(); rOrig != rStr {
			t.Fatalf("Original expression `%s` changed to `%s`", rOrig, rStr)
		}
	}
}

func TestNormalizeExprError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`9223372036854775808`, `integer value out of range`},
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if _, err := defaultContext.NormalizeExpr(expr); !testutils.IsError(err, d.expected) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
