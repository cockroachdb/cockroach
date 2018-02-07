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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestNormalizeExpr(t *testing.T) {
	defer tree.MockNameTypes(map[string]types.T{
		"a":  types.Int,
		"b":  types.Int,
		"c":  types.Int,
		"d":  types.Bool,
		"s":  types.String,
		"j":  types.JSON,
		"jv": types.JSON,
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
		{`a BETWEEN SYMMETRIC b AND c`, `((a >= b) AND (a <= c)) OR ((a >= c) AND (a <= b))`},
		{`a NOT BETWEEN b AND c`, `(a < b) OR (a > c)`},
		{`a NOT BETWEEN SYMMETRIC b AND c`, `((a < b) OR (a > c)) AND ((a < c) OR (a > b))`},
		{`a BETWEEN NULL AND c`, `NULL AND (a <= c)`},
		{`a BETWEEN SYMMETRIC NULL AND c`, `(NULL AND (a <= c)) OR ((a >= c) AND NULL)`},
		{`a BETWEEN b AND NULL`, `(a >= b) AND NULL`},
		{`a BETWEEN SYMMETRIC b AND NULL`, `((a >= b) AND NULL) OR (NULL AND (a <= b))`},
		{`a BETWEEN NULL AND NULL`, `NULL`},
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
		{`a IN (NULL)`, `NULL`},
		{`a IN (NULL, NULL)`, `NULL`},
		{`1 IN (1, NULL)`, `true`},
		{`1 IN (2, NULL)`, `NULL`},
		{`1 = ANY ARRAY[3, 2, 1]`, `true`},
		{`1 < SOME ARRAY[3, 2, 1]`, `true`},
		{`1 > SOME (ARRAY[3, 2, 1])`, `false`},
		{`1 > SOME (NULL)`, `NULL`},
		{`1 > SOME (((NULL)))`, `NULL`},
		{`NULL > SOME ARRAY[3, 2, 1]`, `NULL`},
		{`NULL > ALL ARRAY[3, 2, 1]`, `NULL`},
		{`4 > ALL ARRAY[3, 2, 1]`, `true`},
		{`a > ALL ARRAY[3, 2, 1]`, `a > ALL ARRAY[3,2,1]`},
		{`3 > ALL ARRAY[3, 2, a]`, `3 > ALL ARRAY[3, 2, a]`},
		{`3 > ANY (ARRAY[3, 2, a])`, `3 > ANY ARRAY[3, 2, a]`},
		{`3 > SOME (((ARRAY[3, 2, a])))`, `3 > SOME ARRAY[3, 2, a]`},
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
		{`a=count('FOO') OVER ()`, `a = count('FOO') OVER ()`},
		{`9223372036854775808`, `9223372036854775808`},
		{`-9223372036854775808`, `-9223372036854775808`},
		{`(1, 2, 3) = (1, 2, 3)`, `true`},
		{`(1, 2, 3) IN ((1, 2, 3), (4, 5, 6))`, `true`},
		{`(1, 'one')`, `(1, 'one')`},
		{`ANNOTATE_TYPE(1, float)`, `1.0`},
		{`1:::float`, `1.0`},
		{`IF((true AND a < 0), (0 + a)::decimal, 2 / (1 - 1))`, `IF(a < 0, a::DECIMAL, 2 / 0)`},
		{`IF((true OR a < 0), (0 + a)::decimal, 2 / (1 - 1))`, `a::DECIMAL`},
		{`COALESCE(NULL, (NULL < 3), a = 2 - 1, d)`, `COALESCE(a = 1, d)`},
		{`COALESCE(NULL, a)`, `a`},
		{`NOT NULL`, `NULL`},
		{`NOT d`, `NOT d`},
		{`NOT NOT d`, `d`},
		{`NOT NOT NOT d`, `NOT d`},
		{`NOT NOT NOT NOT d`, `d`},
		{`NULL IS NULL`, `true`},
		{`NULL IS NOT NULL`, `false`},
		{`1 IS NULL`, `false`},
		{`1 IS NOT NULL`, `true`},
		{`d IS NULL`, `d IS NULL`},
		{`d IS NOT NULL`, `d IS NOT NULL`},
		{`NULL IS TRUE`, `false`},
		{`NULL IS NOT TRUE`, `true`},
		{`false IS TRUE`, `false`},
		{`false IS NOT TRUE`, `true`},
		{`NULL IS FALSE`, `false`},
		{`NULL IS NOT FALSE`, `true`},
		{`false IS FALSE`, `true`},
		{`false IS NOT FALSE`, `false`},
		{`NULL IS DISTINCT FROM NULL`, `false`},
		{`1 IS NOT DISTINCT FROM NULL`, `false`},
		{`1 IS DISTINCT FROM NULL`, `true`},
		{`d IS NOT DISTINCT FROM NULL`, `d IS NULL`},
		{`d IS DISTINCT FROM NULL`, `d IS NOT NULL`},
		{`NULL IS NOT DISTINCT FROM TRUE`, `false`},
		{`NULL IS DISTINCT FROM TRUE`, `true`},
		{`false IS NOT DISTINCT FROM TRUE`, `false`},
		{`false IS DISTINCT FROM TRUE`, `true`},
		{`NULL IS NOT DISTINCT FROM FALSE`, `false`},
		{`NULL IS DISTINCT FROM FALSE`, `true`},
		{`false IS NOT DISTINCT FROM FALSE`, `true`},
		{`false IS DISTINCT FROM FALSE`, `false`},
		{`NULL IS NOT DISTINCT FROM 1`, `false`},
		{`NULL IS DISTINCT FROM 1`, `true`},
		{`NULL IS NOT DISTINCT FROM d`, `d IS NULL`},
		{`NULL IS DISTINCT FROM d`, `d IS NOT NULL`},
		// #15454: ensure that operators are pretty-printed correctly after normalization.
		{`(random() + 1.0)::INT`, `(random() + 1.0)::INT`},
		{`('a' || left('b', random()::INT)) COLLATE en`, `('a' || left('b', random()::INT)) COLLATE en`},
		{`(1.0 + random()) IS OF (INT)`, `(1.0 + random()) IS OF (INT)`},
		// #14687: ensure that negative divisors flip the inequality when rotating.
		{`1 < a / -2`, `a < -2`},
		{`1 <= a / -2`, `a <= -2`},
		{`1 > a / -2`, `a > -2`},
		{`1 >= a / -2`, `a >= -2`},
		{`1 = a / -2`, `a = -2`},
		{`1 < a / 2`, `a > 2`},
		{`1 <= a / 2`, `a >= 2`},
		{`1 > a / 2`, `a < 2`},
		{`1 >= a / 2`, `a <= 2`},
		{`1 = a / 2`, `a = 2`},
		{`a - 1 < 9223372036854775807`, `(a - 1) < 9223372036854775807`},
		{`a - 1 < 9223372036854775806`, `a < 9223372036854775807`},
		{`-1 + a < 9223372036854775807`, `(-1 + a) < 9223372036854775807`},
		{`-1 + a < 9223372036854775806`, `a < 9223372036854775807`},
		{`j->'s' = '"jv"'::JSONB`, `j @> '{"s": "jv"}'`},
		{`'"jv"'::JSONB = j->'s'`, `j @> '{"s": "jv"}'`},
		{`j->'s' = jv`, `(j->'s') = jv`},
		{`j->s = jv`, `(j->s) = jv`},
		{`j->2 = '"jv"'::JSONB`, `(j->2) = '"jv"'`},
	}
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			typedExpr, err := expr.TypeCheck(nil, types.Any)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			rOrig := typedExpr.String()
			ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer ctx.Mon.Stop(context.Background())
			defer ctx.ActiveMemAcc.Close(context.Background())
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
		})
	}
}
