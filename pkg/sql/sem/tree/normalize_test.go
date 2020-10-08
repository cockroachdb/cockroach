// Copyright 2015 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestContainsVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr     string
		expected bool
	}{
		{`123`, false},
		{`123+123`, false},
		{`$1`, true},
		{`123+$1`, true},
		{`@1`, true},
		{`123+@1`, true},
	}

	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			res := tree.ContainsVars(expr)
			if res != d.expected {
				t.Fatalf("%s: expected %v, got %v", d.expr, d.expected, res)
			}
		})
	}
}

func TestNormalizeExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer tree.MockNameTypes(map[string]*types.T{
		"a":  types.Int,
		"b":  types.Int,
		"c":  types.Int,
		"d":  types.Bool,
		"s":  types.String,
		"j":  types.Jsonb,
		"jv": types.Jsonb,
	})()
	testData := []struct {
		expr     string
		expected string
	}{
		{`(a)`, `a`},
		{`((((a))))`, `a`},
		// These expression previously always mapped INT2/INT4 to INT8, but after
		// unifying the type system, they now produce better results. Leaving the
		// tests here to make sure they don't regress. See
		// https://github.com/cockroachdb/cockroach/issues/32639
		{`CAST(NULL AS INT2)`, `CAST(NULL AS INT2)`},
		{`CAST(NULL AS INT4)`, `CAST(NULL AS INT4)`},
		{`CAST(NULL AS INT8)`, `CAST(NULL AS INT8)`},
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
		{`(1 + 2) BETWEEN b AND c`, `(b <= 3) AND (c >= 3)`},
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
		{`NULL IN ()`, `false`},
		{`NULL NOT IN ()`, `true`},
		{`NULL IN (1, 2, 3)`, `NULL`},
		{`a IN (NULL)`, `NULL`},
		{`a IN (NULL, NULL)`, `NULL`},
		{`1 IN (1, NULL)`, `true`},
		{`1 IN (2, NULL)`, `CAST(NULL AS BOOL)`},
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
		{`gen_random_uuid()`, `gen_random_uuid()`},
		{`current_date()`, `current_date()`},
		{`clock_timestamp()`, `clock_timestamp()`},
		{`now()`, `now()`},
		{`current_timestamp()`, `current_timestamp()`},
		{`current_timestamp(5)`, `current_timestamp(5)`},
		{`transaction_timestamp()`, `transaction_timestamp()`},
		{`statement_timestamp()`, `statement_timestamp()`},
		{`cluster_logical_timestamp()`, `cluster_logical_timestamp()`},
		{`clock_timestamp()`, `clock_timestamp()`},
		{`crdb_internal.force_error('a', 'b')`, `crdb_internal.force_error('a', 'b')`},
		{`crdb_internal.force_panic('a')`, `crdb_internal.force_panic('a')`},
		{`crdb_internal.force_log_fatal('a')`, `crdb_internal.force_log_fatal('a')`},
		{`crdb_internal.force_retry('1 day'::interval)`, `crdb_internal.force_retry('1 day')`},
		{`crdb_internal.no_constant_folding(123)`, `crdb_internal.no_constant_folding(123)`},
		{`crdb_internal.set_vmodule('a')`, `crdb_internal.set_vmodule('a')`},
		{`uuid_v4()`, `uuid_v4()`},
		{`experimental_uuid_v4()`, `experimental_uuid_v4()`},
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
		{`NOT NULL`, `CAST(NULL AS BOOL)`},
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
		{`d IS NOT DISTINCT FROM NULL`, `d IS NOT DISTINCT FROM NULL`},
		{`d IS DISTINCT FROM NULL`, `d IS DISTINCT FROM NULL`},
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
		{`NULL IS NOT DISTINCT FROM d`, `d IS NOT DISTINCT FROM NULL`},
		{`NULL IS DISTINCT FROM d`, `d IS DISTINCT FROM NULL`},
		// #15454: ensure that operators are pretty-printed correctly after normalization.
		{`(random() + 1.0)::INT8`, `(random() + 1.0)::INT8`},
		{`('a' || left('b', random()::INT8)) COLLATE en`, `('a' || left('b', random()::INT8)) COLLATE en`},
		{`NULL COLLATE en`, `CAST(NULL AS STRING) COLLATE en`},
		{`(1.0 + random()) IS OF (INT8)`, `(1.0 + random()) IS OF (INT8)`},
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
		// We want to check that constant-folded tuples preserve their
		// labels.
		{`(ROW (1) AS a)`, `((1,) AS a)`}, // DTuple
		{`(ROW (a) AS a)`, `((a,) AS a)`}, // Tuple
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			typedExpr, err := expr.TypeCheck(ctx, &semaCtx, types.Any)
			if err != nil {
				t.Fatalf("%s: %v", d.expr, err)
			}
			rOrig := typedExpr.String()
			ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer ctx.Mon.Stop(context.Background())
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
