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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package parser

import (
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
)

func TestTypeCheck(t *testing.T) {
	testData := []string{
		`NULL + 1`,
		`NULL + 1.1`,
		`NULL + '2006-09-23'::date`,
		`NULL + '1h'::interval`,
		`NULL + 'hello'`,
		`NULL + 'hello'::bytes`,
		`NULL = 1`,
		`1 = NULL`,
		`true AND NULL`,
		`NULL OR false`,
		`1 IN (SELECT 1)`,
		`IF(true, 2, 3)`,
		`IF(false, 2, 3)`,
		`IF(NULL, 2, 3)`,
		`IFNULL(1, 2)`,
		`IFNULL(NULL, 2)`,
		`IFNULL(2, NULL)`,
		`NULLIF(1, 2)`,
		`NULLIF(NULL, 2)`,
		`NULLIF(2, NULL)`,
		`COALESCE(1, 2, 3, 4, 5)`,
		`COALESCE(NULL, 2)`,
		`COALESCE(2, NULL)`,
		`true IS NULL`,
		`true IS NOT NULL`,
		`true IS TRUE`,
		`true IS NOT TRUE`,
		`true IS FALSE`,
		`true IS NOT FALSE`,
	}
	for _, d := range testData {
		q, err := ParseTraditional("SELECT " + d)
		if err != nil {
			t.Fatalf("%s: %v", d, err)
		}
		expr := q[0].(*Select).Exprs[0].Expr
		if _, err := expr.TypeCheck(nil); err != nil {
			t.Errorf("%s: unexpected error %s", d, err)
		}
	}
}

func TestTypeCheckError(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`'1' + '2'`, `unsupported binary operator:`},
		{`'a' + 0`, `unsupported binary operator:`},
		{`1.1 # 3.1`, `unsupported binary operator:`},
		{`1 / 0.0`, `unsupported binary operator:`},
		{`~0.1`, `unsupported unary operator:`},
		{`'10' > 2`, `unsupported comparison operator:`},
		{`a`, `qualified name "a" not found`},
		{`1 AND true`, `incompatible AND argument type: int`},
		{`1.0 AND true`, `incompatible AND argument type: float`},
		{`'a' OR true`, `incompatible OR argument type: string`},
		{`(1, 2) OR true`, `incompatible OR argument type: tuple`},
		{`NOT 1`, `incompatible NOT argument type: int`},
		{`lower()`, `unknown signature for lower: lower()`},
		{`lower(1, 2)`, `unknown signature for lower: lower(int, int)`},
		{`lower(1)`, `unknown signature for lower: lower(int)`},
		{`1::decimal`, `invalid cast: int -> DECIMAL`},
		{`1::date`, `invalid cast: int -> DATE`},
		{`1::timestamp`, `invalid cast: int -> TIMESTAMP`},
		{`CASE 'one' WHEN 1 THEN 1 WHEN 'two' THEN 2 END`, `incompatible condition type`},
		{`CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 2 END`, `incompatible value type`},
		{`CASE 1 WHEN 1 THEN 'one' ELSE 2 END`, `incompatible value type`},
		{`(1, 2, 3) = (1, 2)`, `unequal number of entries in tuple expressions`},
		{`(1, 2) = (1, 'a')`, `unsupported comparison operator`},
		{`1 IN ('a', 'b')`, `unsupported comparison operator:`},
		{`1 IN (1, 'a')`, `unsupported comparison operator`},
		{`IF(1, 2, 3)`, `IF condition must be a boolean: int`},
		{`IF(true, 2, 3.0)`, `incompatible IF expressions int, float`},
		{`IFNULL(1, 2.0)`, `incompatible IFNULL expressions int, float`},
		{`NULLIF(1, 2.0)`, `incompatible NULLIF expressions int, float`},
		{`COALESCE(1, 2.0)`, `incompatible COALESCE expressions int, float`},
		{`COALESCE(1, 2, 3, 4, '5')`, `incompatible COALESCE expressions int, string`},
	}
	for _, d := range testData {
		q, err := ParseTraditional("SELECT " + d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		expr := q[0].(*Select).Exprs[0].Expr
		if _, err := expr.TypeCheck(nil); !testutils.IsError(err, regexp.QuoteMeta(d.expected)) {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, err)
		}
	}
}
