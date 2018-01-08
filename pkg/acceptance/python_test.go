// Copyright 2016 The Cockroach Authors.
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

package acceptance

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerPython(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "python", []string{"python", "-c", strings.Replace(python, "%v", "3", 1)})
	testDockerFail(ctx, t, "python", []string{"python", "-c", strings.Replace(python, "%v", `"a"`, 1)})
}

const python = `
import psycopg2
import decimal
conn = psycopg2.connect('')
cur = conn.cursor()
cur.execute("SELECT 1, 2+%v")
v = cur.fetchall()
assert v == [(1, 5)]

# Verify #6597 (timestamp format) is fixed.
cur = conn.cursor()
cur.execute("SELECT now()")
v = cur.fetchall()

# Verify round-trip of strings containing backslashes.
# https://github.com/cockroachdb/cockroachdb-python/issues/23
s = ('\\\\',)
cur.execute("SELECT %s", s)
v = cur.fetchall()
assert v == [s], (v, s)

# Verify decimals with exponents can be parsed.
cur = conn.cursor()
cur.execute("SELECT 1e1::decimal")
v = cur.fetchall()
d = v[0][0]
assert type(d) is decimal.Decimal
# Use of compare_total here guarantees that we didn't just get '10' back, we got '1e1'.
assert d.compare_total(decimal.Decimal('1e1')) == 0

# Verify arrays with strings can be parsed.
cur = conn.cursor()
cur.execute("SELECT ARRAY['foo','bar','baz']")
v = cur.fetchall()
d = v[0][0]
assert d == ["foo","bar","baz"]

# Verify JSON values come through properly.
cur = conn.cursor()
cur.execute("SELECT '{\"a\":\"b\"}'::JSONB")
v = cur.fetchall()
d = v[0][0]
assert d == {"a": "b"}
`
