# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

import decimal
import sys
import psycopg

conn = psycopg.connect('')
cur = conn.cursor()
cur.execute("SELECT 1, 2+{}".format(sys.argv[1]))
v = cur.fetchall()
assert v == [(1, 5)]

# Verify #6597 (timestamp format) is fixed.
cur = conn.cursor()
cur.execute("SELECT now()")
v = cur.fetchall()

# Verify round-trip of strings containing backslashes.
# https://github.com/cockroachdb/cockroachdb-python/issues/23
s = ('\\\\',)
cur.execute("SELECT %s::STRING", s)
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


# Verify queries with large result sets can be run and do not
# hang.
query = """
    SELECT
        '000000000000000000000' AS aaaaaaaa,
        '000000000000000000000' AS b,
        '000000000000000000000' AS c,
        '000000000000000000000' AS d,
        '000000000000000000000' AS e,
        '000000000000000000000' AS f,
        '000000000000000000000' AS g,
        '0000000000000000000' AS h
    FROM generate_series(1, 238)
"""

for i in range(30):
  cur.execute(query)
  v = cur.fetchall()
  assert len(v) == 238

