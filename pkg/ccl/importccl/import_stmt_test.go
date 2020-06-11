// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skipf("failing on teamcity with testrace")

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)

	tests := []struct {
		name     string
		create   string
		with     string
		typ      string
		data     string
		err      string
		rejected string
		query    map[string][][]string
	}{
		{
			name: "duplicate unique index key",
			create: `
				a int8 primary key,
				i int8,
				unique index idx_f (i)
			`,
			typ: "CSV",
			data: `1,1
2,2
3,3
4,3
5,4`,
			err: "duplicate key",
		},
		{
			name: "duplicate PK",
			create: `
				i int8 primary key,
				s string
			`,
			typ: "CSV",
			data: `1, A
2, B
3, C
3, D
4, E`,
			err: "duplicate key",
		},
		{
			name: "duplicate collated string key",
			create: `
				s string collate en_u_ks_level1 primary key
			`,
			typ: "CSV",
			data: `a
B
c
D
d
`,
			err: "duplicate key",
		},
		{
			name: "duplicate PK at sst boundary",
			create: `
				i int8 primary key,
				s string
			`,
			with: `WITH sstsize = '10B'`,
			typ:  "CSV",
			data: `1,0000000000
1,0000000001`,
			err: "duplicate key",
		},
		{
			name: "verify no splits mid row",
			create: `
				i int8 primary key,
				s string,
				b int8,
				c int8,
				index (s),
				index (i, s),
				family (i, b),
				family (s, c)
			`,
			with: `WITH sstsize = '1B'`,
			typ:  "CSV",
			data: `5,STRING,7,9`,
			query: map[string][][]string{
				`SELECT count(*) from t`: {{"1"}},
			},
		},
		{
			name:   "good bytes encoding",
			create: `b bytes`,
			typ:    "CSV",
			data: `\x0143
0143`,
			query: map[string][][]string{
				`SELECT * from t`: {{"\x01C"}, {"0143"}},
			},
		},
		{
			name:     "invalid byte",
			create:   `b bytes`,
			typ:      "CSV",
			data:     `\x0g`,
			rejected: `\x0g` + "\n",
			err:      "invalid byte",
		},
		{
			name:     "bad bytes length",
			create:   `b bytes`,
			typ:      "CSV",
			data:     `\x0`,
			rejected: `\x0` + "\n",
			err:      "odd length hex string",
		},
		{
			name:   "oversample",
			create: `i int8`,
			with:   `WITH oversample = '100'`,
			typ:    "CSV",
			data:   "1",
		},
		{
			name:   "new line characters",
			create: `t text`,
			typ:    "CSV",
			data:   "\"hello\r\nworld\"\n\"friend\nfoe\"\n\"mr\rmrs\"",
			query: map[string][][]string{
				`SELECT t from t`: {{"hello\r\nworld"}, {"friend\nfoe"}, {"mr\rmrs"}},
			},
		},
		{
			name:   "CR in int8, 2 cols",
			create: `a int8, b int8`,
			typ:    "CSV",
			data:   "1,2\r\n3,4\n5,6",
			query: map[string][][]string{
				`SELECT * FROM t ORDER BY a`: {{"1", "2"}, {"3", "4"}, {"5", "6"}},
			},
		},
		{
			name:   "CR in int8, 1 col",
			create: `a int8`,
			typ:    "CSV",
			data:   "1\r\n3\n5",
			query: map[string][][]string{
				`SELECT * FROM t ORDER BY a`: {{"1"}, {"3"}, {"5"}},
			},
		},
		{
			name:   "collated strings",
			create: `s string collate en_u_ks_level1`,
			typ:    "CSV",
			data:   strings.Repeat("1\n", 2000),
			query: map[string][][]string{
				`SELECT s, count(*) FROM t GROUP BY s`: {{"1", "2000"}},
			},
		},
		{
			name:   "quotes are accepted in a quoted string",
			create: `s string`,
			typ:    "CSV",
			data:   `"abc""de"`,
			query: map[string][][]string{
				`SELECT s FROM t`: {{`abc"de`}},
			},
		},
		{
			name:   "bare quote in the middle of a field that is not quoted",
			create: `s string`,
			typ:    "CSV",
			data:   `abc"de`,
			query:  map[string][][]string{`SELECT * from t`: {{`abc"de`}}},
		},
		{
			name:   "strict quotes: bare quote in the middle of a field that is not quoted",
			create: `s string`,
			typ:    "CSV",
			with:   `WITH strict_quotes`,
			data:   `abc"de`,
			err:    `row 1: reading CSV record: parse error on line 1, column 3: bare " in non-quoted-field`,
		},
		{
			name:   "no matching quote in a quoted field",
			create: `s string`,
			typ:    "CSV",
			data:   `"abc"de`,
			query:  map[string][][]string{`SELECT * from t`: {{`abc"de`}}},
		},
		{
			name:   "strict quotes: bare quote in the middle of a quoted field is not ok",
			create: `s string`,
			typ:    "CSV",
			with:   `WITH strict_quotes`,
			data:   `"abc"de"`,
			err:    `row 1: reading CSV record: parse error on line 1, column 4: extraneous or missing " in quoted-field`,
		},
		{
			name:     "too many imported columns",
			create:   `i int8`,
			typ:      "CSV",
			data:     "1,2\n3\n11,22",
			err:      "row 1: expected 1 fields, got 2",
			rejected: "1,2\n11,22\n",
			query:    map[string][][]string{`SELECT * from t`: {{"3"}}},
		},
		{
			name:     "parsing error",
			create:   `i int8, j int8`,
			typ:      "CSV",
			data:     "not_int,2\n3,4",
			err:      `row 1: parse "i" as INT8: could not parse "not_int" as type int`,
			rejected: "not_int,2\n",
			query:    map[string][][]string{`SELECT * from t`: {{"3", "4"}}},
		},

		// MySQL OUTFILE
		// If err field is non-empty, the query filed specifies what expect
		// to get from the rows that are parsed correctly (see option experimental_save_rejected).
		{
			name:   "empty file",
			create: `a string`,
			typ:    "DELIMITED",
			data:   "",
			query:  map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:   "empty field",
			create: `a string, b string`,
			typ:    "DELIMITED",
			data:   "\t",
			query:  map[string][][]string{`SELECT * from t`: {{"", ""}}},
		},
		{
			name:   "empty line",
			create: `a string`,
			typ:    "DELIMITED",
			data:   "\n",
			query:  map[string][][]string{`SELECT * from t`: {{""}}},
		},
		{
			name:     "too many imported columns",
			create:   `i int8`,
			typ:      "DELIMITED",
			data:     "1\t2\n3",
			err:      "row 1: too many columns, got 2 expected 1",
			rejected: "1\t2\n",
			query:    map[string][][]string{`SELECT * from t`: {{"3"}}},
		},
		{
			name:     "cannot parse data",
			create:   `i int8, j int8`,
			typ:      "DELIMITED",
			data:     "bad_int\t2\n3\t4",
			err:      "error parsing row 1",
			rejected: "bad_int\t2\n",
			query:    map[string][][]string{`SELECT * from t`: {{"3", "4"}}},
		},
		{
			name:     "unexpected number of columns",
			create:   `a string, b string`,
			typ:      "DELIMITED",
			data:     "1,2\n3\t4",
			err:      "row 1: unexpected number of columns, expected 2 got 1",
			rejected: "1,2\n",
			query:    map[string][][]string{`SELECT * from t`: {{"3", "4"}}},
		},
		{
			name:     "unexpected number of columns in 1st row",
			create:   `a string, b string`,
			typ:      "DELIMITED",
			data:     "1,2\n3\t4",
			err:      "row 1: unexpected number of columns, expected 2 got 1",
			rejected: "1,2\n",
			query:    map[string][][]string{`SELECT * from t`: {{"3", "4"}}},
		},
		{
			name:   "field enclosure",
			create: `a string, b string`,
			with:   `WITH fields_enclosed_by = '$'`,
			typ:    "DELIMITED",
			data:   "$foo$\tnormal",
			query: map[string][][]string{
				`SELECT * from t`: {{"foo", "normal"}},
			},
		},
		{
			name:   "field enclosure in middle of unquoted field",
			create: `a string, b string`,
			with:   `WITH fields_enclosed_by = '$'`,
			typ:    "DELIMITED",
			data:   "fo$o\tb$a$z",
			query: map[string][][]string{
				`SELECT * from t`: {{"fo$o", "b$a$z"}},
			},
		},
		{
			name:   "field enclosure in middle of quoted field",
			create: `a string, b string`,
			with:   `WITH fields_enclosed_by = '$'`,
			typ:    "DELIMITED",
			data:   "$fo$o$\t$b$a$z$",
			query: map[string][][]string{
				`SELECT * from t`: {{"fo$o", "b$a$z"}},
			},
		},
		{
			name:     "unmatched field enclosure",
			create:   `a string, b string`,
			with:     `WITH fields_enclosed_by = '$'`,
			typ:      "DELIMITED",
			data:     "$foo\tnormal\nbaz\tbar",
			err:      "error parsing row 1: unmatched field enclosure at start of field",
			rejected: "$foo\tnormal\nbaz\tbar\n",
			query:    map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:     "unmatched field enclosure at end",
			create:   `a string, b string`,
			with:     `WITH fields_enclosed_by = '$'`,
			typ:      "DELIMITED",
			data:     "foo$\tnormal\nbar\tbaz",
			err:      "row 1: unmatched field enclosure at end of field",
			rejected: "foo$\tnormal\n",
			query:    map[string][][]string{`SELECT * from t`: {{"bar", "baz"}}},
		},
		{
			name:     "unmatched field enclosure 2nd field",
			create:   `a string, b string`,
			with:     `WITH fields_enclosed_by = '$'`,
			typ:      "DELIMITED",
			data:     "normal\t$foo",
			err:      "row 1: unmatched field enclosure at start of field",
			rejected: "normal\t$foo\n",
			query:    map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:     "unmatched field enclosure at end 2nd field",
			create:   `a string, b string`,
			with:     `WITH fields_enclosed_by = '$'`,
			typ:      "DELIMITED",
			data:     "normal\tfoo$",
			err:      "row 1: unmatched field enclosure at end of field",
			rejected: "normal\tfoo$\n",
			query:    map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:     "unmatched literal",
			create:   `i int8`,
			with:     `WITH fields_escaped_by = '\'`,
			typ:      "DELIMITED",
			data:     `\`,
			err:      "row 1: unmatched literal",
			rejected: "\\\n",
			query:    map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:   "escaped field enclosure",
			create: `a string, b string`,
			with: `WITH fields_enclosed_by = '$', fields_escaped_by = '\',
				    fields_terminated_by = ','`,
			typ:  "DELIMITED",
			data: `\$foo\$,\$baz`,
			query: map[string][][]string{
				`SELECT * from t`: {{"$foo$", "$baz"}},
			},
		},
		{
			name:   "weird escape char",
			create: `s STRING`,
			with:   `WITH fields_escaped_by = '@'`,
			typ:    "DELIMITED",
			data:   "@N\nN@@@\n\nNULL",
			query: map[string][][]string{
				`SELECT COALESCE(s, '(null)') from t`: {{"(null)"}, {"N@\n"}, {"NULL"}},
			},
		},
		{
			name:   `null and \N with escape`,
			create: `s STRING`,
			with:   `WITH fields_escaped_by = '\'`,
			typ:    "DELIMITED",
			data:   "\\N\n\\\\N\nNULL",
			query: map[string][][]string{
				`SELECT COALESCE(s, '(null)') from t`: {{"(null)"}, {`\N`}, {"NULL"}},
			},
		},
		{
			name:     `\N with trailing char`,
			create:   `s STRING`,
			with:     `WITH fields_escaped_by = '\'`,
			typ:      "DELIMITED",
			data:     "\\N1\nfoo",
			err:      "row 1: unexpected data after null encoding",
			rejected: "\\N1\n",
			query:    map[string][][]string{`SELECT * from t`: {{"foo"}}},
		},
		{
			name:     `double null`,
			create:   `s STRING`,
			with:     `WITH fields_escaped_by = '\'`,
			typ:      "DELIMITED",
			data:     `\N\N`,
			err:      "row 1: unexpected null encoding",
			rejected: `\N\N` + "\n",
			query:    map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:   `null and \N without escape`,
			create: `s STRING`,
			typ:    "DELIMITED",
			data:   "\\N\n\\\\N\nNULL",
			query: map[string][][]string{
				`SELECT COALESCE(s, '(null)') from t`: {{`\N`}, {`\\N`}, {"(null)"}},
			},
		},
		{
			name:   `bytes with escape`,
			create: `b BYTES`,
			typ:    "DELIMITED",
			data:   `\x`,
			query: map[string][][]string{
				`SELECT * from t`: {{`\x`}},
			},
		},
		{
			name:   "skip 0 lines",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', skip = '0'`,
			typ:    "DELIMITED",
			data:   "foo,normal",
			query: map[string][][]string{
				`SELECT * from t`: {{"foo", "normal"}},
			},
		},
		{
			name:   "skip 1 lines",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', skip = '1'`,
			typ:    "DELIMITED",
			data:   "a string, b string\nfoo,normal",
			query: map[string][][]string{
				`SELECT * from t`: {{"foo", "normal"}},
			},
		},
		{
			name:   "skip 2 lines",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', skip = '2'`,
			typ:    "DELIMITED",
			data:   "a string, b string\nfoo,normal\nbar,baz",
			query: map[string][][]string{
				`SELECT * from t`: {{"bar", "baz"}},
			},
		},
		{
			name:   "skip all lines",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', skip = '3'`,
			typ:    "DELIMITED",
			data:   "a string, b string\nfoo,normal\nbar,baz",
			query: map[string][][]string{
				`SELECT * from t`: {},
			},
		},
		{
			name:   "skip > all lines",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', skip = '4'`,
			typ:    "DELIMITED",
			data:   "a string, b string\nfoo,normal\nbar,baz",
			query:  map[string][][]string{`SELECT * from t`: {}},
		},
		{
			name:   "skip -1 lines",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', skip = '-1'`,
			typ:    "DELIMITED",
			data:   "a string, b string\nfoo,normal",
			err:    "pq: skip must be >= 0",
		},
		{
			name:   "nullif empty string",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', nullif = ''`,
			typ:    "DELIMITED",
			data:   ",normal",
			query: map[string][][]string{
				`SELECT * from t`: {{"NULL", "normal"}},
			},
		},
		{
			name:   "nullif empty string plus escape",
			create: `a INT8, b INT8`,
			with:   `WITH fields_terminated_by = ',', fields_escaped_by = '\', nullif = ''`,
			typ:    "DELIMITED",
			data:   ",4",
			query: map[string][][]string{
				`SELECT * from t`: {{"NULL", "4"}},
			},
		},
		{
			name:   "nullif single char string",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', nullif = 'f'`,
			typ:    "DELIMITED",
			data:   "f,normal",
			query: map[string][][]string{
				`SELECT * from t`: {{"NULL", "normal"}},
			},
		},
		{
			name:   "nullif multiple char string",
			create: `a string, b string`,
			with:   `WITH fields_terminated_by = ',', nullif = 'foo'`,
			typ:    "DELIMITED",
			data:   "foo,foop",
			query: map[string][][]string{
				`SELECT * from t`: {{"NULL", "foop"}},
			},
		},

		// PG COPY
		{
			name:   "unexpected escape x",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\x`,
			err:    `row 1: unsupported escape sequence: \\x`,
		},
		{
			name:   "unexpected escape 3",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\3`,
			err:    `row 1: unsupported escape sequence: \\3`,
		},
		{
			name:   "escapes",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\x43\122`,
			query: map[string][][]string{
				`SELECT * from t`: {{"CR"}},
			},
		},
		{
			name:   "normal",
			create: `i int8, s string`,
			typ:    "PGCOPY",
			data:   "1\tSTR\n2\t\\N\n\\N\t\\t",
			query: map[string][][]string{
				`SELECT * from t`: {{"1", "STR"}, {"2", "NULL"}, {"NULL", "\t"}},
			},
		},
		{
			name:   "comma delim",
			create: `i int8, s string`,
			typ:    "PGCOPY",
			with:   `WITH delimiter = ','`,
			data:   "1,STR\n2,\\N\n\\N,\\,",
			query: map[string][][]string{
				`SELECT * from t`: {{"1", "STR"}, {"2", "NULL"}, {"NULL", ","}},
			},
		},
		{
			name:   "size out of range",
			create: `i int8`,
			typ:    "PGCOPY",
			with:   `WITH max_row_size = '10GB'`,
			err:    "max_row_size out of range",
		},
		{
			name:   "line too long",
			create: `i int8`,
			typ:    "PGCOPY",
			data:   "123456",
			with:   `WITH max_row_size = '5B'`,
			err:    "line too long",
		},
		{
			name:   "not enough values",
			typ:    "PGCOPY",
			create: "a INT8, b INT8",
			data:   `1`,
			err:    "expected 2 values, got 1",
		},
		{
			name:   "too many values",
			typ:    "PGCOPY",
			create: "a INT8, b INT8",
			data:   "1\t2\t3",
			err:    "expected 2 values, got 3",
		},

		// Postgres DUMP
		{
			name: "mismatch cols",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (i int8);
				COPY t (s) FROM stdin;
				0
				\.
			`,
			err: `COPY columns do not match table columns for table t`,
		},
		{
			name: "missing COPY done",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (i int8);
				COPY t (i) FROM stdin;
0
`,
			err: `unexpected EOF`,
		},
		{
			name: "semicolons and comments",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (i int8);
				;;;
				-- nothing ;
				;
				-- blah
			`,
			query: map[string][][]string{
				`SELECT * from t`: {},
			},
		},
		{
			name: "size out of range",
			typ:  "PGDUMP",
			with: `WITH max_row_size = '10GB'`,
			err:  "max_row_size out of range",
		},
		{
			name: "line too long",
			typ:  "PGDUMP",
			data: "CREATE TABLE t (i INT8);",
			with: `WITH max_row_size = '5B'`,
			err:  "line too long",
		},
		{
			name: "not enough values",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);

COPY t (a, b) FROM stdin;
1
\.
			`,
			err: "expected 2 values, got 1",
		},
		{
			name: "too many values",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);

COPY t (a, b) FROM stdin;
1	2	3
\.
			`,
			err: "expected 2 values, got 3",
		},
		{
			name: "too many cols",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);

COPY t (a, b, c) FROM stdin;
1	2	3
\.
			`,
			err: "expected 2 columns, got 3",
		},
		{
			name: "fk",
			typ:  "PGDUMP",
			data: testPgdumpFk,
			query: map[string][][]string{
				`SHOW TABLES`:              {{"public", "cities", "table"}, {"public", "weather", "table"}},
				`SELECT city FROM cities`:  {{"Berkeley"}},
				`SELECT city FROM weather`: {{"Berkeley"}},

				`SELECT dependson_name
				FROM crdb_internal.backward_dependencies
				`: {{"weather_city_fkey"}},

				`SELECT create_statement
				FROM crdb_internal.create_statements
				WHERE descriptor_name in ('cities', 'weather')
				ORDER BY descriptor_name
				`: {{testPgdumpCreateCities}, {testPgdumpCreateWeather}},

				// Verify the constraint is unvalidated.
				`SHOW CONSTRAINTS FROM weather
				`: {{"weather", "weather_city_fkey", "FOREIGN KEY", "FOREIGN KEY (city) REFERENCES cities(city)", "false"}},
			},
		},
		{
			name: "fk-circular",
			typ:  "PGDUMP",
			data: testPgdumpFkCircular,
			query: map[string][][]string{
				`SHOW TABLES`:        {{"public", "a", "table"}, {"public", "b", "table"}},
				`SELECT i, k FROM a`: {{"2", "2"}},
				`SELECT j FROM b`:    {{"2"}},

				`SELECT dependson_name
				FROM crdb_internal.backward_dependencies ORDER BY dependson_name`: {
					{"a_i_fkey"},
					{"a_k_fkey"},
					{"b_j_fkey"},
				},

				`SELECT create_statement
				FROM crdb_internal.create_statements
				WHERE descriptor_name in ('a', 'b')
				ORDER BY descriptor_name
				`: {{
					`CREATE TABLE a (
	i INT8 NOT NULL,
	k INT8 NULL,
	CONSTRAINT a_pkey PRIMARY KEY (i ASC),
	CONSTRAINT a_i_fkey FOREIGN KEY (i) REFERENCES b(j),
	CONSTRAINT a_k_fkey FOREIGN KEY (k) REFERENCES a(i),
	INDEX a_auto_index_a_k_fkey (k ASC),
	FAMILY "primary" (i, k)
)`}, {
					`CREATE TABLE b (
	j INT8 NOT NULL,
	CONSTRAINT b_pkey PRIMARY KEY (j ASC),
	CONSTRAINT b_j_fkey FOREIGN KEY (j) REFERENCES a(i),
	FAMILY "primary" (j)
)`,
				}},

				`SHOW CONSTRAINTS FROM a`: {
					{"a", "a_i_fkey", "FOREIGN KEY", "FOREIGN KEY (i) REFERENCES b(j)", "false"},
					{"a", "a_k_fkey", "FOREIGN KEY", "FOREIGN KEY (k) REFERENCES a(i)", "false"},
					{"a", "a_pkey", "PRIMARY KEY", "PRIMARY KEY (i ASC)", "true"},
				},
				`SHOW CONSTRAINTS FROM b`: {
					{"b", "b_j_fkey", "FOREIGN KEY", "FOREIGN KEY (j) REFERENCES a(i)", "false"},
					{"b", "b_pkey", "PRIMARY KEY", "PRIMARY KEY (j ASC)", "true"},
				},
			},
		},
		{
			name: "fk-skip",
			typ:  "PGDUMP",
			data: testPgdumpFk,
			with: `WITH skip_foreign_keys`,
			query: map[string][][]string{
				`SHOW TABLES`: {{"public", "cities", "table"}, {"public", "weather", "table"}},
				// Verify the constraint is skipped.
				`SELECT dependson_name FROM crdb_internal.backward_dependencies`: {},
				`SHOW CONSTRAINTS FROM weather`:                                  {},
			},
		},
		{
			name: "fk unreferenced",
			typ:  "TABLE weather FROM PGDUMP",
			data: testPgdumpFk,
			err:  `table "cities" not found`,
		},
		{
			name: "fk unreferenced skipped",
			typ:  "TABLE weather FROM PGDUMP",
			data: testPgdumpFk,
			with: `WITH skip_foreign_keys`,
			query: map[string][][]string{
				`SHOW TABLES`: {{"public", "weather", "table"}},
			},
		},
		{
			name: "sequence",
			typ:  "PGDUMP",
			data: `
					CREATE TABLE t (a INT8);
					CREATE SEQUENCE public.i_seq
						START WITH 1
						INCREMENT BY 1
						NO MINVALUE
						NO MAXVALUE
						CACHE 1;
					ALTER SEQUENCE public.i_seq OWNED BY public.i.id;
					ALTER TABLE ONLY t ALTER COLUMN a SET DEFAULT nextval('public.i_seq'::regclass);
					SELECT pg_catalog.setval('public.i_seq', 10, true);
				`,
			query: map[string][][]string{
				`SELECT nextval('i_seq')`:    {{"11"}},
				`SHOW CREATE SEQUENCE i_seq`: {{"i_seq", "CREATE SEQUENCE i_seq MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1"}},
			},
		},
		{
			name: "non-public schema",
			typ:  "PGDUMP",
			data: "create table s.t (i INT8)",
			err:  `non-public schemas unsupported: s`,
		},
		{
			name: "unsupported type",
			typ:  "PGDUMP",
			data: "create table t (t time with time zone)",
			err: `create table t \(t time with time zone\)
                                 \^`,
		},
		{
			name: "various create ignores",
			typ:  "PGDUMP",
			data: `
				CREATE TRIGGER conditions_set_updated_at BEFORE UPDATE ON conditions FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
				REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM PUBLIC;
				REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM database;
				GRANT ALL ON SEQUENCE knex_migrations_id_seq TO database;
				GRANT SELECT ON SEQUENCE knex_migrations_id_seq TO opentrials_readonly;

				CREATE FUNCTION public.isnumeric(text) RETURNS boolean
				    LANGUAGE sql
				    AS $_$
				SELECT $1 ~ '^[0-9]+$'
				$_$;
				ALTER FUNCTION public.isnumeric(text) OWNER TO roland;

				CREATE TABLE t (i INT8);
			`,
			query: map[string][][]string{
				`SHOW TABLES`: {{"public", "t", "table"}},
			},
		},
		{
			name: "many tables",
			typ:  "PGDUMP",
			data: func() string {
				var sb strings.Builder
				for i := 1; i <= 100; i++ {
					fmt.Fprintf(&sb, "CREATE TABLE t%d ();\n", i)
				}
				return sb.String()
			}(),
		},

		// Error
		{
			name:   "unsupported import format",
			create: `b bytes`,
			typ:    "NOPE",
			err:    `unsupported import format`,
		},
		{
			name:   "sequences",
			create: `i int8 default nextval('s')`,
			typ:    "CSV",
			err:    `"s" not found`,
		},
	}

	var mockRecorder struct {
		syncutil.Mutex
		dataString, rejectedString string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mockRecorder.Lock()
		defer mockRecorder.Unlock()
		if r.Method == "GET" {
			fmt.Fprint(w, mockRecorder.dataString)
		}
		if r.Method == "PUT" {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			mockRecorder.rejectedString = string(body)
		}
	}))
	defer srv.Close()

	// Create and drop a table to make sure a descriptor ID gets used to verify
	// ID rewrites happen correctly. Useful when running just a single test.
	sqlDB.Exec(t, `CREATE TABLE blah (i int8)`)
	sqlDB.Exec(t, `DROP TABLE blah`)

	for _, saveRejected := range []bool{false, true} {
		// this test is big and slow as is, so we can't afford to double it in race.
		if util.RaceEnabled && saveRejected {
			continue
		}

		for i, tc := range tests {
			if tc.typ != "CSV" && tc.typ != "DELIMITED" && saveRejected {
				continue
			}
			if saveRejected {
				if tc.with == "" {
					tc.with = "WITH experimental_save_rejected"
				} else {
					tc.with += ", experimental_save_rejected"
				}
			}
			t.Run(fmt.Sprintf("%s/%s: save_rejected=%v", tc.typ, tc.name, saveRejected), func(t *testing.T) {
				dbName := fmt.Sprintf("d%d", i)
				sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s; USE %[1]s`, dbName))
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP DATABASE %s`, dbName))
				var q string
				if tc.create != "" {
					q = fmt.Sprintf(`IMPORT TABLE t (%s) %s DATA ($1) %s`, tc.create, tc.typ, tc.with)
				} else {
					q = fmt.Sprintf(`IMPORT %s ($1) %s`, tc.typ, tc.with)
				}
				t.Log(q, srv.URL, "\nFile contents:\n", tc.data)
				mockRecorder.dataString = tc.data
				mockRecorder.rejectedString = ""
				if !saveRejected || tc.rejected == "" {
					sqlDB.ExpectErr(t, tc.err, q, srv.URL)
				} else {
					sqlDB.Exec(t, q, srv.URL)
				}
				if tc.err == "" || saveRejected {
					for query, res := range tc.query {
						sqlDB.CheckQueryResults(t, query, res)
					}
					if tc.rejected != mockRecorder.rejectedString {
						t.Errorf("expected:\n%q\ngot:\n%q\n", tc.rejected,
							mockRecorder.rejectedString)
					}
				}
			})
		}
	}

	t.Run("mysqlout multiple", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE mysqlout; USE mysqlout`)
		mockRecorder.dataString = "1"
		sqlDB.Exec(t, `IMPORT TABLE t (s STRING) DELIMITED DATA ($1, $1)`, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1"}, {"1"}})
	})
}

func TestImportUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	// Set up some initial state for the tests.
	sqlDB.Exec(t, `
SET experimental_enable_enums = true;
CREATE TYPE greeting AS ENUM ('hello', 'hi');
`)

	// Create some AVRO encoded data.
	var avroData string
	{
		var data bytes.Buffer
		// Set up a simple schema for the import data.
		schema := map[string]interface{}{
			"type": "record",
			"name": "t",
			"fields": []map[string]interface{}{
				{
					"name": "a",
					"type": "string",
				},
				{
					"name": "b",
					"type": "string",
				},
			},
		}
		schemaStr, err := json.Marshal(schema)
		require.NoError(t, err)
		codec, err := goavro.NewCodec(string(schemaStr))
		require.NoError(t, err)
		// Create an AVRO writer from the schema.
		ocf, err := goavro.NewOCFWriter(goavro.OCFConfig{
			W:     &data,
			Codec: codec,
		})
		require.NoError(t, err)
		row1 := map[string]interface{}{
			"a": "hello",
			"b": "hello",
		}
		row2 := map[string]interface{}{
			"a": "hi",
			"b": "hi",
		}
		// Add the data rows to the writer.
		require.NoError(t, ocf.Append([]interface{}{row1, row2}))
		// Retrieve the AVRO encoded data.
		avroData = data.String()
	}

	tests := []struct {
		create      string
		typ         string
		contents    string
		intoCols    string
		verifyQuery string
		expected    [][]string
	}{
		// Test CSV imports.
		{
			create:      "a greeting, b greeting",
			intoCols:    "a, b",
			typ:         "CSV",
			contents:    "hello,hello\nhi,hi\n",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello"}, {"hi", "hi"}},
		},
		// Test PGDump imports.
		{
			create:      "a greeting, b greeting",
			intoCols:    "a, b",
			typ:         "PGDUMP",
			contents:    `INSERT INTO t VALUES ('hello', 'hello'), ('hi', 'hi')`,
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello"}, {"hi", "hi"}},
		},
		// Test MySQL imports.
		{
			create:      "a greeting, b greeting",
			intoCols:    "a, b",
			typ:         "MYSQLDUMP",
			contents:    "INSERT INTO `t` VALUES ('hello', 'hello'), ('hi', 'hi')",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello"}, {"hi", "hi"}},
		},
		// Test AVRO imports.
		{
			create:      "a greeting, b greeting",
			intoCols:    "a, b",
			typ:         "AVRO",
			contents:    avroData,
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello"}, {"hi", "hi"}},
		},
	}

	// Set up a directory for the data files.
	err := os.Mkdir(filepath.Join(baseDir, "test"), 0777)
	require.NoError(t, err)
	// Test IMPORT and IMPORT INTO.
	for _, into := range []bool{true, false} {
		for _, test := range tests {
			// Write the test data into a file.
			err := ioutil.WriteFile(filepath.Join(baseDir, "test", "data"), []byte(test.contents), 0666)
			require.NoError(t, err)
			// Run the import statement.
			if into {
				sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE t (%s)", test.create))
				sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO t (%s) %s DATA ($1)", test.intoCols, test.typ), "nodelocal://0/test/data")
			} else {
				sqlDB.Exec(t, fmt.Sprintf("IMPORT TABLE t (%s) %s DATA ($1)", test.create, test.typ), "nodelocal://0/test/data")
			}
			// Ensure that the table data is as we expect.
			sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)
			// Clean up after the test.
			sqlDB.Exec(t, "DROP TABLE t")
		}
	}
}

const (
	testPgdumpCreateCities = `CREATE TABLE cities (
	city VARCHAR(80) NOT NULL,
	CONSTRAINT cities_pkey PRIMARY KEY (city ASC),
	FAMILY "primary" (city)
)`
	testPgdumpCreateWeather = `CREATE TABLE weather (
	city VARCHAR(80) NULL,
	temp_lo INT8 NULL,
	temp_hi INT8 NULL,
	prcp FLOAT4 NULL,
	date DATE NULL,
	CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES cities(city),
	INDEX weather_auto_index_weather_city_fkey (city ASC),
	FAMILY "primary" (city, temp_lo, temp_hi, prcp, date, rowid)
)`
	testPgdumpFk = `
CREATE TABLE public.cities (
    city character varying(80) NOT NULL
);

ALTER TABLE public.cities OWNER TO postgres;

CREATE TABLE public.weather (
    city character varying(80),
    temp_lo int8,
    temp_hi int8,
    prcp real,
    date date
);

ALTER TABLE public.weather OWNER TO postgres;

COPY public.cities (city) FROM stdin;
Berkeley
\.

COPY public.weather (city, temp_lo, temp_hi, prcp, date) FROM stdin;
Berkeley	45	53	0	1994-11-28
\.

ALTER TABLE ONLY public.cities
    ADD CONSTRAINT cities_pkey PRIMARY KEY (city);

ALTER TABLE ONLY public.weather
    ADD CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES public.cities(city);
`

	testPgdumpFkCircular = `
CREATE TABLE public.a (
    i int8 NOT NULL,
    k int8
);

CREATE TABLE public.b (
    j int8 NOT NULL
);

COPY public.a (i, k) FROM stdin;
2	2
\.

COPY public.b (j) FROM stdin;
2
\.

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_pkey PRIMARY KEY (i);

ALTER TABLE ONLY public.b
    ADD CONSTRAINT b_pkey PRIMARY KEY (j);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_i_fkey FOREIGN KEY (i) REFERENCES public.b(j);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_k_fkey FOREIGN KEY (k) REFERENCES public.a(i);

ALTER TABLE ONLY public.b
    ADD CONSTRAINT b_j_fkey FOREIGN KEY (j) REFERENCES public.a(i);
`
)

func TestImportCSVStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short")
	}

	const nodes = 3

	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	var forceFailure bool
	blockGC := make(chan struct{})

	ctx := context.Background()
	baseDir := filepath.Join("testdata", "csv")
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		SQLMemoryPoolSize: 256 << 20,
		ExternalIODir:     baseDir,
		Knobs: base.TestingKnobs{
			GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ int64) error { <-blockGC; return nil }},
		},
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]

	for i := range tc.Servers {
		tc.Servers[i].JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*importResumer)
				r.testingKnobs.afterImport = func(_ backupccl.RowCount) error {
					if forceFailure {
						return errors.New("testing injected failure")
					}
					return nil
				}
				return r
			},
		}
	}

	sqlDB := sqlutils.MakeSQLRunner(conn)
	kvDB := tc.Server(0).DB()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)

	testFiles := makeCSVData(t, numFiles, rowsPerFile, nodes, rowsPerRaceFile)
	if util.RaceEnabled {
		// This test takes a while with the race detector, so reduce the number of
		// files and rows per file in an attempt to speed it up.
		numFiles = nodes
		rowsPerFile = rowsPerRaceFile
	}

	// Table schema used in IMPORT TABLE tests.
	tablePath := filepath.Join(baseDir, "table")
	if err := ioutil.WriteFile(tablePath, []byte(`
		CREATE TABLE t (
			a int8 primary key,
			b string,
			index (b),
			index (a, b)
		)
	`), 0666); err != nil {
		t.Fatal(err)
	}
	schema := []interface{}{"nodelocal://0/table"}

	if err := ioutil.WriteFile(filepath.Join(baseDir, "empty.csv"), nil, 0666); err != nil {
		t.Fatal(err)
	}
	empty := []string{"'nodelocal://0/empty.csv'"}
	emptySchema := []interface{}{"nodelocal://0/empty.schema"}

	// Support subtests by keeping track of the number of jobs that are executed.
	testNum := -1
	expectedRows := numFiles * rowsPerFile
	for i, tc := range []struct {
		name    string
		query   string        // must have one `%s` for the files list.
		args    []interface{} // will have backupPath appended
		files   []string
		jobOpts string
		err     string
	}{
		{
			"schema-in-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.files,
			``,
			"",
		},
		{
			"schema-in-file-intodb",
			`IMPORT TABLE csv1.t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.files,
			``,
			"",
		},
		{
			"schema-in-query",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s)`,
			nil,
			testFiles.files,
			``,
			"",
		},
		{
			"schema-in-query-opts",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2'`,
			nil,
			testFiles.filesWithOpts,
			` WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2'`,
			"",
		},
		{
			// Force some SST splits.
			"schema-in-file-sstsize",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH sstsize = '10K'`,
			schema,
			testFiles.files,
			` WITH sstsize = '10K'`,
			"",
		},
		{
			"empty-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			empty,
			``,
			"",
		},
		{
			"empty-with-files",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			append(empty, testFiles.files...),
			``,
			"",
		},
		{
			"schema-in-file-auto-decompress",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'auto'`,
			schema,
			testFiles.files,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"schema-in-file-no-decompress",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'none'`,
			schema,
			testFiles.files,
			` WITH decompress = 'none'`,
			"",
		},
		{
			"schema-in-file-explicit-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'gzip'`,
			schema,
			testFiles.gzipFiles,
			` WITH decompress = 'gzip'`,
			"",
		},
		{
			"schema-in-file-auto-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'auto'`,
			schema,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"schema-in-file-implicit-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.gzipFiles,
			``,
			"",
		},
		{
			"schema-in-file-explicit-bzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'bzip'`,
			schema,
			testFiles.bzipFiles,
			` WITH decompress = 'bzip'`,
			"",
		},
		{
			"schema-in-file-auto-bzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'auto'`,
			schema,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"schema-in-file-implicit-bzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.bzipFiles,
			``,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"bad-opt-name",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH foo = 'bar'`,
			nil,
			testFiles.files,
			``,
			"invalid option \"foo\"",
		},
		{
			"bad-computed-column",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING AS ('hello') STORED, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH skip = '2'`,
			nil,
			testFiles.filesWithOpts,
			``,
			"computed columns not supported",
		},
		{
			"primary-key-dup",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.filesWithDups,
			``,
			"duplicate key in primary index",
		},
		{
			"no-database",
			`IMPORT TABLE nonexistent.t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.files,
			``,
			`database does not exist: "nonexistent.t"`,
		},
		{
			"into-db-fails",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH into_db = 'test'`,
			schema,
			testFiles.files,
			``,
			`invalid option "into_db"`,
		},
		{
			"schema-in-file-no-decompress-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'none'`,
			schema,
			testFiles.gzipFiles,
			` WITH decompress = 'none'`,
			// This returns different errors for `make test` and `make testrace` but
			// field is in both error messages.
			`field`,
		},
		{
			"schema-in-file-decompress-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'gzip'`,
			schema,
			testFiles.files,
			` WITH decompress = 'gzip'`,
			"gzip: invalid header",
		},
		{
			"csv-with-invalid-delimited-option",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH fields_delimited_by = '|'`,
			schema,
			testFiles.files,
			``,
			"invalid option",
		},
		{
			"empty-schema-in-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			emptySchema,
			testFiles.files,
			``,
			"expected 1 create table statement",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.name, "bzip") && len(testFiles.bzipFiles) == 0 {
				t.Skip("bzip2 not available on PATH?")
			}
			intodb := fmt.Sprintf(`csv%d`, i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, intodb))
			sqlDB.Exec(t, fmt.Sprintf(`SET DATABASE = %s`, intodb))

			var unused string
			var restored struct {
				rows, idx, bytes int
			}

			var result int
			query := fmt.Sprintf(tc.query, strings.Join(tc.files, ", "))
			testNum++
			if tc.err != "" {
				sqlDB.ExpectErr(t, tc.err, query, tc.args...)
				return
			}
			sqlDB.QueryRow(t, query, tc.args...).Scan(
				&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.bytes,
			)

			jobPrefix := fmt.Sprintf(`IMPORT TABLE %s.public.t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))`, intodb)

			if err := jobutils.VerifySystemJob(t, sqlDB, testNum, jobspb.TypeImport, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(jobPrefix+` CSV DATA (%s)`+tc.jobOpts, strings.ReplaceAll(strings.Join(tc.files, ", "), "?AWS_SESSION_TOKEN=secrets", "?AWS_SESSION_TOKEN=redacted")),
			}); err != nil {
				t.Fatal(err)
			}

			isEmpty := len(tc.files) == 1 && tc.files[0] == empty[0]

			if isEmpty {
				sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
				if expect := 0; result != expect {
					t.Fatalf("expected %d rows, got %d", expect, result)
				}
				return
			}

			if expected, actual := expectedRows, restored.rows; expected != actual {
				t.Fatalf("expected %d rows, got %d", expected, actual)
			}

			// Verify correct number of rows via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
			if expect := expectedRows; result != expect {
				t.Fatalf("expected %d rows, got %d", expect, result)
			}

			// Verify correct number of NULLs via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
			expectedNulls := 0
			if strings.Contains(tc.query, "nullif") {
				expectedNulls = expectedRows / 4
			}
			if result != expectedNulls {
				t.Fatalf("expected %d rows, got %d", expectedNulls, result)
			}

			// Verify sstsize created > 1 SST files.
			if tc.name == "schema-in-file-sstsize-dist" {
				pattern := filepath.Join(baseDir, fmt.Sprintf("%d", i), "*.sst")
				matches, err := filepath.Glob(pattern)
				if err != nil {
					t.Fatal(err)
				}
				if len(matches) < 2 {
					t.Fatal("expected > 1 SST files")
				}
			}

		})
	}

	// Verify unique_rowid is replaced for tables without primary keys.
	t.Run("unique_rowid", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE pk")
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE pk.t (a INT8, b STRING) CSV DATA (%s)`, strings.Join(testFiles.files, ", ")))
		// Verify the rowids are being generated as expected.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) FROM pk.t`,
			sqlDB.QueryStr(t, `
				SELECT count(*) FROM
					(SELECT * FROM
						(SELECT generate_series(0, $1 - 1) file),
						(SELECT generate_series(1, $2) rownum)
					)
			`, numFiles, rowsPerFile),
		)
	})

	// Verify a failed IMPORT won't prevent a second IMPORT.
	t.Run("checkpoint-leftover", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE checkpoint; USE checkpoint")

		// Specify wrong number of columns.
		sqlDB.ExpectErr(
			t, "expected 1 fields, got 2",
			fmt.Sprintf(`IMPORT TABLE t (a INT8 PRIMARY KEY) CSV DATA (%s)`, testFiles.files[0]),
		)

		// Specify wrong table name; still shouldn't leave behind a checkpoint file.
		sqlDB.ExpectErr(
			t, `file specifies a schema for table t`,
			fmt.Sprintf(`IMPORT TABLE bad CREATE USING $1 CSV DATA (%s)`, testFiles.files[0]), schema[0],
		)

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING) CSV DATA (%s)`, testFiles.files[0]))

		// A second attempt should fail fast. A "slow fail" is the error message
		// "restoring table desc and namespace entries: table already exists".
		sqlDB.ExpectErr(
			t, `relation "t" already exists`,
			fmt.Sprintf(`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING) CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// Verify that a failed import will clean up after itself. This means:
	//  - Delete the garbage data that it partially imported.
	//  - Delete the table descriptor for the table that was created during the
	//  import.
	t.Run("failed-import-gc", func(t *testing.T) {
		forceFailure = true
		defer func() { forceFailure = false }()
		defer gcjob.SetSmallMaxGCIntervalForTest()()
		beforeImport, err := tree.MakeDTimestampTZ(tc.Server(0).Clock().Now().GoTime(), time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		sqlDB.Exec(t, "CREATE DATABASE failedimport; USE failedimport;")
		// Hit a failure during import.
		sqlDB.ExpectErr(
			t, `testing injected failure`,
			fmt.Sprintf(`IMPORT TABLE t (a INT PRIMARY KEY, b STRING) CSV DATA (%s)`, testFiles.files[1]),
		)
		// Nudge the registry to quickly adopt the job.
		tc.Server(0).JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

		// In the case of the test, the ID of the table that will be cleaned up due
		// to the failed import will be one higher than the ID of the empty database
		// it was created in.
		dbID := sqlutils.QueryDatabaseID(t, sqlDB.DB, "failedimport")
		tableID := sqlbase.ID(dbID + 1)
		var td *sqlbase.TableDescriptor
		if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			td, err = sqlbase.GetTableDescFromID(ctx, txn, keys.SystemSQLCodec, tableID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		// Ensure that we have garbage written to the descriptor that we want to
		// clean up.
		tests.CheckKeyCount(t, kvDB, td.TableSpan(keys.SystemSQLCodec), rowsPerFile)

		// Allow GC to progress.
		close(blockGC)
		// Ensure that a GC job was created, and wait for it to finish.
		doneGCQuery := fmt.Sprintf(
			"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '%s' AND status = '%s' AND created > %s",
			"SCHEMA CHANGE GC", jobs.StatusSucceeded, beforeImport.String(),
		)
		sqlDB.CheckQueryResultsRetry(t, doneGCQuery, [][]string{{"1"}})
		// Expect there are no more KVs for this span.
		tests.CheckKeyCount(t, kvDB, td.TableSpan(keys.SystemSQLCodec), 0)
		// Expect that the table descriptor is deleted.
		if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := sqlbase.GetTableDescFromID(ctx, txn, keys.SystemSQLCodec, tableID)
			if !testutils.IsError(err, "descriptor not found") {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	// Test basic role based access control. Users who have the admin role should
	// be able to IMPORT.
	t.Run("RBAC", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE USER testuser`)
		sqlDB.Exec(t, `GRANT admin TO testuser`)
		pgURL, cleanupFunc := sqlutils.PGUrl(
			t, tc.Server(0).ServingSQLAddr(), "TestImportPrivileges-testuser", url.User("testuser"),
		)
		defer cleanupFunc()
		testuser, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer testuser.Close()

		t.Run("IMPORT TABLE", func(t *testing.T) {
			if _, err := testuser.Exec(fmt.Sprintf(`IMPORT TABLE rbac_table_t (a INT8 PRIMARY KEY, b STRING) CSV DATA (%s)`, testFiles.files[0])); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("IMPORT INTO", func(t *testing.T) {
			if _, err := testuser.Exec("CREATE TABLE rbac_into_t (a INT8 PRIMARY KEY, b STRING)"); err != nil {
				t.Fatal(err)
			}
			if _, err := testuser.Exec(fmt.Sprintf(`IMPORT INTO rbac_into_t (a, b) CSV DATA (%s)`, testFiles.files[0])); err != nil {
				t.Fatal(err)
			}
		})
	})

	// Verify DEFAULT columns and SERIAL are allowed but not evaluated.
	t.Run("allow-default", func(t *testing.T) {
		var data string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, `CREATE DATABASE d`)
		sqlDB.Exec(t, `SET DATABASE = d`)

		const (
			query = `IMPORT TABLE t (
			a SERIAL8,
			b INT8 DEFAULT unique_rowid(),
			c STRING DEFAULT 's',
			d SERIAL8,
			e INT8 DEFAULT unique_rowid(),
			f STRING DEFAULT 's',
			PRIMARY KEY (a, b, c)
		) CSV DATA ($1)`
			nullif = ` WITH nullif=''`
		)

		data = ",5,e,7,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `row 1: parse "a" as INT8: could not parse ""`,
				query, srv.URL,
			)
			sqlDB.ExpectErr(
				t, `row 1: generate insert row: null value in column "a" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})
		data = "2,5,e,,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `row 1: generate insert row: null value in column "d" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})
		data = "2,,e,,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `"b" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})

		data = "2,5,,,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `"c" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})

		data = "2,5,e,-1,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, query+nullif, srv.URL)
			sqlDB.CheckQueryResults(t,
				`SELECT * FROM t`,
				sqlDB.QueryStr(t, `SELECT 2, 5, 'e', -1, NULL, NULL`),
			)
		})
	})
}

func TestExportImportRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	tests := []struct {
		stmts    string
		tbl      string
		expected string
	}{
		// Note that the directory names that are being imported from and exported into
		// need to differ across runs, so we let the test runner format the stmts field
		// with a unique directory name per run.
		{
			stmts: `EXPORT INTO CSV 'nodelocal://0/%[1]s' FROM SELECT ARRAY['a', 'b', 'c'];
							IMPORT TABLE t (x TEXT[]) CSV DATA ('nodelocal://0/%[1]s/n1.0.csv')`,
			tbl:      "t",
			expected: `SELECT ARRAY['a', 'b', 'c']`,
		},
		{
			stmts: `EXPORT INTO CSV 'nodelocal://0/%[1]s' FROM SELECT ARRAY[b'abc', b'\141\142\143', b'\x61\x62\x63'];
							IMPORT TABLE t (x BYTES[]) CSV DATA ('nodelocal://0/%[1]s/n1.0.csv')`,
			tbl:      "t",
			expected: `SELECT ARRAY[b'abc', b'\141\142\143', b'\x61\x62\x63']`,
		},
		{
			stmts: `EXPORT INTO CSV 'nodelocal://0/%[1]s' FROM SELECT 'dog' COLLATE en;
							IMPORT TABLE t (x STRING COLLATE en) CSV DATA ('nodelocal://0/%[1]s/n1.0.csv')`,
			tbl:      "t",
			expected: `SELECT 'dog' COLLATE en`,
		},
	}

	for i, test := range tests {
		sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, test.tbl))
		sqlDB.Exec(t, fmt.Sprintf(test.stmts, fmt.Sprintf("run%d", i)))
		sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %s`, test.tbl), sqlDB.QueryStr(t, test.expected))
	}
}

// TODO(adityamaru): Tests still need to be added incrementally as
// relevant IMPORT INTO logic is added. Some of them include:
// -> FK and constraint violation
// -> CSV containing keys which will shadow existing data
// -> Rollback of a failed IMPORT INTO
func TestImportIntoCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short")
	}

	const nodes = 3

	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	ctx := context.Background()
	baseDir := filepath.Join("testdata", "csv")
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]

	var forceFailure bool
	var importBodyFinished chan struct{}
	var delayImportFinish chan struct{}

	for i := range tc.Servers {
		tc.Servers[i].JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*importResumer)
				r.testingKnobs.afterImport = func(_ backupccl.RowCount) error {
					if importBodyFinished != nil {
						importBodyFinished <- struct{}{}
					}
					if delayImportFinish != nil {
						<-delayImportFinish
					}

					if forceFailure {
						return errors.New("testing injected failure")
					}
					return nil
				}
				return r
			},
		}
	}

	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)

	testFiles := makeCSVData(t, numFiles, rowsPerFile, nodes, rowsPerRaceFile)
	if util.RaceEnabled {
		// This test takes a while with the race detector, so reduce the number of
		// files and rows per file in an attempt to speed it up.
		numFiles = nodes
		rowsPerFile = rowsPerRaceFile
	}

	if err := ioutil.WriteFile(filepath.Join(baseDir, "empty.csv"), nil, 0666); err != nil {
		t.Fatal(err)
	}
	empty := []string{"'nodelocal://0/empty.csv'"}

	// Support subtests by keeping track of the number of jobs that are executed.
	testNum := -1
	insertedRows := numFiles * rowsPerFile

	for _, tc := range []struct {
		name    string
		query   string // must have one `%s` for the files list.
		files   []string
		jobOpts string
		err     string
	}{
		{
			"simple-import-into",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			"",
		},
		{
			"import-into-with-opts",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2'`,
			testFiles.filesWithOpts,
			` WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2'`,
			"",
		},
		{
			// Force some SST splits.
			"import-into-sstsize",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH sstsize = '10K'`,
			testFiles.files,
			` WITH sstsize = '10K'`,
			"",
		},
		{
			"empty-file",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			empty,
			``,
			"",
		},
		{
			"empty-with-files",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			append(empty, testFiles.files...),
			``,
			"",
		},
		{
			"import-into-auto-decompress",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.files,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"import-into-no-decompress",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.files,
			` WITH decompress = 'none'`,
			"",
		},
		{
			"import-into-explicit-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.gzipFiles,
			` WITH decompress = 'gzip'`,
			"",
		},
		{
			"import-into-auto-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.gzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"import-into-implicit-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			testFiles.gzipFiles,
			``,
			"",
		},
		{
			"import-into-explicit-bzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'bzip'`,
			testFiles.bzipFiles,
			` WITH decompress = 'bzip'`,
			"",
		},
		{
			"import-into-auto-bzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"import-into-implicit-bzip",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			testFiles.bzipFiles,
			``,
			"",
		},
		{
			"import-into-no-decompress-wildcard",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.filesUsingWildcard,
			` WITH decompress = 'none'`,
			"",
		},
		{
			"import-into-explicit-gzip-wildcard",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.gzipFilesUsingWildcard,
			` WITH decompress = 'gzip'`,
			"",
		},
		{
			"import-into-auto-bzip-wildcard",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.gzipFilesUsingWildcard,
			` WITH decompress = 'auto'`,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"import-into-bad-opt-name",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH foo = 'bar'`,
			testFiles.files,
			``,
			"invalid option \"foo\"",
		},
		{
			"import-into-no-database",
			`IMPORT INTO nonexistent.t (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			`database does not exist: "nonexistent.t"`,
		},
		{
			"import-into-no-table",
			`IMPORT INTO g (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			`pq: relation "g" does not exist`,
		},
		{
			"import-into-no-decompress-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.gzipFiles,
			` WITH decompress = 'none'`,
			// This returns different errors for `make test` and `make testrace` but
			// field is in both error messages.
			"field",
		},
		{
			"import-into-no-decompress-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.files,
			` WITH decompress = 'gzip'`,
			"gzip: invalid header",
		},
		{
			"import-no-files-match-wildcard",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			[]string{`'nodelocal://0/data-[0-9][0-9]*'`},
			` WITH decompress = 'auto'`,
			`pq: no files matched uri provided`,
		},
		{
			"import-into-no-glob-wildcard",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH disable_glob_matching`,
			testFiles.filesUsingWildcard,
			` WITH disable_glob_matching`,
			"pq: (.+) no such file or directory",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.name, "bzip") && len(testFiles.bzipFiles) == 0 {
				t.Skip("bzip2 not available on PATH?")
			}
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)

			var unused string
			var restored struct {
				rows, idx, bytes int
			}

			// Insert the test data
			insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}
			numExistingRows := len(insert)

			for i, v := range insert {
				sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i, v)
			}

			var result int
			query := fmt.Sprintf(tc.query, strings.Join(tc.files, ", "))
			testNum++
			if tc.err != "" {
				sqlDB.ExpectErr(t, tc.err, query)
				return
			}

			sqlDB.QueryRow(t, query).Scan(
				&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.bytes,
			)

			jobPrefix := fmt.Sprintf(`IMPORT INTO defaultdb.public.t(a, b)`)
			if err := jobutils.VerifySystemJob(t, sqlDB, testNum, jobspb.TypeImport, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(jobPrefix+` CSV DATA (%s)`+tc.jobOpts, strings.ReplaceAll(strings.Join(tc.files, ", "), "?AWS_SESSION_TOKEN=secrets", "?AWS_SESSION_TOKEN=redacted")),
			}); err != nil {
				t.Fatal(err)
			}

			isEmpty := len(tc.files) == 1 && tc.files[0] == empty[0]
			if isEmpty {
				sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
				if result != numExistingRows {
					t.Fatalf("expected %d rows, got %d", numExistingRows, result)
				}
				return
			}

			if expected, actual := insertedRows, restored.rows; expected != actual {
				t.Fatalf("expected %d rows, got %d", expected, actual)
			}

			// Verify correct number of rows via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
			if expect := numExistingRows + insertedRows; result != expect {
				t.Fatalf("expected %d rows, got %d", expect, result)
			}

			// Verify correct number of NULLs via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
			expectedNulls := 0
			if strings.Contains(tc.query, "nullif") {
				expectedNulls = insertedRows / 4
			}
			if result != expectedNulls {
				t.Fatalf("expected %d rows, got %d", expectedNulls, result)
			}
		})
	}

	// Verify unique_rowid is replaced for tables without primary keys.
	t.Run("import-into-unique_rowid", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}
		numExistingRows := len(insert)

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i, v)
		}

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, strings.Join(testFiles.files, ", ")))
		// Verify the rowids are being generated as expected.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) FROM t`,
			sqlDB.QueryStr(t, `
			SELECT count(*) + $3 FROM
			(SELECT * FROM
				(SELECT generate_series(0, $1 - 1) file),
				(SELECT generate_series(1, $2) rownum)
			)
			`, numFiles, rowsPerFile, numExistingRows),
		)
	})

	// Verify a failed IMPORT INTO won't prevent a subsequent IMPORT INTO.
	t.Run("import-into-checkpoint-leftover", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i, v)
		}

		// Hit a failure during import.
		forceFailure = true
		sqlDB.ExpectErr(
			t, `testing injected failure`,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[1]),
		)
		forceFailure = false

		// Expect it to succeed on re-attempt.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[1]))
	})

	// Verify that during IMPORT INTO the table is offline.
	t.Run("offline-state", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i, v)
		}

		// Hit a failure during import.
		importBodyFinished = make(chan struct{})
		delayImportFinish = make(chan struct{})
		defer func() {
			importBodyFinished = nil
			delayImportFinish = nil
		}()

		var unused interface{}

		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			defer close(importBodyFinished)
			_, err := sqlDB.DB.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[1]))
			return err
		})
		g.GoCtx(func(ctx context.Context) error {
			defer close(delayImportFinish)
			<-importBodyFinished

			err := sqlDB.DB.QueryRowContext(ctx, `SELECT 1 FROM t`).Scan(&unused)
			if !testutils.IsError(err, "relation \"t\" does not exist") {
				return err
			}
			return nil
		})
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		t.Skip()

		// Expect it to succeed on re-attempt.
		sqlDB.QueryRow(t, `SELECT 1 FROM t`).Scan(&unused)
	})

	// Tests for user specified target columns in IMPORT INTO statements.
	//
	// Tests IMPORT INTO with various target column sets, and an implicit PK
	// provided by the hidden column row_id.
	t.Run("target-cols-with-default-pk", func(t *testing.T) {
		var data string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		createQuery := `CREATE TABLE t (a INT8,
			b INT8,
			c STRING,
			d INT8,
			e INT8,
			f STRING)`

		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, createQuery)
			defer sqlDB.Exec(t, `DROP TABLE t`)

			data = "1"
			sqlDB.Exec(t, `IMPORT INTO t (a) CSV DATA ($1)`, srv.URL)
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`,
				sqlDB.QueryStr(t, `SELECT 1, NULL, NULL, NULL, NULL, 'NULL'`),
			)
		})
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, createQuery)
			defer sqlDB.Exec(t, `DROP TABLE t`)

			data = "1,teststr"
			sqlDB.Exec(t, `IMPORT INTO t (a, f) CSV DATA ($1)`, srv.URL)
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`,
				sqlDB.QueryStr(t, `SELECT 1, NULL, NULL, NULL, NULL, 'teststr'`),
			)
		})
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, createQuery)
			defer sqlDB.Exec(t, `DROP TABLE t`)

			data = "7,12,teststr"
			sqlDB.Exec(t, `IMPORT INTO t (d, e, f) CSV DATA ($1)`, srv.URL)
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`,
				sqlDB.QueryStr(t, `SELECT NULL, NULL, NULL, 7, 12, 'teststr'`),
			)
		})
	})

	// Tests IMPORT INTO with a target column set, and an explicit PK.
	t.Run("target-cols-with-explicit-pk", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i+1000, v)
		}

		data := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(strings.Join(data, "\n")))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, "IMPORT INTO t (a) CSV DATA ($1)", srv.URL)

		var result int
		numExistingRows := len(insert)
		// Verify that the target column has been populated.
		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE a IS NOT NULL`).Scan(&result)
		if expect := numExistingRows + len(data); result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}

		// Verify that the non-target columns have NULLs.
		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
		expectedNulls := len(data)
		if result != expectedNulls {
			t.Fatalf("expected %d rows, got %d", expectedNulls, result)
		}
	})

	// Tests IMPORT INTO with a CSV file having more columns when targeted, expected to
	// get an error indicating the error.
	t.Run("csv-with-more-than-targeted-columns", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Expect an error if attempting to IMPORT INTO with CSV having more columns
		// than targeted.
		sqlDB.ExpectErr(
			t, `row 1: expected 1 fields, got 2`,
			fmt.Sprintf("IMPORT INTO t (a) CSV DATA (%s)", testFiles.files[0]),
		)
	})

	// Tests IMPORT INTO with a target column set which does not include all PKs.
	// As a result the non-target column is non-nullable, which is not allowed
	// until we support DEFAULT expressions.
	t.Run("target-cols-excluding-explicit-pk", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Expect an error if attempting to IMPORT INTO a target list which does
		// not include all the PKs of the table.
		sqlDB.ExpectErr(
			t, `pq: all non-target columns in IMPORT INTO must be nullable`,
			fmt.Sprintf(`IMPORT INTO t (b) CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// Tests behavior when the existing table being imported into has more columns
	// in its schema then the source CSV file.
	t.Run("more-table-cols-than-csv", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING, c INT)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i, v)
		}

		sqlDB.ExpectErr(
			t, "row 1: expected 3 fields, got 2",
			fmt.Sprintf(`IMPORT INTO t (a, b, c) CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// Tests the case where we create table columns in specific order while trying
	// to import data from csv where columns order is different and import expression
	// defines in what order columns should be imported to align with table definition
	t.Run("target-cols-reordered", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE TABLE t (a INT PRIMARY KEY, b INT, c STRING NOT NULL, d DECIMAL NOT NULL)")
		defer sqlDB.Exec(t, `DROP TABLE t`)

		const data = "3.14,c is a string,1\n2.73,another string,2"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (d, c, a) CSV DATA ("%s")`, srv.URL))
		sqlDB.CheckQueryResults(t, `SELECT * FROM t ORDER BY a`,
			[][]string{{"1", "NULL", "c is a string", "3.14"}, {"2", "NULL", "another string", "2.73"}},
		)
	})

	// Tests that we can import into the table even if the table has columns named with
	// reserved keywords.
	t.Run("cols-named-with-reserved-keywords", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t ("select" INT PRIMARY KEY, "from" INT, "Some-c,ol-'Name'" STRING NOT NULL)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		const data = "today,1,2"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, fmt.Sprintf(
			`IMPORT INTO t ("Some-c,ol-'Name'", "select", "from") CSV DATA ("%s")`, srv.URL))
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1", "2", "today"}})
	})

	// Tests behvior when the existing table being imported into has fewer columns
	// in its schema then the source CSV file.
	t.Run("fewer-table-cols-than-csv", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		sqlDB.ExpectErr(
			t, "row 1: expected 1 fields, got 2",
			fmt.Sprintf(`IMPORT INTO t (a) CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// Tests IMPORT INTO without any target columns specified. This implies an
	// import of all columns in the exisiting table.
	t.Run("no-target-cols-specified", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i+rowsPerFile, v)
		}

		sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO t CSV DATA (%s)", testFiles.files[0]))

		var result int
		numExistingRows := len(insert)
		// Verify that all columns have been populated with imported data.
		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE a IS NOT NULL`).Scan(&result)
		if expect := numExistingRows + rowsPerFile; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}

		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NOT NULL`).Scan(&result)
		if expect := numExistingRows + rowsPerFile; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}
	})

	// Test that IMPORT INTO works when columns with default expressions are present.
	// The default expressions supported by IMPORT INTO are constant expressions,
	// which are literals and functions that always return the same value given the
	// same arguments (examples of non-constant expressions are given in the last two
	// subtests below). The default expression of a column is used when this column is not
	// targeted; otherwise, data from source file (like CSV) is used. It also checks
	// that IMPORT TABLE works when there are default columns.
	t.Run("import-into-default", func(t *testing.T) {
		var data string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()
		t.Run("is-not-target", func(t *testing.T) {
			data = "1\n2"
			sqlDB.Exec(t, `CREATE TABLE t (b INT DEFAULT 42, a INT)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"42", "1"}, {"42", "2"}})
		})
		t.Run("is-not-target-not-null", func(t *testing.T) {
			data = "1\n2"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b INT DEFAULT 42 NOT NULL)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1", "42"}, {"2", "42"}})
		})
		t.Run("is-target", func(t *testing.T) {
			data = "1,36\n2,37"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b INT DEFAULT 42)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1", "36"}, {"2", "37"}})
		})
		t.Run("is-target-with-null-data", func(t *testing.T) {
			data = ",36\n2,"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b INT DEFAULT 42)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA ("%s") WITH nullif = ''`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"NULL", "36"}, {"2", "NULL"}})
		})
		t.Run("mixed-target-and-non-target", func(t *testing.T) {
			data = "35,test string\n72,another test string"
			sqlDB.Exec(t, `CREATE TABLE t (b STRING, a INT DEFAULT 53, c INT DEFAULT 42)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"test string", "35", "42"}, {"another test string", "72", "42"}})
		})
		t.Run("with-import-table", func(t *testing.T) {
			data = "35,string1,65\n72,string2,17"
			sqlDB.Exec(t, fmt.Sprintf(
				`IMPORT TABLE t (a INT, b STRING, c INT DEFAULT 33)
			CSV DATA ("%s")`,
				srv.URL,
			))
			defer sqlDB.Exec(t, `DROP TABLE t`)
			data = "11,string3\n29,string4"
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
				{"35", "string1", "65"},
				{"72", "string2", "17"},
				{"11", "string3", "33"},
				{"29", "string4", "33"}})
		})
		t.Run("null-as-default", func(t *testing.T) {
			data = "1\n2\n3"
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (a INT DEFAULT NULL, b INT)`))
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (b) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"NULL", "1"}, {"NULL", "2"}, {"NULL", "3"}})
		})
		t.Run("default-value-change", func(t *testing.T) {
			data = "1\n2"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b INT DEFAULT 7)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			data = "3\n4"
			sqlDB.Exec(t, `ALTER TABLE t ALTER COLUMN b SET DEFAULT 8`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1", "7"}, {"2", "7"}, {"3", "8"}, {"4", "8"}})
		})
		t.Run("math-constant", func(t *testing.T) {
			data = "35\n67"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b FLOAT DEFAULT round(pi()))`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
				{"35", "3"},
				{"67", "3"}})
		})
		t.Run("string-function", func(t *testing.T) {
			data = "1\n2"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING DEFAULT repeat('dog', 2))`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
				{"1", "dogdog"},
				{"2", "dogdog"}})
		})
		t.Run("arithmetic", func(t *testing.T) {
			data = "35\n67"
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b INT DEFAULT 34 * 3)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
			sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{
				{"35", "102"},
				{"67", "102"}})
		})
		t.Run("sequence-impure", func(t *testing.T) {
			data = "1\n2"
			sqlDB.Exec(t, `CREATE SEQUENCE testseq`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.Exec(t, `CREATE TABLE t(a INT, b INT DEFAULT nextval('testseq'))`)
			sqlDB.ExpectErr(t,
				fmt.Sprintf(`non-constant default expression .* for non-targeted column "b" is not supported by IMPORT INTO`),
				fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
		})
		t.Run("now-impure", func(t *testing.T) {
			data = "1\n2"
			sqlDB.Exec(t, `CREATE TABLE t(a INT, b TIMESTAMP DEFAULT now())`)
			defer sqlDB.Exec(t, `DROP TABLE t`)
			sqlDB.ExpectErr(t,
				fmt.Sprintf(`non-constant default expression .* for non-targeted column "b" is not supported by IMPORT INTO`),
				fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
		})
	})

	t.Run("import-not-targeted-not-null", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b INT NOT NULL)`)
		const data = "1\n2\n3"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()
		defer sqlDB.Exec(t, `DROP TABLE t`)
		sqlDB.ExpectErr(t, `violated by column "b"`,
			fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL),
		)
	})

	// IMPORT INTO does not currently support import into interleaved tables.
	t.Run("import-into-rejects-interleaved-tables", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE parent (parent_id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE child (
				parent_id INT,
				child_id INT,
				PRIMARY KEY(parent_id, child_id))
				INTERLEAVE IN PARENT parent(parent_id)`)
		defer sqlDB.Exec(t, `DROP TABLE parent`)
		defer sqlDB.Exec(t, `DROP TABLE child`)

		// Cannot IMPORT INTO interleaved parent
		sqlDB.ExpectErr(
			t, "Cannot use IMPORT INTO with interleaved tables",
			fmt.Sprintf(`IMPORT INTO parent (parent_id) CSV DATA (%s)`, testFiles.files[0]))

		// Cannot IMPORT INTO interleaved child either.
		sqlDB.ExpectErr(
			t, "Cannot use IMPORT INTO with interleaved tables",
			fmt.Sprintf(`IMPORT INTO child (parent_id, child_id) CSV DATA (%s)`, testFiles.files[0]))
	})

	// This tests that consecutive imports from unique data sources into an
	// existing table without an explicit PK, do not overwrite each other. It
	// exercises the row_id generation in IMPORT.
	t.Run("multiple-import-into-without-pk", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}
		numExistingRows := len(insert)
		insertedRows := rowsPerFile * 3

		for i, v := range insert {
			sqlDB.Exec(t, "INSERT INTO t (a, b) VALUES ($1, $2)", i, v)
		}

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]))
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[1]))
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[2]))

		// Verify correct number of rows via COUNT.
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
		if expect := numExistingRows + insertedRows; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}
	})

	// This tests that a collision is not detected when importing the same source
	// file twice in the same IMPORT, into a table without a PK. It exercises the
	// row_id generation logic.
	t.Run("multiple-file-import-into-without-pk", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		sqlDB.Exec(t,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s, %s)`, testFiles.files[0], testFiles.files[0]),
		)

		// Verify correct number of rows via COUNT.
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
		if result != rowsPerFile*2 {
			t.Fatalf("expected %d rows, got %d", rowsPerFile*2, result)
		}
	})

	// IMPORT INTO disallows shadowing of existing keys when ingesting data. With
	// the exception of shadowing keys having the same ts and value.
	//
	// This tests key collision detection when importing the same source file
	// twice. The ts across imports is different, and so this is considered a
	// collision.
	t.Run("import-into-same-file-diff-imports", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		sqlDB.Exec(t,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]),
		)

		sqlDB.ExpectErr(
			t, `ingested key collides with an existing one: /Table/\d+/1/0/0`,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// When the ts and value of the ingested keys across SSTs match the existing
	// keys we do not consider this to be a collision. This is to support IMPORT
	// job pause/resumption.
	//
	// To ensure uniform behavior we apply the same exception to keys within the
	// same SST.
	//
	// This test attempts to ingest duplicate keys in the same SST, with the same
	// value, and succeeds in doing so.
	t.Run("import-into-dups-in-sst", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		sqlDB.Exec(t,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.fileWithDupKeySameValue[0]),
		)

		// Verify correct number of rows via COUNT.
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
		if result != 200 {
			t.Fatalf("expected 200 rows, got %d", result)
		}
	})

	// This tests key collision detection and importing a source file with the
	// colliding key sandwiched between valid keys.
	t.Run("import-into-key-collision", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		sqlDB.Exec(t,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]),
		)

		sqlDB.ExpectErr(
			t, `ingested key collides with an existing one: /Table/\d+/1/0/0`,
			fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.fileWithShadowKeys[0]),
		)
	})

	// Tests that IMPORT INTO invalidates FK and CHECK constraints.
	t.Run("import-into-invalidate-constraints", func(t *testing.T) {

		sqlDB.Exec(t, `CREATE TABLE ref (b STRING PRIMARY KEY)`)
		defer sqlDB.Exec(t, `DROP TABLE ref`)
		sqlDB.Exec(t, `CREATE TABLE t (a INT CHECK (a >= 0), b STRING, CONSTRAINT fk_ref FOREIGN KEY (b) REFERENCES ref)`)
		defer sqlDB.Exec(t, `DROP TABLE t`)

		var checkValidated, fkValidated bool
		sqlDB.QueryRow(t, fmt.Sprintf(`SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'check_a'`)).Scan(&checkValidated)
		sqlDB.QueryRow(t, fmt.Sprintf(`SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'fk_ref'`)).Scan(&fkValidated)

		// Prior to import all constraints should be validated.
		if !checkValidated || !fkValidated {
			t.Fatal("Constraints not validated on creation.\n")
		}

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]))

		sqlDB.QueryRow(t, fmt.Sprintf(`SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'check_a'`)).Scan(&checkValidated)
		sqlDB.QueryRow(t, fmt.Sprintf(`SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'fk_ref'`)).Scan(&fkValidated)

		// Following an import the constraints should be unvalidated.
		if checkValidated || fkValidated {
			t.Fatal("FK and CHECK constraints not unvalidated after IMPORT INTO\n")
		}
	})
}

func BenchmarkImport(b *testing.B) {
	const (
		nodes    = 3
		numFiles = nodes + 2
	)
	baseDir := filepath.Join("testdata", "csv")
	ctx := context.Background()
	tc := testcluster.StartTestCluster(b, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	testFiles := makeCSVData(b, numFiles, b.N*100, nodes, 16)

	b.ResetTimer()

	sqlDB.Exec(b,
		fmt.Sprintf(
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))
			CSV DATA (%s)`,
			strings.Join(testFiles.files, ","),
		))
}

// a importRowProducer implementation that returns 'n' rows.
type csvBenchmarkStream struct {
	n    int
	pos  int
	data [][]string
}

func (s *csvBenchmarkStream) Progress() float32 {
	return float32(s.pos) / float32(s.n)
}

func (s *csvBenchmarkStream) Scan() bool {
	s.pos++
	return s.pos <= s.n
}

func (s *csvBenchmarkStream) Err() error {
	return nil
}

func (s *csvBenchmarkStream) Skip() error {
	return nil
}

func (s *csvBenchmarkStream) Row() (interface{}, error) {
	return s.data[s.pos%len(s.data)], nil
}

// Read implements Reader interface.  It's used by delimited
// benchmark to read its tab separated input.
func (s *csvBenchmarkStream) Read(buf []byte) (int, error) {
	if s.Scan() {
		r, err := s.Row()
		if err != nil {
			return 0, err
		}
		return copy(buf, strings.Join(r.([]string), "\t")+"\n"), nil
	}
	return 0, io.EOF
}

var _ importRowProducer = &csvBenchmarkStream{}

// BenchmarkConvertRecord-16    	 1000000	      2107 ns/op	  56.94 MB/s	    3600 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2106 ns/op	  56.97 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2100 ns/op	  57.14 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2286 ns/op	  52.49 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2378 ns/op	  50.46 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2427 ns/op	  49.43 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2399 ns/op	  50.02 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2365 ns/op	  50.73 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2376 ns/op	  50.49 MB/s	    3606 B/op	     101 allocs/op
// BenchmarkConvertRecord-16    	  500000	      2390 ns/op	  50.20 MB/s	    3606 B/op	     101 allocs/op
func BenchmarkCSVConvertRecord(b *testing.B) {
	ctx := context.Background()

	tpchLineItemDataRows := [][]string{
		{"1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"},
		{"1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "},
		{"1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"},
		{"1", "2132", "4633", "4", "28", "28955.64", "0.09", "0.06", "N", "O", "1996-04-21", "1996-03-30", "1996-05-16", "NONE", "AIR", "lites. fluffily even de"},
		{"1", "24027", "1534", "5", "24", "22824.48", "0.10", "0.04", "N", "O", "1996-03-30", "1996-03-14", "1996-04-01", "NONE", "FOB", " pending foxes. slyly re"},
		{"1", "15635", "638", "6", "32", "49620.16", "0.07", "0.02", "N", "O", "1996-01-30", "1996-02-07", "1996-02-03", "DELIVER IN PERSON", "MAIL", "arefully slyly ex"},
		{"2", "106170", "1191", "1", "38", "44694.46", "0.00", "0.05", "N", "O", "1997-01-28", "1997-01-14", "1997-02-02", "TAKE BACK RETURN", "RAIL", "ven requests. deposits breach a"},
		{"3", "4297", "1798", "1", "45", "54058.05", "0.06", "0.00", "R", "F", "1994-02-02", "1994-01-04", "1994-02-23", "NONE", "AIR", "ongside of the furiously brave acco"},
		{"3", "19036", "6540", "2", "49", "46796.47", "0.10", "0.00", "R", "F", "1993-11-09", "1993-12-20", "1993-11-24", "TAKE BACK RETURN", "RAIL", " unusual accounts. eve"},
		{"3", "128449", "3474", "3", "27", "39890.88", "0.06", "0.07", "A", "F", "1994-01-16", "1993-11-22", "1994-01-23", "DELIVER IN PERSON", "SHIP", "nal foxes wake."},
	}
	b.SetBytes(120) // Raw input size. With 8 indexes, expect more on output side.

	stmt, err := parser.ParseOne(`CREATE TABLE lineitem (
		l_orderkey      INT8 NOT NULL,
		l_partkey       INT8 NOT NULL,
		l_suppkey       INT8 NOT NULL,
		l_linenumber    INT8 NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY     (l_orderkey, l_linenumber),
		INDEX l_ok      (l_orderkey ASC),
		INDEX l_pk      (l_partkey ASC),
		INDEX l_sk      (l_suppkey ASC),
		INDEX l_sd      (l_shipdate ASC),
		INDEX l_cd      (l_commitdate ASC),
		INDEX l_rd      (l_receiptdate ASC),
		INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
	)`)
	if err != nil {
		b.Fatal(err)
	}
	create := stmt.AST.(*tree.CreateTable)
	st := cluster.MakeTestingClusterSettings()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(st)

	tableDesc, err := MakeSimpleTableDescriptor(ctx, &semaCtx, st, create, sqlbase.ID(100), sqlbase.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	descr := tableDesc.TableDesc()
	importCtx := &parallelImportContext{
		evalCtx:   &evalCtx,
		tableDesc: descr,
		kvCh:      kvCh,
	}

	producer := &csvBenchmarkStream{
		n:    b.N,
		pos:  0,
		data: tpchLineItemDataRows,
	}
	consumer := &csvRowConsumer{importCtx: importCtx, opts: &roachpb.CSVOptions{}}
	b.ResetTimer()
	require.NoError(b, runParallelImport(ctx, importCtx, &importFileContext{}, producer, consumer))
	close(kvCh)
	b.ReportAllocs()
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/importccl
// BenchmarkDelimitedConvertRecord-16    	  500000	      2473 ns/op	  48.51 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      2580 ns/op	  46.51 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      2678 ns/op	  44.80 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      2897 ns/op	  41.41 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      3250 ns/op	  36.92 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      3261 ns/op	  36.80 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      3016 ns/op	  39.79 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      2943 ns/op	  40.77 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      3004 ns/op	  39.94 MB/s
// BenchmarkDelimitedConvertRecord-16    	  500000	      2966 ns/op	  40.45 MB/s
func BenchmarkDelimitedConvertRecord(b *testing.B) {
	ctx := context.Background()

	tpchLineItemDataRows := [][]string{
		{"1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"},
		{"1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "},
		{"1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"},
		{"1", "2132", "4633", "4", "28", "28955.64", "0.09", "0.06", "N", "O", "1996-04-21", "1996-03-30", "1996-05-16", "NONE", "AIR", "lites. fluffily even de"},
		{"1", "24027", "1534", "5", "24", "22824.48", "0.10", "0.04", "N", "O", "1996-03-30", "1996-03-14", "1996-04-01", "NONE", "FOB", " pending foxes. slyly re"},
		{"1", "15635", "638", "6", "32", "49620.16", "0.07", "0.02", "N", "O", "1996-01-30", "1996-02-07", "1996-02-03", "DELIVER IN PERSON", "MAIL", "arefully slyly ex"},
		{"2", "106170", "1191", "1", "38", "44694.46", "0.00", "0.05", "N", "O", "1997-01-28", "1997-01-14", "1997-02-02", "TAKE BACK RETURN", "RAIL", "ven requests. deposits breach a"},
		{"3", "4297", "1798", "1", "45", "54058.05", "0.06", "0.00", "R", "F", "1994-02-02", "1994-01-04", "1994-02-23", "NONE", "AIR", "ongside of the furiously brave acco"},
		{"3", "19036", "6540", "2", "49", "46796.47", "0.10", "0.00", "R", "F", "1993-11-09", "1993-12-20", "1993-11-24", "TAKE BACK RETURN", "RAIL", " unusual accounts. eve"},
		{"3", "128449", "3474", "3", "27", "39890.88", "0.06", "0.07", "A", "F", "1994-01-16", "1993-11-22", "1994-01-23", "DELIVER IN PERSON", "SHIP", "nal foxes wake."},
	}
	b.SetBytes(120) // Raw input size. With 8 indexes, expect more on output side.

	stmt, err := parser.ParseOne(`CREATE TABLE lineitem (
		l_orderkey      INT8 NOT NULL,
		l_partkey       INT8 NOT NULL,
		l_suppkey       INT8 NOT NULL,
		l_linenumber    INT8 NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY     (l_orderkey, l_linenumber),
		INDEX l_ok      (l_orderkey ASC),
		INDEX l_pk      (l_partkey ASC),
		INDEX l_sk      (l_suppkey ASC),
		INDEX l_sd      (l_shipdate ASC),
		INDEX l_cd      (l_commitdate ASC),
		INDEX l_rd      (l_receiptdate ASC),
		INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
	)`)
	if err != nil {
		b.Fatal(err)
	}
	create := stmt.AST.(*tree.CreateTable)
	st := cluster.MakeTestingClusterSettings()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(st)

	tableDesc, err := MakeSimpleTableDescriptor(ctx, &semaCtx, st, create, sqlbase.ID(100), sqlbase.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	descr := tableDesc.TableDesc()
	cols := make(tree.NameList, len(descr.Columns))
	for i, col := range descr.Columns {
		cols[i] = tree.Name(col.Name)
	}
	r, err := newMysqloutfileReader(roachpb.MySQLOutfileOptions{
		RowSeparator:   '\n',
		FieldSeparator: '\t',
	}, kvCh, 0, 0, descr, &evalCtx)
	require.NoError(b, err)

	producer := &csvBenchmarkStream{
		n:    b.N,
		pos:  0,
		data: tpchLineItemDataRows,
	}

	delimited := &fileReader{Reader: producer}
	b.ResetTimer()
	require.NoError(b, r.readFile(ctx, delimited, 0, 0, nil))
	close(kvCh)
	b.ReportAllocs()
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/importccl
// BenchmarkPgCopyConvertRecord-16    	  317534	      3752 ns/op	  31.98 MB/s
// BenchmarkPgCopyConvertRecord-16    	  317433	      3767 ns/op	  31.86 MB/s
// BenchmarkPgCopyConvertRecord-16    	  308832	      3867 ns/op	  31.03 MB/s
// BenchmarkPgCopyConvertRecord-16    	  255715	      3913 ns/op	  30.67 MB/s
// BenchmarkPgCopyConvertRecord-16    	  303086	      3942 ns/op	  30.44 MB/s
// BenchmarkPgCopyConvertRecord-16    	  304741	      3520 ns/op	  34.09 MB/s
// BenchmarkPgCopyConvertRecord-16    	  338954	      3506 ns/op	  34.22 MB/s
// BenchmarkPgCopyConvertRecord-16    	  339795	      3531 ns/op	  33.99 MB/s
// BenchmarkPgCopyConvertRecord-16    	  339940	      3610 ns/op	  33.24 MB/s
// BenchmarkPgCopyConvertRecord-16    	  307701	      3833 ns/op	  31.30 MB/s
func BenchmarkPgCopyConvertRecord(b *testing.B) {
	ctx := context.Background()

	tpchLineItemDataRows := [][]string{
		{"1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"},
		{"1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "},
		{"1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"},
		{"1", "2132", "4633", "4", "28", "28955.64", "0.09", "0.06", "N", "O", "1996-04-21", "1996-03-30", "1996-05-16", "NONE", "AIR", "lites. fluffily even de"},
		{"1", "24027", "1534", "5", "24", "22824.48", "0.10", "0.04", "N", "O", "1996-03-30", "1996-03-14", "1996-04-01", "NONE", "FOB", " pending foxes. slyly re"},
		{"1", "15635", "638", "6", "32", "49620.16", "0.07", "0.02", "N", "O", "1996-01-30", "1996-02-07", "1996-02-03", "DELIVER IN PERSON", "MAIL", "arefully slyly ex"},
		{"2", "106170", "1191", "1", "38", "44694.46", "0.00", "0.05", "N", "O", "1997-01-28", "1997-01-14", "1997-02-02", "TAKE BACK RETURN", "RAIL", "ven requests. deposits breach a"},
		{"3", "4297", "1798", "1", "45", "54058.05", "0.06", "0.00", "R", "F", "1994-02-02", "1994-01-04", "1994-02-23", "NONE", "AIR", "ongside of the furiously brave acco"},
		{"3", "19036", "6540", "2", "49", "46796.47", "0.10", "0.00", "R", "F", "1993-11-09", "1993-12-20", "1993-11-24", "TAKE BACK RETURN", "RAIL", " unusual accounts. eve"},
		{"3", "128449", "3474", "3", "27", "39890.88", "0.06", "0.07", "A", "F", "1994-01-16", "1993-11-22", "1994-01-23", "DELIVER IN PERSON", "SHIP", "nal foxes wake."},
	}
	b.SetBytes(120) // Raw input size. With 8 indexes, expect more on output side.

	stmt, err := parser.ParseOne(`CREATE TABLE lineitem (
		l_orderkey      INT8 NOT NULL,
		l_partkey       INT8 NOT NULL,
		l_suppkey       INT8 NOT NULL,
		l_linenumber    INT8 NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY     (l_orderkey, l_linenumber),
		INDEX l_ok      (l_orderkey ASC),
		INDEX l_pk      (l_partkey ASC),
		INDEX l_sk      (l_suppkey ASC),
		INDEX l_sd      (l_shipdate ASC),
		INDEX l_cd      (l_commitdate ASC),
		INDEX l_rd      (l_receiptdate ASC),
		INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
	)`)
	if err != nil {
		b.Fatal(err)
	}
	create := stmt.AST.(*tree.CreateTable)
	semaCtx := tree.MakeSemaContext()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	tableDesc, err := MakeSimpleTableDescriptor(ctx, &semaCtx, st, create, sqlbase.ID(100),
		sqlbase.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	descr := tableDesc.TableDesc()
	cols := make(tree.NameList, len(descr.Columns))
	for i, col := range descr.Columns {
		cols[i] = tree.Name(col.Name)
	}
	r, err := newPgCopyReader(roachpb.PgCopyOptions{
		Delimiter:  '\t',
		Null:       `\N`,
		MaxRowSize: 4096,
	}, kvCh, 0, 0, descr, &evalCtx)
	require.NoError(b, err)

	producer := &csvBenchmarkStream{
		n:    b.N,
		pos:  0,
		data: tpchLineItemDataRows,
	}

	pgCopyInput := &fileReader{Reader: producer}
	b.ResetTimer()
	require.NoError(b, r.readFile(ctx, pgCopyInput, 0, 0, nil))
	close(kvCh)
	b.ReportAllocs()
}

// TestImportControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on import jobs.
func TestImportControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("TODO(dt): add knob to force faster progress checks.")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	var serverArgs base.TestServerArgs
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	serverArgs.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		// TODO (lucy): if/when this test gets reinstated, figure out what knobs are
		// needed.
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)

	makeSrv := func() *httptest.Server {
		var once sync.Once
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				// The following code correctly handles both the case where, after the
				// CANCEL JOB is issued, the second stage of the IMPORT (the shuffle,
				// after the sampling) may or may not be started. If it was started, then a
				// second GET request is done. The once here will cause that request to not
				// block. The draining for loop below will cause jobutils.RunJob's second send
				// on allowResponse to succeed (which it does after issuing the CANCEL JOB).
				once.Do(func() {
					<-allowResponse
					go func() {
						for range allowResponse {
						}
					}()
				})

				_, _ = w.Write([]byte(r.URL.Path[1:]))
			}
		}))
	}

	t.Run("cancel", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE cancelimport`)

		srv := makeSrv()
		defer srv.Close()

		var urls []string
		for i := 0; i < 10; i++ {
			urls = append(urls, fmt.Sprintf("'%s/%d'", srv.URL, i))
		}
		csvURLs := strings.Join(urls, ", ")

		query := fmt.Sprintf(`IMPORT TABLE cancelimport.t (i INT8 PRIMARY KEY) CSV DATA (%s)`, csvURLs)

		if _, err := jobutils.RunJob(
			t, sqlDB, &allowResponse, []string{"cancel"}, query,
		); !testutils.IsError(err, "job canceled") {
			t.Fatalf("expected 'job canceled' error, but got %+v", err)
		}
		// Check that executing again succeeds. This won't work if the first import
		// was not successfully canceled.
		sqlDB.Exec(t, query)
	})

	t.Run("pause", func(t *testing.T) {
		// Test that IMPORT can be paused and resumed. This test also attempts to
		// only pause the job after it has begun splitting ranges. When the job
		// is resumed, if the sampling phase is re-run, the splits points will
		// differ. When AddSSTable attempts to import the new ranges, they will
		// fail because there is an existing split in the key space that it cannot
		// handle. Use a sstsize that will more-or-less (since it is statistical)
		// always cause this condition.

		sqlDB.Exec(t, `CREATE DATABASE pauseimport`)

		srv := makeSrv()
		defer srv.Close()

		count := 100
		// This test takes a while with the race detector, so reduce the number of
		// files in an attempt to speed it up.
		if util.RaceEnabled {
			count = 20
		}

		urls := make([]string, count)
		for i := 0; i < count; i++ {
			urls[i] = fmt.Sprintf("'%s/%d'", srv.URL, i)
		}
		csvURLs := strings.Join(urls, ", ")
		query := fmt.Sprintf(`IMPORT TABLE pauseimport.t (i INT8 PRIMARY KEY) CSV DATA (%s) WITH sstsize = '50B'`, csvURLs)

		jobID, err := jobutils.RunJob(
			t, sqlDB, &allowResponse, []string{"PAUSE"}, query,
		)
		if !testutils.IsError(err, "job paused") {
			t.Fatalf("unexpected: %v", err)
		}
		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
		jobutils.WaitForJob(t, sqlDB, jobID)
		sqlDB.CheckQueryResults(t,
			`SELECT * FROM pauseimport.t ORDER BY i`,
			sqlDB.QueryStr(t, `SELECT * FROM generate_series(0, $1)`, count-1),
		)
	})
}

// TestImportWorkerFailure tests that IMPORT can restart after the failure
// of a worker node.
func TestImportWorkerFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(mjibson): Although this test passes most of the time it still
	// sometimes fails because not all kinds of failures caused by shutting a
	// node down are detected and retried.
	t.Skip("flaky due to undetected kinds of failures when the node is shutdown")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	allowResponse := make(chan struct{})
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(r.URL.Path[1:]))
		}
	}))
	defer srv.Close()

	count := 20
	urls := make([]string, count)
	for i := 0; i < count; i++ {
		urls[i] = fmt.Sprintf("'%s/%d'", srv.URL, i)
	}
	csvURLs := strings.Join(urls, ", ")
	query := fmt.Sprintf(`IMPORT TABLE t (i INT8 PRIMARY KEY) CSV DATA (%s) WITH sstsize = '1B'`, csvURLs)

	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	select {
	case allowResponse <- struct{}{}:
	case err := <-errCh:
		t.Fatalf("%s: query returned before expected: %s", err, query)
	}
	var jobID int64
	sqlDB.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)

	// Shut down a node. This should force LoadCSV to fail in its current
	// execution. It should detect this as a context canceled error.
	tc.StopServer(1)

	close(allowResponse)
	// We expect the statement to fail.
	if err := <-errCh; !testutils.IsError(err, "node failure") {
		t.Fatal(err)
	}

	// But the job should be restarted and succeed eventually.
	jobutils.WaitForJob(t, sqlDB, jobID)
	sqlDB.CheckQueryResults(t,
		`SELECT * FROM t ORDER BY i`,
		sqlDB.QueryStr(t, `SELECT * FROM generate_series(0, $1)`, count-1),
	)
}

// TestImportLivenessWithRestart tests that a node liveness transition
// during IMPORT correctly resumes after the node executing the job
// becomes non-live (from the perspective of the jobs registry).
//
// Its actual purpose is to address the second bug listed in #22924 about
// the addsstable arguments not in request range. The theory was that the
// restart in that issue was caused by node liveness and that the work
// already performed (the splits and addsstables) somehow caused the second
// error. However this does not appear to be the case, as running many stress
// iterations with differing constants (rows, sstsize, kv.bulk_ingest.batch_size)
// was not able to fail in the way listed by the second bug.
func TestImportLivenessWithRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("TODO(dt): this relies on chunking done by prior version of IMPORT." +
		"Rework this test, or replace it with resume-tests + jobs infra tests.")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	const rows = 5000
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT8 PRIMARY KEY) CSV DATA ($1) WITH sstsize = '500B', experimental_sorted_ingestion`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Progress{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, progress FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node non-live and wait for cancellation
	nl.FakeSetExpiration(1, hlc.MinTimestamp)
	// Wait for the registry cancel loop to run and cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)
	err := <-errCh
	if !testutils.IsError(err, "job .*: node liveness error") {
		t.Fatalf("unexpected: %v", err)
	}

	// Ensure that partial progress has been recorded
	partialProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	if len(partialProgress.Details.(*jobspb.Progress_Import).Import.SpanProgress) == 0 {
		t.Fatal("no partial import progress detected")
	}

	// Make the node live again
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)
	// The registry should now adopt the job and resume it.
	jobutils.WaitForJob(t, sqlDB, jobID)
	// Verify that the job lease was updated
	rescheduledProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	if rescheduledProgress.ModifiedMicros <= originalLease.ModifiedMicros {
		t.Fatalf("expecting rescheduled job to have a later modification time: %d vs %d",
			rescheduledProgress.ModifiedMicros, originalLease.ModifiedMicros)
	}

	// Verify that all expected rows are present after a stop/start cycle.
	var rowCount int
	sqlDB.QueryRow(t, "SELECT count(*) from liveness.t").Scan(&rowCount)
	if rowCount != rows {
		t.Fatalf("not all rows were present.  Expecting %d, had %d", rows, rowCount)
	}

	// Verify that all write progress coalesced into a single span
	// encompassing the entire table.
	spans := rescheduledProgress.Details.(*jobspb.Progress_Import).Import.SpanProgress
	if len(spans) != 1 {
		t.Fatalf("expecting only a single progress span, had %d\n%s", len(spans), spans)
	}

	// Ensure that an entire table range is marked as complete
	tableSpan := roachpb.Span{
		Key:    keys.MinKey,
		EndKey: keys.MaxKey,
	}
	if !tableSpan.EqualValue(spans[0]) {
		t.Fatalf("expected entire table to be marked complete, had %s", spans[0])
	}
}

// TestImportLivenessWithLeniency tests that a temporary node liveness
// transition during IMPORT doesn't cancel the job, but allows the
// owning node to continue processing.
func TestImportLivenessWithLeniency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)
	// We want to know exactly how much leniency is configured.
	sqlDB.Exec(t, `SET CLUSTER SETTING jobs.registry.leniency = '1m'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const rows = 5000
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT8 PRIMARY KEY) CSV DATA ($1) WITH sstsize = '500B'`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Payload{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, payload FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node slightly tardy.
	nl.FakeSetExpiration(1, hlc.Timestamp{
		WallTime: hlc.UnixNano() - (15 * time.Second).Nanoseconds(),
	})

	// Wait for the registry cancel loop to run and not cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)

	// Set the node to be fully live again.  This prevents the registry
	// from canceling all of the jobs if the test node is saturated
	// and the import runs slowly.
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)

	// Verify that the client didn't see anything amiss.
	if err := <-errCh; err != nil {
		t.Fatalf("import job should have completed: %s", err)
	}

	// The job should have completed normally.
	jobutils.WaitForJob(t, sqlDB, jobID)
}

// TestImportMVCCChecksums verifies that MVCC checksums are correctly
// computed by issuing a secondary index change that runs a CPut on the
// index. See #23984.
func TestImportMVCCChecksums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "1,1,1")
		}
	}))
	defer srv.Close()

	sqlDB.Exec(t, `IMPORT TABLE d.t (
		a INT8 PRIMARY KEY,
		b INT8,
		c INT8,
		INDEX (b) STORING (c)
	) CSV DATA ($1)`, srv.URL)
	sqlDB.Exec(t, `UPDATE d.t SET c = 2 WHERE a = 1`)
}

func TestImportMysql(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://github.com/cockroachdb/cockroach/issues/40263")

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	files := getMysqldumpTestdata(t)
	simple := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.simple, baseDir))}
	second := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.second, baseDir))}
	multitable := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.wholeDB, baseDir))}
	multitableGz := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.wholeDB+".gz", baseDir))}
	multitableBz := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.wholeDB+".bz2", baseDir))}

	const expectSimple, expectSecond, expectEverything = 1 << 0, 1 << 2, 1 << 3
	const expectAll = -1
	for _, c := range []struct {
		name     string
		expected int
		query    string
		args     []interface{}
	}{
		{`read data only`, expectSimple, `IMPORT TABLE simple (i INT8 PRIMARY KEY, s text, b bytea) MYSQLDUMP DATA ($1)`, simple},
		{`single table dump`, expectSimple, `IMPORT TABLE simple FROM MYSQLDUMP ($1)`, simple},
		{`second table dump`, expectSecond, `IMPORT TABLE second FROM MYSQLDUMP ($1) WITH skip_foreign_keys`, second},
		{`simple from multi`, expectSimple, `IMPORT TABLE simple FROM MYSQLDUMP ($1)`, multitable},
		{`second from multi`, expectSecond, `IMPORT TABLE second FROM MYSQLDUMP ($1) WITH skip_foreign_keys`, multitable},
		{`all from multi`, expectAll, `IMPORT MYSQLDUMP ($1)`, multitable},
		{`all from multi gzip`, expectAll, `IMPORT MYSQLDUMP ($1)`, multitableGz},
		{`all from multi bzip`, expectAll, `IMPORT MYSQLDUMP ($1)`, multitableBz},
	} {
		t.Run(c.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second, third, everything CASCADE`)
			sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS simple_auto_inc, third_auto_inc`)
			sqlDB.Exec(t, c.query, c.args...)

			if c.expected&expectSimple != 0 {
				if c.name != "read data only" {
					sqlDB.Exec(t, "INSERT INTO simple (s) VALUES ('auto-inc')")
				}

				for idx, row := range sqlDB.QueryStr(t, "SELECT * FROM simple ORDER BY i") {
					{
						if idx == len(simpleTestRows) {
							if expected, actual := "auto-inc", row[1]; expected != actual {
								t.Fatalf("expected rowi=%s string to be %q, got %q", row[0], expected, actual)
							}
							continue
						}
						expected, actual := simpleTestRows[idx].s, row[1]
						if expected == injectNull {
							expected = "NULL"
						}
						if expected != actual {
							t.Fatalf("expected rowi=%s string to be %q, got %q", row[0], expected, actual)
						}
					}

					{
						expected, actual := simpleTestRows[idx].b, row[2]
						if expected == nil {
							expected = []byte("NULL")
						}
						if !bytes.Equal(expected, []byte(actual)) {
							t.Fatalf("expected rowi=%s bytes to be %q, got %q", row[0], expected, actual)
						}
					}
				}
			} else {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM simple LIMIT 1`)
			}

			if c.expected&expectSecond != 0 {
				res := sqlDB.QueryStr(t, "SELECT * FROM second ORDER BY i")
				if expected, actual := secondTableRows, len(res); expected != actual {
					t.Fatalf("expected %d, got %d", expected, actual)
				}
				for _, row := range res {
					if i, j := row[0], row[1]; i != "-"+j {
						t.Fatalf("expected %s = - %s", i, j)
					}
				}
			} else {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM second LIMIT 1`)
			}
			if c.expected&expectEverything != 0 {
				res := sqlDB.QueryStr(t, "SELECT i, c, iw, fl, d53, j FROM everything ORDER BY i")
				if expected, actual := len(everythingTestRows), len(res); expected != actual {
					t.Fatalf("expected %d, got %d", expected, actual)
				}
				for i := range res {
					if got, expected := res[i][0], fmt.Sprintf("%v", everythingTestRows[i].i); got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][1], everythingTestRows[i].c; got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][2], fmt.Sprintf("%v", everythingTestRows[i].iw); got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][3], fmt.Sprintf("%v", everythingTestRows[i].fl); got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][4], everythingTestRows[i].d53; got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][5], everythingTestRows[i].j; got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
				}
			} else {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM everything LIMIT 1`)
			}
		})
	}
}

func TestImportMysqlOutfile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata", "mysqlout")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	testRows, configs := getMysqlOutfileTestdata(t)

	for i, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			var opts []interface{}

			cmd := fmt.Sprintf(`IMPORT TABLE test%d (i INT8 PRIMARY KEY, s text, b bytea) DELIMITED DATA ($1)`, i)
			opts = append(opts, fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(cfg.filename, baseDir)))

			var flags []string
			if cfg.opts.RowSeparator != '\n' {
				opts = append(opts, string(cfg.opts.RowSeparator))
				flags = append(flags, fmt.Sprintf("rows_terminated_by = $%d", len(opts)))
			}
			if cfg.opts.FieldSeparator != '\t' {
				opts = append(opts, string(cfg.opts.FieldSeparator))
				flags = append(flags, fmt.Sprintf("fields_terminated_by = $%d", len(opts)))
			}
			if cfg.opts.Enclose == roachpb.MySQLOutfileOptions_Always {
				opts = append(opts, string(cfg.opts.Encloser))
				flags = append(flags, fmt.Sprintf("fields_enclosed_by = $%d", len(opts)))
			}
			if cfg.opts.HasEscape {
				opts = append(opts, string(cfg.opts.Escape))
				flags = append(flags, fmt.Sprintf("fields_escaped_by = $%d", len(opts)))
			}
			if len(flags) > 0 {
				cmd += " WITH " + strings.Join(flags, ", ")
			}
			sqlDB.Exec(t, cmd, opts...)
			for idx, row := range sqlDB.QueryStr(t, fmt.Sprintf("SELECT * FROM test%d ORDER BY i", i)) {
				expected, actual := testRows[idx].s, row[1]
				if expected == injectNull {
					expected = "NULL"
				}

				if expected != actual {
					t.Fatalf("expected row i=%s string to be %q, got %q", row[0], expected, actual)
				}
			}
		})
	}
}

func TestImportPgCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata", "pgcopy")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	testRows, configs := getPgCopyTestdata(t)

	for i, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			var opts []interface{}

			cmd := fmt.Sprintf(`IMPORT TABLE test%d (i INT8 PRIMARY KEY, s text, b bytea) PGCOPY DATA ($1)`, i)
			opts = append(opts, fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(cfg.filename, baseDir)))

			var flags []string
			if cfg.opts.Delimiter != '\t' {
				opts = append(opts, string(cfg.opts.Delimiter))
				flags = append(flags, fmt.Sprintf("delimiter = $%d", len(opts)))
			}
			if cfg.opts.Null != `\N` {
				opts = append(opts, cfg.opts.Null)
				flags = append(flags, fmt.Sprintf("nullif = $%d", len(opts)))
			}
			if len(flags) > 0 {
				cmd += " WITH " + strings.Join(flags, ", ")
			}
			t.Log(cmd, opts)
			sqlDB.Exec(t, cmd, opts...)
			for idx, row := range sqlDB.QueryStr(t, fmt.Sprintf("SELECT * FROM test%d ORDER BY i", i)) {
				{
					expected, actual := testRows[idx].s, row[1]
					if expected == injectNull {
						expected = "NULL"
					}

					if expected != actual {
						t.Fatalf("expected row i=%s string to be %q, got %q", row[0], expected, actual)
					}
				}

				{
					expected, actual := testRows[idx].b, row[2]
					if expected == nil {
						expected = []byte("NULL")
					}
					if !bytes.Equal(expected, []byte(actual)) {
						t.Fatalf("expected rowi=%s bytes to be %q, got %q", row[0], expected, actual)
					}
				}
			}
		})
	}
}

func TestImportPgDump(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	simplePgTestRows, simpleFile := getSimplePostgresDumpTestdata(t)
	simple := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(simpleFile, baseDir))}
	secondTableRowCount, secondFile := getSecondPostgresDumpTestdata(t)
	second := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(secondFile, baseDir))}
	multitableFile := getMultiTablePostgresDumpTestdata(t)
	multitable := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(multitableFile, baseDir))}

	const expectAll, expectSimple, expectSecond = 1, 2, 3

	for _, c := range []struct {
		name     string
		expected int
		query    string
		args     []interface{}
	}{
		{
			`read data only`,
			expectSimple,
			`IMPORT TABLE simple (
				i INT8,
				s text,
				b bytea,
				CONSTRAINT simple_pkey PRIMARY KEY (i),
				UNIQUE INDEX simple_b_s_idx (b, s),
				INDEX simple_s_idx (s)
			) PGDUMP DATA ($1)`,
			simple,
		},
		{`single table dump`, expectSimple, `IMPORT TABLE simple FROM PGDUMP ($1)`, simple},
		{`second table dump`, expectSecond, `IMPORT TABLE second FROM PGDUMP ($1)`, second},
		{`simple from multi`, expectSimple, `IMPORT TABLE simple FROM PGDUMP ($1)`, multitable},
		{`second from multi`, expectSecond, `IMPORT TABLE second FROM PGDUMP ($1)`, multitable},
		{`all from multi`, expectAll, `IMPORT PGDUMP ($1)`, multitable},
	} {
		t.Run(c.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second`)
			sqlDB.Exec(t, c.query, c.args...)

			if c.expected == expectSimple || c.expected == expectAll {
				// Verify table schema because PKs and indexes are at the bottom of pg_dump.
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE simple`, [][]string{{
					"simple", `CREATE TABLE simple (
	i INT8 NOT NULL,
	s STRING NULL,
	b BYTES NULL,
	CONSTRAINT simple_pkey PRIMARY KEY (i ASC),
	UNIQUE INDEX simple_b_s_idx (b ASC, s ASC),
	INDEX simple_s_idx (s ASC),
	FAMILY "primary" (i, s, b)
)`,
				}})

				rows := sqlDB.QueryStr(t, "SELECT * FROM simple ORDER BY i")
				if a, e := len(rows), len(simplePostgresTestRows); a != e {
					t.Fatalf("got %d rows, expected %d", a, e)
				}

				for idx, row := range rows {
					{
						expected, actual := simplePostgresTestRows[idx].s, row[1]
						if expected == injectNull {
							expected = "NULL"
						}
						if expected != actual {
							t.Fatalf("expected rowi=%s string to be %q, got %q", row[0], expected, actual)
						}
					}

					{
						expected, actual := simplePgTestRows[idx].b, row[2]
						if expected == nil {
							expected = []byte("NULL")
						}
						if !bytes.Equal(expected, []byte(actual)) {
							t.Fatalf("expected rowi=%s bytes to be %q, got %q", row[0], expected, actual)
						}
					}
				}
			}

			if c.expected == expectSecond || c.expected == expectAll {
				// Verify table schema because PKs and indexes are at the bottom of pg_dump.
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE second`, [][]string{{
					"second", `CREATE TABLE second (
	i INT8 NOT NULL,
	s STRING NULL,
	CONSTRAINT second_pkey PRIMARY KEY (i ASC),
	FAMILY "primary" (i, s)
)`,
				}})
				res := sqlDB.QueryStr(t, "SELECT * FROM second ORDER BY i")
				if expected, actual := secondTableRowCount, len(res); expected != actual {
					t.Fatalf("expected %d, got %d", expected, actual)
				}
				for _, row := range res {
					if i, s := row[0], row[1]; i != s {
						t.Fatalf("expected %s = %s", i, s)
					}
				}
			}

			if c.expected == expectSecond {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM simple LIMIT 1`)
			}
			if c.expected == expectSimple {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM second LIMIT 1`)
			}
			if c.expected == expectAll {
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE seqtable`, [][]string{{
					"seqtable", `CREATE TABLE seqtable (
	a INT8 NULL DEFAULT nextval('public.a_seq':::STRING),
	b INT8 NULL,
	FAMILY "primary" (a, b, rowid)
)`,
				}})
				sqlDB.CheckQueryResults(t, `SHOW CREATE SEQUENCE a_seq`, [][]string{{
					"a_seq", `CREATE SEQUENCE a_seq MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
				}})
				sqlDB.CheckQueryResults(t, `select last_value from a_seq`, [][]string{{"7"}})
				sqlDB.CheckQueryResults(t,
					`SELECT * FROM seqtable ORDER BY a`,
					sqlDB.QueryStr(t, `select a+1, a*10 from generate_series(0, 6) a`),
				)
				sqlDB.CheckQueryResults(t, `select last_value from a_seq`, [][]string{{"7"}})
				// This can sometimes retry, so the next value might not be 8.
				sqlDB.Exec(t, `INSERT INTO seqtable (b) VALUES (70)`)
				sqlDB.CheckQueryResults(t, `select last_value >= 8 from a_seq`, [][]string{{"true"}})
				sqlDB.CheckQueryResults(t,
					`SELECT b FROM seqtable WHERE a = (SELECT last_value FROM a_seq)`,
					[][]string{{"70"}},
				)
			}
		})
	}
}

func TestImportCockroachDump(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, "IMPORT PGDUMP ($1)", "nodelocal://0/cockroachdump/dump.sql")
	sqlDB.CheckQueryResults(t, "SELECT * FROM t ORDER BY i", [][]string{
		{"1", "test"},
		{"2", "other"},
	})
	sqlDB.CheckQueryResults(t, "SELECT * FROM a", [][]string{
		{"2"},
	})
	sqlDB.CheckQueryResults(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE t", [][]string{
		{"primary", "-6413178410144704641"},
		{"t_t_idx", "-4841734847805280813"},
	})
	sqlDB.CheckQueryResults(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE a", [][]string{
		{"primary", "-5808590958014384147"},
	})
	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE t", [][]string{
		{"t", `CREATE TABLE t (
	i INT8 NOT NULL,
	t STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	INDEX t_t_idx (t ASC),
	FAMILY "primary" (i, t)
)`},
	})
	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE a", [][]string{
		{"a", `CREATE TABLE a (
	i INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	CONSTRAINT fk_i_ref_t FOREIGN KEY (i) REFERENCES t(i),
	FAMILY "primary" (i)
)`},
	})
}

func TestCreateStatsAfterImport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = time.Microsecond

	const nodes = 1
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`)

	sqlDB.Exec(t, "IMPORT PGDUMP ($1)", "nodelocal://0/cockroachdump/dump.sql")

	// Verify that statistics have been created.
	sqlDB.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE t]`,
		[][]string{
			{"__auto__", "{i}", "2", "2", "0"},
			{"__auto__", "{t}", "2", "2", "0"},
		})
	sqlDB.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE a]`,
		[][]string{
			{"__auto__", "{i}", "1", "1", "0"},
		})
}

func TestImportAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata", "avro")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	simpleOcf := fmt.Sprintf("nodelocal://0/%s", "simple.ocf")
	simpleSchemaURI := fmt.Sprintf("nodelocal://0/%s", "simple-schema.json")
	simpleJSON := fmt.Sprintf("nodelocal://0/%s", "simple-sorted.json")
	simplePrettyJSON := fmt.Sprintf("nodelocal://0/%s", "simple-sorted.pjson")
	simpleBinRecords := fmt.Sprintf("nodelocal://0/%s", "simple-sorted-records.avro")
	tableSchema := fmt.Sprintf("nodelocal://0/%s", "simple-schema.sql")

	data, err := ioutil.ReadFile("testdata/avro/simple-schema.json")
	if err != nil {
		t.Fatal(err)
	}
	simpleSchema := string(data)

	tests := []struct {
		name   string
		sql    string
		create string
		args   []interface{}
		err    bool
	}{
		{
			name: "import-ocf",
			sql:  "IMPORT TABLE simple (i INT8 PRIMARY KEY, s text, b bytea) AVRO DATA ($1)",
			args: []interface{}{simpleOcf},
		},
		{
			name:   "import-ocf-into-table",
			sql:    "IMPORT INTO simple AVRO DATA ($1)",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea)",
			args:   []interface{}{simpleOcf},
		},
		{
			name:   "import-ocf-into-table-with-strict-validation",
			sql:    "IMPORT INTO simple AVRO DATA ($1)  WITH strict_validation",
			create: "CREATE TABLE simple (i INT8, s text, b bytea)",
			args:   []interface{}{simpleOcf},
		},
		{
			name: "import-ocf-create-using",
			sql:  "IMPORT TABLE simple CREATE USING $1 AVRO DATA ($2)",
			args: []interface{}{tableSchema, simpleOcf},
		},
		{
			name: "import-json-records",
			sql:  "IMPORT TABLE simple CREATE USING $1 AVRO DATA ($2) WITH data_as_json_records, schema_uri=$3",
			args: []interface{}{tableSchema, simpleJSON, simpleSchemaURI},
		},
		{
			name:   "import-json-records-into-table-ignores-extra-fields",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH data_as_json_records, schema_uri=$2",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY)",
			args:   []interface{}{simpleJSON, simpleSchemaURI},
		},
		{
			name: "import-json-records-inline-schema",
			sql:  "IMPORT TABLE simple CREATE USING $1 AVRO DATA ($2) WITH data_as_json_records, schema=$3",
			args: []interface{}{tableSchema, simpleJSON, simpleSchema},
		},
		{
			name: "import-json-pretty-printed-records",
			sql:  "IMPORT TABLE simple CREATE USING $1 AVRO DATA ($2) WITH data_as_json_records, schema_uri=$3",
			args: []interface{}{tableSchema, simplePrettyJSON, simpleSchemaURI},
		},
		{
			name: "import-avro-fragments",
			sql:  "IMPORT TABLE simple CREATE USING $1 AVRO DATA ($2) WITH data_as_binary_records, records_terminated_by='', schema_uri=$3",
			args: []interface{}{tableSchema, simpleBinRecords, simpleSchemaURI},
		},
		{
			name: "fail-import-expect-ocf-got-json",
			sql:  "IMPORT TABLE simple CREATE USING $1 AVRO DATA ($2)",
			args: []interface{}{tableSchema, simpleJSON},
			err:  true,
		},
		{
			name: "relaxed-import-sets-missing-fields",
			sql:  "IMPORT TABLE simple (i INT8 PRIMARY KEY, s text, b bytea, z int) AVRO DATA ($1)",
			args: []interface{}{simpleOcf},
		},
		{
			name: "relaxed-import-ignores-extra-fields",
			sql:  "IMPORT TABLE simple (i INT8 PRIMARY KEY) AVRO DATA ($1)",
			args: []interface{}{simpleOcf},
		},
		{
			name: "strict-import-errors-missing-fields",
			sql:  "IMPORT TABLE simple (i INT8 PRIMARY KEY, s text, b bytea, z int) AVRO DATA ($1) WITH strict_validation",
			args: []interface{}{simpleOcf},
			err:  true,
		},
		{
			name: "strict-import-errors-extra-fields",
			sql:  "IMPORT TABLE simple (i INT8 PRIMARY KEY) AVRO DATA ($1) WITH strict_validation",
			args: []interface{}{simpleOcf},
			err:  true,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Play a bit with producer/consumer batch sizes.
			defer TestingSetParallelImporterReaderBatchSize(13 * i)()

			_, err := sqlDB.DB.ExecContext(context.Background(), `DROP TABLE IF EXISTS simple CASCADE`)
			require.NoError(t, err)

			if len(test.create) > 0 {
				_, err := sqlDB.DB.ExecContext(context.Background(), test.create)
				require.NoError(t, err)
			}

			_, err = sqlDB.DB.ExecContext(context.Background(), test.sql, test.args...)
			if test.err {
				if err == nil {
					t.Error("expected error, but alas")
				}
				return
			}

			require.NoError(t, err)

			var numRows int
			sqlDB.QueryRow(t, `SELECT count(*) FROM simple`).Scan(&numRows)
			if numRows == 0 {
				t.Error("expected some rows after import")
			}
		})
	}
}

// TestImportClientDisconnect ensures that an import job can complete even if
// the client connection which started it closes. This test uses a helper
// subprocess to force a closed client connection without needing to rely
// on the driver to close a TCP connection. See TestImportClientDisconnectHelper
// for the subprocess.
func TestImportClientDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")

	// Make a server that will tell us when somebody has sent a request, wait to
	// be signaled, and then serve a CSV row for our table.
	allowResponse := make(chan struct{})
	gotRequest := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			return
		}
		select {
		case gotRequest <- struct{}{}:
		default:
		}
		select {
		case <-allowResponse:
		case <-ctx.Done(): // Deal with test failures.
		}
		_, _ = w.Write([]byte("1,asdfasdfasdfasdf"))
	}))
	defer srv.Close()

	// Make credentials for the new connection.
	runner.Exec(t, `CREATE USER testuser`)
	runner.Exec(t, `GRANT admin TO testuser`)
	pgURL, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(),
		"TestImportClientDisconnect-testuser", url.User("testuser"))
	defer cleanup()

	// Kick off the import on a new connection which we're going to close.
	done := make(chan struct{})
	ctxToCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer close(done)
		connCfg, err := pgx.ParseConnectionString(pgURL.String())
		assert.NoError(t, err)
		db, err := pgx.Connect(connCfg)
		assert.NoError(t, err)
		defer func() { _ = db.Close() }()
		_, err = db.ExecEx(ctxToCancel, `IMPORT TABLE foo (k INT PRIMARY KEY, v STRING) CSV DATA ($1)`,
			nil /* options */, srv.URL)
		assert.Equal(t, context.Canceled, err)
	}()

	// Wait for the import job to start.
	var jobID string
	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' ORDER BY created DESC LIMIT 1")
		return row.Scan(&jobID)
	})

	// Wait for it to actually start.
	<-gotRequest

	// Cancel the import context and wait for the goroutine to exit.
	cancel()
	<-done

	// Allow the import to proceed.
	close(allowResponse)

	// Wait for the job to get marked as succeeded.
	testutils.SucceedsSoon(t, func() error {
		var status string
		if err := conn.QueryRow("SELECT status FROM [SHOW JOB " + jobID + "]").Scan(&status); err != nil {
			return err
		}
		const succeeded = "succeeded"
		if status != succeeded {
			return errors.Errorf("expected %s, got %v", succeeded, status)
		}
		return nil
	})
}

func TestDisallowsInvalidFormatOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	allOpts := make(map[string]struct{})
	addOpts := func(opts map[string]struct{}) {
		for opt := range opts {
			allOpts[opt] = struct{}{}
		}
	}
	addOpts(allowedCommonOptions)
	addOpts(avroAllowedOptions)
	addOpts(csvAllowedOptions)
	addOpts(mysqlDumpAllowedOptions)
	addOpts(mysqlOutAllowedOptions)
	addOpts(pgDumpAllowedOptions)
	addOpts(pgCopyAllowedOptions)

	// Helper to pick num options from the set of allowed and the set
	// of all other options.  Returns generated options plus a flag indicating
	// if the generated options contain disallowed ones.
	pickOpts := func(num int, allowed map[string]struct{}) (map[string]string, bool) {
		opts := make(map[string]string, num)
		haveDisallowed := false
		var picks []string
		if rand.Intn(10) > 5 {
			for opt := range allOpts {
				picks = append(picks, opt)
			}
		} else {
			for opt := range allowed {
				picks = append(picks, opt)
			}
		}
		require.NotNil(t, picks)

		for i := 0; i < num; i++ {
			pick := picks[rand.Intn(len(picks))]
			_, allowed := allowed[pick]
			if !allowed {
				_, allowed = allowedCommonOptions[pick]
			}
			if allowed {
				opts[pick] = "ok"
			} else {
				opts[pick] = "bad"
				haveDisallowed = true
			}
		}

		return opts, haveDisallowed
	}

	tests := []struct {
		format  string
		allowed map[string]struct{}
	}{
		{"avro", avroAllowedOptions},
		{"csv", csvAllowedOptions},
		{"mysqouout", mysqlOutAllowedOptions},
		{"mysqldump", mysqlDumpAllowedOptions},
		{"pgdump", pgDumpAllowedOptions},
		{"pgcopy", pgCopyAllowedOptions},
	}

	for _, tc := range tests {
		for i := 0; i < 5; i++ {
			opts, haveBadOptions := pickOpts(i, tc.allowed)
			t.Run(fmt.Sprintf("validate-%s-%d/badOpts=%t", tc.format, i, haveBadOptions),
				func(t *testing.T) {
					err := validateFormatOptions(tc.format, opts, tc.allowed)
					if haveBadOptions {
						require.Error(t, err, opts)
					} else {
						require.NoError(t, err, opts)
					}
				})
		}
	}
}
