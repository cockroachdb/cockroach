// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

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
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/cloud/userfile"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createAvroData(
	t *testing.T, name string, fields []map[string]interface{}, rows []map[string]interface{},
) string {
	var data bytes.Buffer
	// Set up a simple schema for the import data.
	schema := map[string]interface{}{
		"type":   "record",
		"name":   name,
		"fields": fields,
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
	for _, row := range rows {
		require.NoError(t, ocf.Append([]interface{}{row}))
	}
	// Retrieve the AVRO encoded data.
	return data.String()
}

func TestImportData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes >1min under race")
	const getTablesQuery = `
SELECT schema_name, table_name, type
FROM [SHOW TABLES]
ORDER BY table_name
`

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)

	tests := []struct {
		name      string
		create    string
		with      string
		typ       string
		data      string
		err       string
		rejected  string
		query     map[string][][]string
		skipIssue int
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
			data: `'a' collate en_u_ks_level1
'B' collate en_u_ks_level1
'c' collate en_u_ks_level1
'D' collate en_u_ks_level1
'd' collate en_u_ks_level1
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
			data:   strings.Repeat("'1' COLLATE en_u_ks_level1\n", 2000),
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
			err:    `parse error on line 1, column 3: bare " in non-quoted-field`,
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
			err:    `parse error on line 1, column 4: extraneous or missing " in quoted-field`,
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
			err:    `unsupported escape sequence: \\x`,
		},
		{
			name:   "unexpected escape 3",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\3`,
			err:    `unsupported escape sequence: \\3`,
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
			err:    "out of range: 10000000000",
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
			err: `targeted column "s" not found`,
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
			err:  "out of range: 10000000000",
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
			err: `targeted column "c" not found`,
		},
		{
			name: "out-of-order and omitted COPY columns",
			typ:  "PGDUMP",
			data: `
CREATE TABLE "public"."tbl" ("a" int primary key, "B" string, "c" int, d int DEFAULT 6);
COPY "public"."tbl" (c, "a", "B") FROM STDIN;
5	1	carrot
9	3	mango
\.
END;
			`,
			query: map[string][][]string{
				`SELECT a, "B", c, d FROM tbl`: {
					{"1", "carrot", "5", "6"},
					{"3", "mango", "9", "6"},
				},
			},
		},
		{
			name: "fk",
			typ:  "PGDUMP",
			data: testPgdumpFk,
			with: "WITH ignore_unsupported_statements",
			query: map[string][][]string{
				getTablesQuery: {
					{"public", "cities", "table"},
					{"public", "weather", "table"},
				},
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
				`: {{"weather", "weather_city_fkey", "FOREIGN KEY", "FOREIGN KEY (city) REFERENCES cities(city) NOT VALID", "false"}},
			},
		},
		{
			name: "fk-circular",
			typ:  "PGDUMP",
			data: testPgdumpFkCircular,
			query: map[string][][]string{
				getTablesQuery: {
					{"public", "a", "table"},
					{"public", "b", "table"},
				},
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
					`CREATE TABLE public.a (
	i INT8 NOT NULL,
	k INT8 NULL,
	CONSTRAINT a_pkey PRIMARY KEY (i ASC),
	CONSTRAINT a_i_fkey FOREIGN KEY (i) REFERENCES public.b(j) NOT VALID,
	CONSTRAINT a_k_fkey FOREIGN KEY (k) REFERENCES public.a(i) NOT VALID
)`,
				}, {
					`CREATE TABLE public.b (
	j INT8 NOT NULL,
	CONSTRAINT b_pkey PRIMARY KEY (j ASC),
	CONSTRAINT b_j_fkey FOREIGN KEY (j) REFERENCES public.a(i) NOT VALID
)`,
				}},

				`SHOW CONSTRAINTS FROM a`: {
					{"a", "a_i_fkey", "FOREIGN KEY", "FOREIGN KEY (i) REFERENCES b(j) NOT VALID", "false"},
					{"a", "a_k_fkey", "FOREIGN KEY", "FOREIGN KEY (k) REFERENCES a(i) NOT VALID", "false"},
					{"a", "a_pkey", "PRIMARY KEY", "PRIMARY KEY (i ASC)", "true"},
				},
				`SHOW CONSTRAINTS FROM b`: {
					{"b", "b_j_fkey", "FOREIGN KEY", "FOREIGN KEY (j) REFERENCES a(i) NOT VALID", "false"},
					{"b", "b_pkey", "PRIMARY KEY", "PRIMARY KEY (j ASC)", "true"},
				},
			},
		},
		{
			name: "fk-skip",
			typ:  "PGDUMP",
			data: testPgdumpFk,
			with: `WITH skip_foreign_keys, ignore_unsupported_statements`,
			query: map[string][][]string{
				getTablesQuery: {
					{"public", "cities", "table"},
					{"public", "weather", "table"},
				},
				// Verify the constraint is skipped.
				`SELECT dependson_name FROM crdb_internal.backward_dependencies`: {},
				`SHOW CONSTRAINTS FROM weather`:                                  {},
			},
		},
		{
			name: "fk unreferenced",
			typ:  "TABLE weather FROM PGDUMP",
			data: testPgdumpFk,
			with: "WITH ignore_unsupported_statements",
			err:  `table "public.cities" not found`,
		},
		{
			name: "fk unreferenced skipped",
			typ:  "TABLE weather FROM PGDUMP",
			data: testPgdumpFk,
			with: `WITH skip_foreign_keys, ignore_unsupported_statements`,
			query: map[string][][]string{
				getTablesQuery: {{"public", "weather", "table"}},
			},
		},
		{
			name: "case sensitive table names",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t ("sPoNgE" int8);
				INSERT INTO t ("sPoNgE") VALUES (1337);
			`,
			query: map[string][][]string{
				`SELECT * from t`: {{"1337"}},
			},
		},
		{
			name: "sequence",
			typ:  "PGDUMP",
			with: "WITH ignore_unsupported_statements",
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
				`SHOW CREATE SEQUENCE i_seq`: {{"i_seq", "CREATE SEQUENCE public.i_seq MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1"}},
			},
		},
		{
			name: "INSERT without specifying all column values",
			typ:  "PGDUMP",
			data: `
					SET standard_conforming_strings = OFF;
					BEGIN;
					CREATE TABLE "bob" ("a" int, "b" int, c int default 2);
					INSERT INTO "bob" ("a") VALUES (1), (5);
					INSERT INTO "bob" ("c", "b") VALUES (3, 2);
					COMMIT
			`,
			with: `WITH ignore_unsupported_statements`,
			query: map[string][][]string{
				`SELECT * FROM bob`: {
					{"1", "NULL", "2"},
					{"5", "NULL", "2"},
					{"NULL", "2", "3"},
				},
			},
		},
		{
			name: "ALTER COLUMN x SET NOT NULL",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (a INT8 PRIMARY KEY, b INT8);
				ALTER TABLE t ALTER COLUMN b SET NOT NULL;
			`,
			query: map[string][][]string{
				`SHOW CREATE TABLE t`: {
					{
						"t",
						`CREATE TABLE public.t (
	a INT8 NOT NULL,
	b INT8 NOT NULL,
	CONSTRAINT t_pkey PRIMARY KEY (a ASC)
)`,
					},
				},
			},
		},
		{
			name: "ALTER COLUMN x SET VISIBLE",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (a INT8 PRIMARY KEY, b INT8, c INT8 NOT VISIBLE);
				ALTER TABLE t ALTER COLUMN c SET VISIBLE;
				ALTER TABLE t ALTER COLUMN b SET NOT VISIBLE;
			`,
			query: map[string][][]string{
				`SHOW CREATE TABLE t`: {
					{
						"t",
						`CREATE TABLE public.t (
	a INT8 NOT NULL,
	b INT8 NOT VISIBLE NULL,
	c INT8 NULL,
	CONSTRAINT t_pkey PRIMARY KEY (a ASC)
)`,
					},
				},
			},
		},
		{
			name: "ALTER COLUMN x SET DEFAULT",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (a INT8 PRIMARY KEY, b INT8);
				ALTER TABLE t ALTER COLUMN b SET DEFAULT 8;
			`,
			query: map[string][][]string{
				`SHOW CREATE TABLE t`: {
					{
						"t",
						`CREATE TABLE public.t (
	a INT8 NOT NULL,
	b INT8 NULL DEFAULT 8:::INT8,
	CONSTRAINT t_pkey PRIMARY KEY (a ASC)
)`,
					},
				},
			},
		},
		{
			name: "non-public schema",
			typ:  "PGDUMP",
			data: `
        create schema s;
        create table s.t (i INT8)`,
			query: map[string][][]string{
				getTablesQuery: {{"s", "t", "table"}},
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
		{
			name: "partial index",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);
CREATE INDEX i ON t USING btree (a) WHERE (b > 10);
			`,
			err: "cannot import a table with partial indexes",
		},
		{
			name: "user defined type",
			typ:  "PGDUMP",
			data: `
CREATE TYPE duration AS ENUM (
    'YESTERDAY',
    'LAST_7_DAYS',
    'LAST_28_DAYS',
    'LAST_90_DAYS',
    'LAST_365_DAYS',
    'LIFE_TIME'
);
CREATE TABLE t (a duration);
			`,
			err: "IMPORT PGDUMP does not support user defined types",
		},
		{
			name: "user defined type without create",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a duration);
			`,
			err: "type \"duration\" does not exist",
		},

		// Error
		{
			name:   "unsupported import format",
			create: `b bytes`,
			typ:    "NOPE",
			err:    `unsupported import format`,
		},
		{
			name: "statistics collection",
			typ:  "PGDUMP",
			data: func() string {
				var sb strings.Builder
				_, err := fmt.Fprint(&sb, `
CREATE TABLE t (a INT);
COPY public.t (a) FROM stdin;
1
2
3
4
5
6
7
8
9
10
\.
`)
				require.NoError(t, err)
				return sb.String()
			}(),
			query: map[string][][]string{
				"SELECT column_names, row_count, distinct_count, null_count " +
					"FROM [SHOW STATISTICS FOR TABLE t] " +
					"WHERE statistics_name = '__import__' " +
					"ORDER BY column_names": {
					{"{a}", "10", "1", "0"},
					{"{rowid}", "10", "1", "0"},
				},
			},
		},
		{
			name: "statistics collection multi",
			typ:  "PGDUMP",
			data: func() string {
				var sb strings.Builder
				_, err := fmt.Fprint(&sb, `
CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX(b, a));
COPY public.t (a, b) FROM stdin;
1	1
2	2
3	3
4	4
5	5
6	6
7	7
8	8
9	9
10	10
\.
`)
				require.NoError(t, err)
				return sb.String()
			}(),
			query: map[string][][]string{
				"SELECT column_names, row_count, distinct_count, null_count " +
					"FROM [SHOW STATISTICS FOR TABLE t] " +
					"WHERE statistics_name = '__import__' " +
					"ORDER BY column_names": {
					{"{a}", "10", "1", "0"},
					{"{b}", "10", "1", "0"},
				},
			},
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
				if tc.skipIssue != 0 {
					skip.WithIssue(t, tc.skipIssue)
					return
				}
				dbName := fmt.Sprintf("d%d", i)
				sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s; USE %[1]s`, dbName))
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP DATABASE %s`, dbName))
				var q string
				if tc.create != "" {
					sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (%s)`, tc.create))
					q = fmt.Sprintf(`IMPORT INTO t %s DATA ($1) %s`, tc.typ, tc.with)
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
		sqlDB.Exec(t, `CREATE TABLE t (s STRING)`)
		sqlDB.Exec(t, `IMPORT INTO t DELIMITED DATA ($1, $1)`, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1"}, {"1"}})
	})
}

func TestImportIntoUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := serverutils.StartNewTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)
	// Set up some initial state for the tests.
	sqlDB.Exec(t, `CREATE TYPE greeting AS ENUM ('hello', 'hi')`)

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
		errString   string
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
		// Test CSV default and computed column imports.
		{
			create: `
a greeting, b greeting default 'hi', c greeting
AS (
CASE a
WHEN 'hello' THEN 'hi'
WHEN 'hi' THEN 'hello'
END
) STORED`,
			intoCols:    "a",
			typ:         "CSV",
			contents:    "hello\nhi\n",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hi", "hi"}, {"hi", "hi", "hello"}},
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
		// Test AVRO default and computed column imports.
		{
			create: `
a greeting, b greeting, c greeting
AS (
CASE a
WHEN 'hello' THEN 'hi'
WHEN 'hi' THEN 'hello'
END
) STORED`,
			intoCols:    "a, b",
			typ:         "AVRO",
			contents:    avroData,
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello", "hi"}, {"hi", "hi", "hello"}},
		},
		// Test DELIMITED imports.
		{
			create:      "a greeting, b greeting",
			intoCols:    "a, b",
			typ:         "DELIMITED",
			contents:    "hello\thello\nhi\thi\n",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello"}, {"hi", "hi"}},
		},
		// Test DELIMITED default and computed column imports.
		{
			create: `
a greeting, b greeting default 'hi', c greeting
AS (
CASE a
WHEN 'hello' THEN 'hi'
WHEN 'hi' THEN 'hello'
END
) STORED`,
			intoCols:    "a",
			typ:         "DELIMITED",
			contents:    "hello\nhi\n",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hi", "hi"}, {"hi", "hi", "hello"}},
		},
		// Test PGCOPY imports.
		{
			create:      "a greeting, b greeting",
			intoCols:    "a, b",
			typ:         "PGCOPY",
			contents:    "hello\thello\nhi\thi\n",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hello"}, {"hi", "hi"}},
		},
		// Test PGCOPY default and computed column imports.
		{
			create: `
a greeting, b greeting default 'hi', c greeting
AS (
CASE a
WHEN 'hello' THEN 'hi'
WHEN 'hi' THEN 'hello'
END
) STORED`,
			intoCols:    "a",
			typ:         "PGCOPY",
			contents:    "hello\nhi\n",
			verifyQuery: "SELECT * FROM t ORDER BY a",
			expected:    [][]string{{"hello", "hi", "hi"}, {"hi", "hi", "hello"}},
		},
		// Test table with an invalid enum value.
		{
			create:    "a greeting",
			intoCols:  "a",
			typ:       "PGCOPY",
			contents:  "randomvalue\n",
			errString: "invalid input value for enum greeting",
		},
	}

	// Test IMPORT INTO.
	for _, test := range tests {
		// Write the test data into a file.
		f, err := ioutil.TempFile(baseDir, "data")
		require.NoError(t, err)
		n, err := f.Write([]byte(test.contents))
		require.NoError(t, err)
		require.Equal(t, len(test.contents), n)
		// Run the import statement.
		sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE t (%s)", test.create))

		importStmt := fmt.Sprintf("IMPORT INTO t (%s) %s DATA ($1)", test.intoCols, test.typ)
		importArgs := fmt.Sprintf("nodelocal://0/%s", filepath.Base(f.Name()))

		if test.errString == "" {
			sqlDB.Exec(t, importStmt, importArgs)
			// Ensure that the table data is as we expect.
			sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)
		} else {
			sqlDB.ExpectErr(t, test.errString, importStmt, importArgs)
		}

		// Clean up after the test.
		sqlDB.Exec(t, "DROP TABLE t")
	}
}

const (
	testPgdumpCreateCities = `CREATE TABLE public.cities (
	city VARCHAR(80) NOT NULL,
	CONSTRAINT cities_pkey PRIMARY KEY (city ASC)
)`
	testPgdumpCreateWeather = `CREATE TABLE public.weather (
	city VARCHAR(80) NULL,
	temp_lo INT8 NULL,
	temp_hi INT8 NULL,
	prcp FLOAT4 NULL,
	date DATE NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT weather_pkey PRIMARY KEY (rowid ASC),
	CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES public.cities(city) NOT VALID
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

func TestImportRowLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	avroField := []map[string]interface{}{
		{
			"name": "a",
			"type": "int",
		},
		{
			"name": "b",
			"type": "int",
		},
	}
	avroRows := []map[string]interface{}{
		{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6},
	}
	avroData := createAvroData(t, "t", avroField, avroRows)

	tests := []struct {
		name        string
		create      string
		typ         string
		with        string
		data        string
		verifyQuery string
		err         string
		expected    [][]string
	}{
		// Test CSV imports.
		{
			name:        "skip 1 row and limit 1 row",
			create:      `a string, b string`,
			with:        `WITH row_limit = '1', skip='1'`,
			typ:         "CSV",
			data:        "a string, b string\nfoo,normal\nbar,baz\nchocolate,cake\n",
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"foo", "normal"}},
		},
		{
			name:        "row limit 0",
			create:      `a string, b string`,
			with:        `WITH row_limit = '0', skip='1'`,
			typ:         "CSV",
			data:        "a string, b string\nfoo,normal\nbar,baz\nchocolate,cake\n",
			verifyQuery: `SELECT * from t`,
			err:         "pq: row_limit must be > 0",
		},
		{
			name:        "row limit negative",
			create:      `a string, b string`,
			with:        `WITH row_limit = '-5', skip='1'`,
			typ:         "CSV",
			data:        "a string, b string\nfoo,normal\nbar,baz\nchocolate,cake\n",
			verifyQuery: `SELECT * from t`,
			err:         "pq: row_limit must be > 0",
		},
		{
			name:        "invalid row limit",
			create:      `a string, b string`,
			with:        `WITH row_limit = 'abc', skip='1'`,
			typ:         "CSV",
			data:        "a string, b string\nfoo,normal\nbar,baz\nchocolate,cake\n",
			verifyQuery: `SELECT * from t`,
			err:         "invalid numeric row_limit value",
		},
		{
			name:        "row limit > max rows",
			create:      `a string, b string`,
			with:        `WITH row_limit = '13', skip='1'`,
			typ:         "CSV",
			data:        "a string, b string\nfoo,normal\nbar,baz\nchocolate,cake\n",
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"foo", "normal"}, {"bar", "baz"}, {"chocolate", "cake"}},
		},
		// Test DELIMITED imports.
		{
			name:        "tsv row limit",
			create:      "a string, b string",
			with:        `WITH row_limit = '1', skip='1'`,
			typ:         "DELIMITED",
			data:        "hello\thello\navocado\ttoast\npoached\tegg\n",
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"avocado", "toast"}},
		},
		{
			name:        "tsv invalid row limit",
			create:      `a string, b string`,
			with:        `WITH row_limit = 'potato', skip='1'`,
			typ:         "DELIMITED",
			data:        "hello\thello\navocado\ttoast\npoached\tegg\n",
			verifyQuery: `SELECT * from t`,
			err:         "invalid numeric row_limit value",
		},
		// Test AVRO imports.
		{
			name:        "avro row limit",
			create:      "a INT, b INT",
			with:        `WITH row_limit = '1'`,
			typ:         "AVRO",
			data:        avroData,
			verifyQuery: "SELECT * FROM t",
			expected:    [][]string{{"1", "2"}},
		},
		{
			name:        "avro invalid row limit",
			create:      "a INT, b INT",
			with:        `WITH row_limit = 'potato'`,
			typ:         "AVRO",
			data:        avroData,
			verifyQuery: `SELECT * from t`,
			err:         "invalid numeric row_limit value",
		},
		// Test PGDump imports.
		{
			name: "pgdump single table with insert",
			typ:  "PGDUMP",
			data: `CREATE TABLE t (a INT, b INT);
				INSERT INTO t (a, b) VALUES (1, 2), (3, 4);
				`,
			with:        `WITH row_limit = '1'`,
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"1", "2"}},
		},
		{
			name: "pgdump multiple inserts same table",
			typ:  "PGDUMP",
			data: `CREATE TABLE t (a INT, b INT);
				INSERT INTO t (a, b) VALUES (1, 2);
				INSERT INTO t (a, b) VALUES (3, 4);
				INSERT INTO t (a, b) VALUES (5, 6);
				INSERT INTO t (a, b) VALUES (7, 8);
				`,
			with:        `WITH row_limit = '2'`,
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"1", "2"}, {"3", "4"}},
		},
		// Test Mysql imports.
		{
			name: "mysqldump single table",
			typ:  "MYSQLDUMP",
			data: `CREATE TABLE t (a INT, b INT);
				INSERT INTO t (a, b) VALUES (5, 6), (7, 8);
				`,
			with:        `WITH row_limit = '1'`,
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"5", "6"}},
		},
		{
			name: "mysqldump multiple inserts same table",
			typ:  "MYSQLDUMP",
			data: `CREATE TABLE t (a INT, b INT);
				INSERT INTO t (a, b) VALUES (1, 2);
				INSERT INTO t (a, b) VALUES (3, 4);
				INSERT INTO t (a, b) VALUES (5, 6);
				INSERT INTO t (a, b) VALUES (7, 8);
				`,
			with:        `WITH row_limit = '2'`,
			verifyQuery: `SELECT * from t`,
			expected:    [][]string{{"1", "2"}, {"3", "4"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer sqlDB.Exec(t, `DROP TABLE IF EXISTS t`)
			data = test.data
			importIntoQuery := fmt.Sprintf(`IMPORT INTO t %s DATA ($1) %s`, test.typ, test.with)

			if test.err != "" {
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (%s)`, test.create))
				sqlDB.ExpectErr(t, test.err, importIntoQuery, srv.URL)
			} else {
				if test.typ == "CSV" || test.typ == "AVRO" || test.typ == "DELIMITED" {
					sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (%s)`, test.create))
					sqlDB.Exec(t, importIntoQuery, srv.URL)

					// Ensure that the table data is as we expect.
					sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)

				} else if test.typ == "PGDUMP" || test.typ == "MYSQLDUMP" {
					sqlDB.Exec(t, `DROP TABLE IF EXISTS t, u`)

					// Import table from dump format.
					importDumpQuery := fmt.Sprintf(`IMPORT TABLE t FROM %s ($1) %s`, test.typ, test.with)
					sqlDB.Exec(t, importDumpQuery, srv.URL)
					sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)

					sqlDB.Exec(t, `DROP TABLE t`)

					// Import dump format directly.
					importDumpQuery = fmt.Sprintf(`IMPORT %s ($1) %s`, test.typ, test.with)
					sqlDB.Exec(t, importDumpQuery, srv.URL)
					sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)

					sqlDB.Exec(t, `DROP TABLE t`)
				}
			}
		})
	}

	t.Run("pgdump multitable", func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second, seqtable CASCADE`)
		sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS a_seq`)

		// Data is populated from files using COPY FROM STDIN statement.
		_, secondFile := getSecondPostgresDumpTestdata(t)
		second := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(secondFile, baseDir))}
		multitableFile := getMultiTablePostgresDumpTestdata(t)
		multitable := []interface{}{fmt.Sprintf("nodelocal://0/%s", strings.TrimPrefix(multitableFile, baseDir))}
		expectedRowLimit := 4

		// Import a single table `second` and verify number of rows imported.
		importQuery := fmt.Sprintf(`IMPORT TABLE second FROM PGDUMP ($1) WITH row_limit="%d",ignore_unsupported_statements`,
			expectedRowLimit)
		sqlDB.Exec(t, importQuery, second...)

		var numRows int
		sqlDB.QueryRow(t, "SELECT count(*) FROM second").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)

		sqlDB.Exec(t, `DROP TABLE IF EXISTS second`)

		// Import multiple tables including `simple` and `second`.
		expectedRowLimit = 3
		importQuery = fmt.Sprintf(`IMPORT PGDUMP ($1) WITH row_limit="%d",ignore_unsupported_statements`, expectedRowLimit)
		sqlDB.Exec(t, importQuery, multitable...)
		sqlDB.QueryRow(t, "SELECT count(*) FROM second").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)

		sqlDB.QueryRow(t, "SELECT count(*) FROM second").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)
		sqlDB.Exec(t, `DROP TABLE simple, second, seqtable CASCADE`)
		sqlDB.Exec(t, `DROP SEQUENCE a_seq`)
	})

	t.Run("pgdump multitable interleaved inserts", func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS t, u`)

		var numRows int
		expectedRowLimit := 1
		data = `CREATE TABLE t (a INT, b INT);
				CREATE TABLE u (a INT);
				INSERT INTO t (a, b) VALUES (1, 2);
				INSERT INTO u (a) VALUES (100);
				INSERT INTO t (a, b) VALUES (7, 8);
				INSERT INTO u (a) VALUES (600);`

		importDumpQuery := fmt.Sprintf(`IMPORT PGDUMP ($1) WITH row_limit="%d"`, expectedRowLimit)
		sqlDB.Exec(t, importDumpQuery, srv.URL)

		// Verify expectedRowLimit number of rows were imported from t, u
		sqlDB.QueryRow(t, "SELECT count(*) FROM t").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)

		sqlDB.QueryRow(t, "SELECT count(*) FROM u").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)
	})

	t.Run("mysqldump multitable", func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second, third, everything CASCADE`)
		sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS simple_auto_inc, third_auto_inc`)

		var numRows int

		files := getMysqldumpTestdata(t)
		simpleMysql := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.simple, baseDir))}
		multitableMysql := []interface{}{fmt.Sprintf("nodelocal://0%s", strings.TrimPrefix(files.wholeDB, baseDir))}
		expectedRowLimit := 2

		// single table
		importQuery := fmt.Sprintf(`IMPORT TABLE simple FROM MYSQLDUMP ($1) WITH row_limit="%d"`, expectedRowLimit)
		sqlDB.Exec(t, importQuery, simpleMysql...)
		sqlDB.QueryRow(t, "SELECT count(*) FROM simple").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)

		sqlDB.Exec(t, `DROP TABLE IF EXISTS simple`)
		sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS simple_auto_inc`)

		// multiple tables
		importQuery = fmt.Sprintf(`IMPORT MYSQLDUMP ($1) WITH row_limit="%d"`, expectedRowLimit)
		sqlDB.Exec(t, importQuery, multitableMysql...)

		sqlDB.QueryRow(t, "SELECT count(*) FROM second").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)

		sqlDB.QueryRow(t, "SELECT count(*) FROM simple").Scan(&numRows)
		require.Equal(t, expectedRowLimit, numRows)
	})

	t.Run("row limit multiple csv", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE test; USE test`)
		defer sqlDB.Exec(t, (`DROP DATABASE test`))

		data = "pear\navocado\nwatermelon\nsugar"
		sqlDB.Exec(t, `CREATE TABLE t (s STRING)`)
		sqlDB.Exec(t, `IMPORT INTO t CSV DATA ($1, $2) WITH row_limit='2'`,
			srv.URL, srv.URL)

		sqlDB.CheckQueryResults(t, `SELECT * FROM t`,
			[][]string{{"pear"}, {"avocado"}, {"pear"}, {"avocado"}})

		sqlDB.Exec(t, "DROP TABLE t")
	})
}

func TestFailedImportGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodes = 3

	var forceFailure bool
	blockGC := make(chan struct{})

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "pgdump")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		SQLMemoryPoolSize: 256 << 20,
		ExternalIODir:     baseDir,
		Knobs: base.TestingKnobs{
			GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { <-blockGC; return nil }},
		},
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	for i := 0; i < tc.NumServers(); i++ {
		tc.Server(i).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*importResumer)
				r.testingKnobs.afterImport = func(_ roachpb.RowCount) error {
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
		fmt.Sprintf(`IMPORT TABLE simple FROM PGDUMP ('%s') WITH ignore_unsupported_statements`, "nodelocal://1/simple.sql"),
	)
	// Nudge the registry to quickly adopt the job.
	tc.Server(0).JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

	// In the case of the test, the ID of the table that will be cleaned up due
	// to the failed import will be two higher than the ID of the empty database
	// it was created in.
	// We increment the id once for the public schema and a second time for the
	// "MakeSimpleTableDescriptor".
	dbID := sqlutils.QueryDatabaseID(t, sqlDB.DB, "failedimport")
	tableID := descpb.ID(dbID + 2)
	var td catalog.TableDescriptor
	if err := sql.TestingDescsTxn(ctx, tc.Server(0), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		td, err = col.Direct().MustGetTableDescByID(ctx, txn, tableID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Ensure that we have garbage written to the descriptor that we want to
	// clean up.
	tests.CheckKeyCount(t, kvDB, td.TableSpan(keys.SystemSQLCodec), 87)

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
}

// Verify that a failed import will clean up after itself. This means:
//  - Delete the garbage data that it partially imported.
//  - Delete the table descriptor for the table that was created during the
//  import.
func TestImportCSVStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderRace(t, "takes >1min under race")

	const nodes = 3

	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	var forceFailure bool

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "csv")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		SQLMemoryPoolSize: 256 << 20,
		ExternalIODir:     baseDir,
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	for i := 0; i < tc.NumServers(); i++ {
		tc.Server(i).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*importResumer)
				r.testingKnobs.afterImport = func(_ roachpb.RowCount) error {
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

	empty := []string{"'nodelocal://0/empty.csv'"}

	// Support subtests by keeping track of the number of jobs that are executed.
	testNum := -1
	expectedRows := numFiles * rowsPerFile
	for i, tc := range []struct {
		name        string
		createQuery string
		query       string // must have one `%s` for the files list.
		files       []string
		jobOpts     string
		err         string
	}{
		{
			"query-opts",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2'`,
			testFiles.filesWithOpts,
			` WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2'`,
			"",
		},
		{
			// Force some SST splits.
			"file-sstsize",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH sstsize = '10K'`,
			testFiles.files,
			` WITH sstsize = '10K'`,
			"",
		},
		{
			"empty-file",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s)`,
			empty,
			``,
			"",
		},
		{
			"empty-with-files",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s)`,
			append(empty, testFiles.files...),
			``,
			"",
		},
		{
			"auto-decompress",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.files,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"no-decompress",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.files,
			` WITH decompress = 'none'`,
			"",
		},
		{
			"explicit-gzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.gzipFiles,
			` WITH decompress = 'gzip'`,
			"",
		},
		{
			"auto-gzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"implicit-gzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s)`,
			testFiles.gzipFiles,
			``,
			"",
		},
		{
			"explicit-bzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'bzip'`,
			testFiles.bzipFiles,
			` WITH decompress = 'bzip'`,
			"",
		},
		{
			"auto-bzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"implicit-bzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s)`,
			testFiles.bzipFiles,
			``,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"bad-opt-name",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH foo = 'bar'`,
			testFiles.files,
			``,
			"invalid option \"foo\"",
		},
		{
			"primary-key-dup",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s)`,
			testFiles.filesWithDups,
			``,
			"duplicate key in primary index",
		},
		{
			"no-database",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO nonexistent.t CSV DATA (%s)`,
			testFiles.files,
			``,
			`database does not exist: "nonexistent.t"`,
		},
		{
			"into-db-fails",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH into_db = 'test'`,
			testFiles.files,
			``,
			`invalid option "into_db"`,
		},
		{
			"no-decompress-gzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.gzipFiles,
			` WITH decompress = 'none'`,
			// This returns different errors for `make test` and `make testrace` but
			// field is in both error messages.
			`field`,
		},
		{
			"decompress-gzip",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.files,
			` WITH decompress = 'gzip'`,
			"gzip: invalid header",
		},
		{
			"csv-with-invalid-delimited-option",
			`CREATE TABLE t (a int8 primary key, b string, index (b), index (a, b))`,
			`IMPORT INTO t CSV DATA (%s) WITH fields_delimited_by = '|'`,
			testFiles.files,
			``,
			"invalid option",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.name, "bzip") && len(testFiles.bzipFiles) == 0 {
				skip.IgnoreLint(t, "bzip2 not available on PATH?")
			}
			intodb := fmt.Sprintf(`csv%d`, i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, intodb))
			sqlDB.Exec(t, fmt.Sprintf(`SET DATABASE = %s`, intodb))

			var unused string
			var restored struct {
				rows, idx, bytes int
			}

			if tc.createQuery != "" {
				sqlDB.Exec(t, tc.createQuery)
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

			jobPrefix := fmt.Sprintf(`IMPORT INTO %s.public.t`, intodb)

			var intodbID descpb.ID
			sqlDB.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = '%s'`,
				intodb)).Scan(&intodbID)
			var publicSchemaID descpb.ID
			sqlDB.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = '%s' AND "parentID" = %d`,
				tree.PublicSchema, intodbID)).Scan(&publicSchemaID)
			var tableID int64
			sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE "parentID" = $1 AND "parentSchemaID" = $2`,
				intodbID, publicSchemaID).Scan(&tableID)

			if err := jobutils.VerifySystemJob(t, sqlDB, testNum, jobspb.TypeImport, jobs.StatusSucceeded, jobs.Record{
				Username:      security.RootUserName(),
				Description:   fmt.Sprintf(jobPrefix+` CSV DATA (%s)`+tc.jobOpts, strings.ReplaceAll(strings.Join(tc.files, ", "), "?AWS_SESSION_TOKEN=secrets", "?AWS_SESSION_TOKEN=redacted")),
				DescriptorIDs: []descpb.ID{descpb.ID(tableID)},
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
		sqlDB.Exec(t, `CREATE TABLE pk.t (a INT8, b STRING)`)
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO pk.t CSV DATA (%s)`, strings.Join(testFiles.files, ", ")))
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
		sqlDB.Exec(t, `CREATE TABLE t (a INT8 PRIMARY KEY, b STRING)`)

		// Specify wrong number of columns.
		sqlDB.ExpectErr(
			t, "error parsing row 1: expected 1 fields, got 2",
			fmt.Sprintf(`IMPORT INTO t (a) CSV DATA (%s)`, testFiles.files[0]),
		)

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t CSV DATA (%s)`, testFiles.files[0]))

		// A second attempt should fail fast. A "slow fail" is the error message
		// "restoring table desc and namespace entries: table already exists".
		sqlDB.ExpectErr(
			t, `ingested key collides with an existing one`,
			fmt.Sprintf(`IMPORT INTO t CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// Test basic role based access control. Users who have the admin role should
	// be able to IMPORT.
	t.Run("RBAC-SuperUser", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE USER testuser`)
		sqlDB.Exec(t, `GRANT admin TO testuser`)
		pgURL, cleanupFunc := sqlutils.PGUrl(
			t, tc.Server(0).ServingSQLAddr(), "TestImportPrivileges-testuser",
			url.User("testuser"),
		)
		defer cleanupFunc()
		testuser, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer testuser.Close()

		if _, err := testuser.Exec("CREATE TABLE rbac_into_superuser (a INT8 PRIMARY KEY, " +
			"b STRING)"); err != nil {
			t.Fatal(err)
		}
		if _, err := testuser.Exec(fmt.Sprintf(`IMPORT INTO rbac_into_superuser (a, b) CSV DATA (%s)`, testFiles.files[0])); err != nil {
			t.Fatal(err)
		}
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
			create = `CREATE TABLE t (
					a SERIAL8,
					b INT8 DEFAULT unique_rowid(),
					c STRING DEFAULT 's',
					d SERIAL8,
					e INT8 DEFAULT unique_rowid(),
					f STRING DEFAULT 's',
					PRIMARY KEY (a, b, c)
				)`
			query  = `IMPORT INTO t CSV DATA ($1)`
			nullif = ` WITH nullif=''`
		)

		sqlDB.Exec(t, create)

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

	// Test userfile import CSV.
	t.Run("userfile-simple", func(t *testing.T) {
		userfileURI := "userfile://defaultdb.public.root/test.csv"
		userfileStorage, err := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).DistSQLSrv.
			ExternalStorageFromURI(ctx, userfileURI, security.RootUserName())
		require.NoError(t, err)

		data := []byte("1,2")
		require.NoError(t, cloud.WriteFile(ctx, userfileStorage, "", bytes.NewReader(data)))

		sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY, id2 INT)`)
		sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO foo CSV DATA ('%s')", userfileURI))
		sqlDB.CheckQueryResults(t, "SELECT * FROM foo", sqlDB.QueryStr(t, "SELECT 1, 2"))

		require.NoError(t, userfileStorage.Delete(ctx, ""))
	})

	t.Run("userfile-relative-file-path", func(t *testing.T) {
		userfileURI := "userfile:///import-test/employees.csv"
		userfileStorage, err := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).DistSQLSrv.
			ExternalStorageFromURI(ctx, userfileURI, security.RootUserName())
		require.NoError(t, err)

		data := []byte("1,2")
		require.NoError(t, cloud.WriteFile(ctx, userfileStorage, "", bytes.NewReader(data)))

		sqlDB.Exec(t, `CREATE TABLE baz (id INT PRIMARY KEY, id2 INT)`)
		sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO baz CSV DATA ('%s')", userfileURI))
		sqlDB.CheckQueryResults(t, "SELECT * FROM baz", sqlDB.QueryStr(t, "SELECT 1, 2"))

		require.NoError(t, userfileStorage.Delete(ctx, ""))
	})

	t.Run("import-with-db-privs", func(t *testing.T) {
		sqlDB.Exec(t, `USE defaultdb`)
		sqlDB.Exec(t, `CREATE USER foo`)
		sqlDB.Exec(t, `GRANT ALL ON DATABASE defaultdb TO foo`)
		sqlDB.Exec(t, `CREATE TABLE import_with_db_privs (a INT8 PRIMARY KEY, b STRING)`)

		sqlDB.Exec(t, fmt.Sprintf(`
		IMPORT INTO import_with_db_privs CSV DATA (%s)`,
			testFiles.files[0]))

		// Verify correct number of rows via COUNT.
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM import_with_db_privs`).Scan(&result)
		if result != rowsPerFile {
			t.Fatalf("expected %d rows, got %d", rowsPerFile, result)
		}
	})

	t.Run("user-defined-schemas", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE uds`)
		sqlDB.Exec(t, `USE uds`)
		sqlDB.Exec(t, `CREATE SCHEMA sc`)
		// Now import into a table under sc.
		sqlDB.Exec(t, `CREATE TABLE uds.sc.t (a INT8 PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO uds.sc.t (a, b) CSV DATA (%s)`, testFiles.files[0]))
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM uds.sc.t`).Scan(&result)
		require.Equal(t, rowsPerFile, result)
	})
}

// TestImportFeatureFlag tests the feature flag logic that allows the IMPORT and
// IMPORT INTO commands to be toggled off via cluster settings.
func TestImportFeatureFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	const nodes = 1
	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "csv")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	data := `
CREATE TABLE t (id INT);
INSERT INTO foo VALUES (1);
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	testFiles := makeCSVData(t, numFiles, rowsPerFile, nodes, rowsPerRaceFile)

	// Feature flag is off — test that IMPORT and IMPORT INTO surface error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.import.enabled = FALSE`)
	sqlDB.ExpectErr(t, `feature IMPORT was disabled by the database administrator`,
		fmt.Sprintf(`IMPORT TABLE t FROM PGDUMP '%s'`, srv.URL))
	sqlDB.Exec(t, `CREATE TABLE feature_flags (a INT8 PRIMARY KEY, b STRING)`)
	sqlDB.ExpectErr(t, `feature IMPORT was disabled by the database administrator`,
		fmt.Sprintf(`IMPORT INTO feature_flags (a, b) CSV DATA (%s)`, testFiles.files[0]))

	// Feature flag is on — test that IMPORT and IMPORT INTO do not error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.import.enabled = TRUE`)
	sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE t FROM PGDUMP '%s'`, srv.URL))
	sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO feature_flags (a, b) CSV DATA (%s)`, testFiles.files[0]))
}

func TestImportObjectLevelRBAC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const nodes = 3

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "pgdump")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		ExternalIODir:     baseDir,
		SQLMemoryPoolSize: 256 << 20,
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	rootDB := sqlutils.MakeSQLRunner(conn)

	rootDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), "TestImportPrivileges-testuser",
		url.User("testuser"),
	)
	defer cleanupFunc()

	startTestUser := func() *gosql.DB {
		testuser, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return testuser
	}

	qualifiedTableName := "defaultdb.public.user_file_table_test"
	filename := "path/to/file"
	dest := userfile.MakeUserFileStorageURI(qualifiedTableName, filename)

	writeToUserfile := func(filename, data string) {
		// Write to userfile storage now that testuser has CREATE privileges.
		ie := tc.Server(0).InternalExecutor().(*sql.InternalExecutor)
		fileTableSystem1, err := cloud.ExternalStorageFromURI(ctx, dest, base.ExternalIODirConfig{},
			cluster.NoSettings, blobs.TestEmptyBlobClientFactory, security.TestUserName(), ie, tc.Server(0).DB())
		require.NoError(t, err)
		require.NoError(t, cloud.WriteFile(ctx, fileTableSystem1, filename, bytes.NewReader([]byte(data))))
	}

	t.Run("import-RBAC", func(t *testing.T) {
		userFileDest := dest + "/" + t.Name()
		testuser := startTestUser()

		// User has no privileges at this point. Check that an IMPORT requires
		// CREATE privileges on the database.
		_, err := testuser.Exec(fmt.Sprintf(`IMPORT TABLE simple FROM PGDUMP '%s'`, userFileDest))
		require.True(t, testutils.IsError(err, "testuser does not have CREATE privilege on database"))

		// Grant user CREATE privilege on the database.
		rootDB.Exec(t, `GRANT create ON DATABASE defaultdb TO testuser`)
		// Reopen testuser sql connection.
		// TODO(adityamaru): The above GRANT does not reflect unless we restart
		// the testuser SQL connection, understand why.
		require.NoError(t, testuser.Close())

		testuser = startTestUser()
		defer testuser.Close()

		// Write to userfile now that the user has CREATE privileges.
		writeToUserfile(t.Name(), `
CREATE TABLE simple (id INT);
`)

		// Import should now have the required privileges to start the job.
		_, err = testuser.Exec(fmt.Sprintf(`IMPORT TABLE simple FROM PGDUMP '%s'`, userFileDest))
		require.NoError(t, err)
	})

	t.Run("import-into-RBAC", func(t *testing.T) {
		// Create table to IMPORT INTO.
		rootDB.Exec(t, `CREATE TABLE rbac_import_into_priv (a INT8 PRIMARY KEY, b STRING)`)
		userFileDest := dest + "/" + t.Name()
		testuser := startTestUser()

		// User has no privileges at this point. Check that an IMPORT INTO requires
		// INSERT and DROP privileges.
		for _, privilege := range []string{"INSERT", "DROP"} {
			_, err := testuser.Exec(fmt.Sprintf(`IMPORT INTO rbac_import_into_priv (a,
b) CSV DATA ('%s')`, userFileDest))
			require.True(t, testutils.IsError(err,
				fmt.Sprintf("user testuser does not have %s privilege on relation rbac_import_into_priv",
					privilege)))

			rootDB.Exec(t, fmt.Sprintf(`GRANT %s ON TABLE rbac_import_into_priv TO testuser`, privilege))
		}

		// Grant user CREATE privilege on the database.
		rootDB.Exec(t, `GRANT create ON DATABASE defaultdb TO testuser`)
		// Reopen testuser sql connection.
		// TODO(adityamaru): The above GRANT does not reflect unless we restart
		// the testuser SQL connection, understand why.
		require.NoError(t, testuser.Close())
		testuser = startTestUser()
		defer testuser.Close()

		// Write to userfile now that the user has CREATE privileges.
		writeToUserfile(t.Name(), "1,aaa")

		// Import should now have the required privileges to start the job.
		_, err := testuser.Exec(fmt.Sprintf(`IMPORT INTO rbac_import_into_priv (a,b) CSV DATA ('%s')`,
			userFileDest))
		require.NoError(t, err)
	})
}

// TestURIRequiresAdminRole tests the IMPORT logic which guards certain
// privileged ExternalStorage IO paths with an admin only check.
func TestURIRequiresAdminRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const nodes = 3

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		SQLMemoryPoolSize: 256 << 20,
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	rootDB := sqlutils.MakeSQLRunner(conn)

	rootDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), "TestImportPrivileges-testuser",
		url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer testuser.Close()
	rootDB.Exec(t, `CREATE TABLE foo (id INT)`)

	for _, tc := range []struct {
		name          string
		uri           string
		requiresAdmin bool
	}{
		{
			name:          "s3-implicit",
			uri:           "s3://foo/bar?AUTH=implicit",
			requiresAdmin: true,
		},
		{
			name:          "s3-specified",
			uri:           "s3://foo/bar?AUTH=specified",
			requiresAdmin: false,
		},
		{
			name:          "s3-custom",
			uri:           "s3://foo/bar?AUTH=specified&AWS_ENDPOINT=baz",
			requiresAdmin: true,
		},
		{
			name:          "gs-implicit",
			uri:           "gs://foo/bar?AUTH=implicit",
			requiresAdmin: true,
		},
		{
			name:          "gs-specified",
			uri:           "gs://foo/bar?AUTH=specified",
			requiresAdmin: false,
		},
		{
			name:          "userfile",
			uri:           "userfile:///foo",
			requiresAdmin: false,
		},
		{
			name:          "nodelocal",
			uri:           "nodelocal://self/foo",
			requiresAdmin: true,
		},
		{
			name:          "http",
			uri:           "http://foo/bar",
			requiresAdmin: true,
		},
		{
			name:          "https",
			uri:           "https://foo/bar",
			requiresAdmin: true,
		},
	} {
		t.Run(tc.name+"-via-import", func(t *testing.T) {
			_, err := testuser.Exec(fmt.Sprintf(`IMPORT INTO foo CSV DATA ('%s')`, tc.uri))
			if tc.requiresAdmin {
				require.True(t, testutils.IsError(err, "only users with the admin role are allowed to IMPORT"))
			} else {
				require.False(t, testutils.IsError(err, "only users with the admin role are allowed to IMPORT"))
			}
		})

		t.Run(tc.name+"-direct", func(t *testing.T) {
			conf, err := cloud.ExternalStorageConfFromURI(tc.uri, security.RootUserName())
			require.NoError(t, err)
			require.Equal(t, conf.AccessIsWithExplicitAuth(), !tc.requiresAdmin)
		})
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := serverutils.StartNewTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.ServerConn(0)
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
							CREATE TABLE t (x TEXT[]);
							IMPORT INTO t CSV DATA ('nodelocal://0/%[1]s/export*-n*.0.csv')`,
			tbl:      "t",
			expected: `SELECT ARRAY['a', 'b', 'c']`,
		},
		{
			stmts: `EXPORT INTO CSV 'nodelocal://0/%[1]s' FROM SELECT ARRAY[b'abc', b'\141\142\143', b'\x61\x62\x63'];
							CREATE TABLE t (x BYTES[]);
							IMPORT INTO t CSV DATA ('nodelocal://0/%[1]s/export*-n*.0.csv')`,
			tbl:      "t",
			expected: `SELECT ARRAY[b'abc', b'\141\142\143', b'\x61\x62\x63']`,
		},
		{
			stmts: `EXPORT INTO CSV 'nodelocal://0/%[1]s' FROM SELECT 'dog' COLLATE en;
							CREATE TABLE t (x STRING COLLATE en);
							IMPORT INTO t CSV DATA ('nodelocal://0/%[1]s/export*-n*.0.csv')`,
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
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderRace(t, "takes >1min under race")

	const nodes = 3

	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "csv")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	var forceFailure bool
	var importBodyFinished chan struct{}
	var delayImportFinish chan struct{}

	for i := 0; i < tc.NumServers(); i++ {
		tc.Server(i).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*importResumer)
				r.testingKnobs.afterImport = func(_ roachpb.RowCount) error {
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
			`pq: no files matched`,
		},
		{
			"import-into-no-glob-wildcard",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH disable_glob_matching`,
			testFiles.filesUsingWildcard,
			` WITH disable_glob_matching`,
			"pq: (.+)no such file or directory: nodelocal storage file does not exist:",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.name, "bzip") && len(testFiles.bzipFiles) == 0 {
				skip.IgnoreLint(t, "bzip2 not available on PATH?")
			}
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)
			defer sqlDB.Exec(t, `DROP TABLE t`)

			var tableID int64
			sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 't'`).Scan(&tableID)

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

			jobPrefix := `IMPORT INTO defaultdb.public.t(a, b)`
			if err := jobutils.VerifySystemJob(t, sqlDB, testNum, jobspb.TypeImport, jobs.StatusSucceeded, jobs.Record{
				Username:      security.RootUserName(),
				Description:   fmt.Sprintf(jobPrefix+` CSV DATA (%s)`+tc.jobOpts, strings.ReplaceAll(strings.Join(tc.files, ", "), "?AWS_SESSION_TOKEN=secrets", "?AWS_SESSION_TOKEN=redacted")),
				DescriptorIDs: []descpb.ID{descpb.ID(tableID)},
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

		var jobID int
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			defer close(importBodyFinished)
			return sqlDB.DB.QueryRowContext(ctx, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`,
				testFiles.files[1])).Scan(&jobID, &unused, &unused, &unused, &unused, &unused)
		})
		g.GoCtx(func(ctx context.Context) error {
			defer close(delayImportFinish)
			<-importBodyFinished

			err := sqlDB.DB.QueryRowContext(ctx, `SELECT 1 FROM t`).Scan(&unused)
			if !testutils.IsError(err, `relation "t" is offline: importing`) {
				return err
			}
			return nil
		})
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		waitForJobResult(t, tc, jobspb.JobID(jobID), jobs.StatusSucceeded)

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
		sqlDB.QueryRow(t, `SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'check_a'`).Scan(&checkValidated)
		sqlDB.QueryRow(t, `SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'fk_ref'`).Scan(&fkValidated)

		// Prior to import all constraints should be validated.
		if !checkValidated || !fkValidated {
			t.Fatal("Constraints not validated on creation.\n")
		}

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]))

		sqlDB.QueryRow(t, `SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'check_a'`).Scan(&checkValidated)
		sqlDB.QueryRow(t, `SELECT validated from [SHOW CONSTRAINT FROM t] WHERE constraint_name = 'fk_ref'`).Scan(&fkValidated)

		// Following an import the constraints should be unvalidated.
		if checkValidated || fkValidated {
			t.Fatal("FK and CHECK constraints not unvalidated after IMPORT INTO\n")
		}
	})

	// Test userfile IMPORT INTO CSV.
	t.Run("import-into-userfile-simple", func(t *testing.T) {
		userfileURI := "userfile://defaultdb.public.root/test.csv"
		userfileStorage, err := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).DistSQLSrv.
			ExternalStorageFromURI(ctx, userfileURI, security.RootUserName())
		require.NoError(t, err)

		data := []byte("1,2")
		require.NoError(t, cloud.WriteFile(ctx, userfileStorage, "", bytes.NewReader(data)))

		sqlDB.Exec(t, "CREATE TABLE foo (id INT PRIMARY KEY, id2 INT)")
		sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO foo (id, id2) CSV DATA ('%s')", userfileURI))
		sqlDB.CheckQueryResults(t, "SELECT * FROM foo", sqlDB.QueryStr(t, "SELECT 1, 2"))

		require.NoError(t, userfileStorage.Delete(ctx, ""))
	})

	t.Run("import-into-with-db-privs", func(t *testing.T) {
		sqlDB.Exec(t, `USE defaultdb`)
		sqlDB.Exec(t, `CREATE USER foo`)
		sqlDB.Exec(t, `GRANT ALL ON DATABASE defaultdb TO foo`)
		sqlDB.Exec(t, `CREATE TABLE d (a INT PRIMARY KEY, b STRING)`)
		defer sqlDB.Exec(t, `DROP TABLE d`)

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO d (a, b) CSV DATA (%s)`,
			testFiles.files[0]))

		// Verify correct number of rows via COUNT.
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM d`).Scan(&result)
		if result != rowsPerFile {
			t.Fatalf("expected %d rows, got %d", rowsPerFile, result)
		}
	})
}

func benchUserUpload(b *testing.B, uploadBaseURI string) {
	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(b)
	defer cleanup()
	f, err := ioutil.TempFile(baseDir, "test_file")
	require.NoError(b, err)
	testFileBase := fmt.Sprintf("/%s", filepath.Base(f.Name()))

	tc := serverutils.StartNewTestCluster(b, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Every row (int, string) generated by the CSVGenerator is ~25 bytes.
	// So numRows gives us ~25 MiB of generated CSV content.
	numRows := 1 * 1024 * 1024
	csvGen := newCsvGenerator(0, numRows, &intGenerator{}, &strGenerator{})

	uri, err := url.ParseRequestURI(uploadBaseURI)
	require.NoError(b, err)

	r, err := csvGen.Open()
	require.NoError(b, err)

	var numBytes int64
	if uri.Scheme == "nodelocal" {
		// Write the test data into a file.
		require.NoError(b, err)
		numBytes, err = io.Copy(f, ioctx.ReaderCtxAdapter(ctx, r))
		require.NoError(b, err)
	} else if uri.Scheme == "userfile" {
		// Write the test data to userfile storage.
		userfileStorage, err := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).DistSQLSrv.
			ExternalStorageFromURI(ctx, uploadBaseURI+testFileBase, security.RootUserName())
		require.NoError(b, err)
		content, err := ioctx.ReadAll(ctx, r)
		require.NoError(b, err)
		err = cloud.WriteFile(ctx, userfileStorage, "", bytes.NewReader(content))
		require.NoError(b, err)
		numBytes = int64(len(content))
	} else {
		b.Fatal(errors.New("benchmarking unsupported URI scheme"))
	}

	b.SetBytes(numBytes)
	b.ResetTimer()

	sqlDB.Exec(b, `CREATE TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))`)
	sqlDB.Exec(b,
		fmt.Sprintf(
			`IMPORT INTO t CSV DATA ('%s%s')`,
			uploadBaseURI, testFileBase,
		))
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/sql/importer
// BenchmarkNodelocalImport-16    	       1	4444906026 ns/op	   6.11 MB/s
// BenchmarkNodelocalImport-16    	       1	3943970329 ns/op	   6.88 MB/s
// BenchmarkNodelocalImport-16    	       1	4372378719 ns/op	   6.21 MB/s
// BenchmarkNodelocalImport-16    	       1	4182168878 ns/op	   6.49 MB/s
// BenchmarkNodelocalImport-16    	       1	4255328766 ns/op	   6.38 MB/s
// BenchmarkNodelocalImport-16    	       1	5367984071 ns/op	   5.06 MB/s
// BenchmarkNodelocalImport-16    	       1	4130455146 ns/op	   6.57 MB/s
// BenchmarkNodelocalImport-16    	       1	4080583559 ns/op	   6.65 MB/s
// BenchmarkNodelocalImport-16    	       1	4774760252 ns/op	   5.68 MB/s
// BenchmarkNodelocalImport-16    	       1	4967456028 ns/op	   5.46 MB/s
func BenchmarkNodelocalImport(b *testing.B) {
	benchUserUpload(b, "nodelocal://0")
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/sql/importer
// BenchmarkUserfileImport-16    	       1	3950434182 ns/op	   6.87 MB/s
// BenchmarkUserfileImport-16    	       1	4087946074 ns/op	   6.64 MB/s
// BenchmarkUserfileImport-16    	       1	4422526863 ns/op	   6.14 MB/s
// BenchmarkUserfileImport-16    	       1	5062665154 ns/op	   5.36 MB/s
// BenchmarkUserfileImport-16    	       1	3829669681 ns/op	   7.09 MB/s
// BenchmarkUserfileImport-16    	       1	4553600442 ns/op	   5.96 MB/s
// BenchmarkUserfileImport-16    	       1	4333825355 ns/op	   6.26 MB/s
// BenchmarkUserfileImport-16    	       1	4565827783 ns/op	   5.94 MB/s
// BenchmarkUserfileImport-16    	       1	4060204527 ns/op	   6.68 MB/s
// BenchmarkUserfileImport-16    	       1	4627419761 ns/op	   5.86 MB/s
func BenchmarkUserfileImport(b *testing.B) {
	benchUserUpload(b, "userfile://defaultdb.public.root")
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

	tableDesc, err := MakeTestingSimpleTableDescriptor(ctx, &semaCtx, st, create, descpb.ID(100), keys.PublicSchemaIDForBackup, descpb.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	importCtx := &parallelImportContext{
		semaCtx:    &semaCtx,
		evalCtx:    &evalCtx,
		tableDesc:  tableDesc.ImmutableCopy().(catalog.TableDescriptor),
		kvCh:       kvCh,
		numWorkers: 1,
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

func selectNotNull(col string) string {
	return fmt.Sprintf(`SELECT %s FROM t WHERE %s IS NOT NULL`, col, col)
}

// Test that IMPORT INTO works when columns with default expressions are present.
// The default expressions supported by IMPORT INTO are constant expressions,
// which are literals and functions that always return the same value given the
// same arguments (examples of non-constant expressions are given in the last two
// subtests below). The default expression of a column is used when this column is not
// targeted; otherwise, data from source file (like CSV) is used. It also checks
// that IMPORT TABLE works when there are default columns.
func TestImportDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes >1min under race")
	const nodes = 3
	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16
	testFiles := makeCSVData(t, numFiles, rowsPerFile, nodes, rowsPerRaceFile)

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "csv")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()
	tests := []struct {
		name       string
		data       string
		create     string
		targetCols string
		format     string
		sequence   string
		with       string
		// We expect exactly one of expectedResults and expectedError:
		// the latter is relevant for default expressions we don't support.
		expectedResults [][]string
		expectedError   string
	}{
		// CSV formats.
		{
			name:            "is-not-target",
			data:            "1\n2",
			create:          "b INT DEFAULT 42, a INT",
			targetCols:      "a",
			format:          "CSV",
			expectedResults: [][]string{{"42", "1"}, {"42", "2"}},
		},
		{
			name:            "is-not-target-not-null",
			data:            "1\n2",
			create:          "a INT, b INT DEFAULT 42 NOT NULL",
			targetCols:      "a",
			format:          "CSV",
			expectedResults: [][]string{{"1", "42"}, {"2", "42"}},
		},
		{
			name:            "is-target",
			data:            "1,36\n2,37",
			create:          "a INT, b INT DEFAULT 42",
			targetCols:      "a, b",
			format:          "CSV",
			expectedResults: [][]string{{"1", "36"}, {"2", "37"}},
		},
		{
			name:            "mixed-target-and-non-target",
			data:            "35,test string\n72,another test string",
			create:          "b STRING, a INT DEFAULT 53, c INT DEFAULT 42",
			targetCols:      "a, b",
			format:          "CSV",
			expectedResults: [][]string{{"test string", "35", "42"}, {"another test string", "72", "42"}},
		},
		{
			name:            "null-as-default",
			data:            "1\n2\n3",
			create:          "a INT DEFAULT NULL, b INT",
			targetCols:      "b",
			format:          "CSV",
			expectedResults: [][]string{{"NULL", "1"}, {"NULL", "2"}, {"NULL", "3"}},
		},
		{
			name:            "is-target-with-null-data",
			data:            ",36\n2,",
			create:          "a INT, b INT DEFAULT 42",
			targetCols:      "a, b",
			format:          "CSV",
			with:            `nullif = ''`,
			expectedResults: [][]string{{"NULL", "36"}, {"2", "NULL"}},
		},
		{
			name:            "math-constant",
			data:            "35\n67",
			create:          "a INT, b FLOAT DEFAULT round(pi())",
			targetCols:      "a",
			format:          "CSV",
			expectedResults: [][]string{{"35", "3"}, {"67", "3"}},
		},
		{
			name:            "string-function",
			data:            "1\n2",
			create:          `a INT, b STRING DEFAULT repeat('dog', 2)`,
			targetCols:      "a",
			format:          "CSV",
			expectedResults: [][]string{{"1", "dogdog"}, {"2", "dogdog"}},
		},
		{
			name:            "arithmetic",
			data:            "1\n2",
			create:          `a INT, b INT DEFAULT 34 * 3`,
			targetCols:      "a",
			format:          "CSV",
			expectedResults: [][]string{{"1", "102"}, {"2", "102"}},
		},
		// TODO (anzoteh96): add AVRO format, and also MySQL and PGDUMP once
		// IMPORT INTO are supported for these file formats.
		{
			name:            "delimited",
			data:            "1\t2\n3\t4",
			create:          "a INT, b INT DEFAULT 42, c INT",
			targetCols:      "c, a",
			format:          "DELIMITED",
			expectedResults: [][]string{{"2", "42", "1"}, {"4", "42", "3"}},
		},
		{
			name:            "pgcopy",
			data:            "1,2\n3,4",
			create:          "a INT, b INT DEFAULT 42, c INT",
			targetCols:      "c, a",
			with:            `delimiter = ","`,
			format:          "PGCOPY",
			expectedResults: [][]string{{"2", "42", "1"}, {"4", "42", "3"}},
		},
	}
	for _, test := range tests {
		if test.sequence != "" {
			defer sqlDB.Exec(t, fmt.Sprintf(`DROP SEQUENCE IF EXISTS %s`, test.sequence))
		}
		t.Run(test.name, func(t *testing.T) {
			defer sqlDB.Exec(t, `DROP TABLE t`)
			if test.sequence != "" {
				sqlDB.Exec(t, fmt.Sprintf(`CREATE SEQUENCE %s`, test.sequence))
			}
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (%s)`, test.create))
			data = test.data
			importStmt := fmt.Sprintf(`IMPORT INTO t (%s) %s DATA ("%s")`, test.targetCols, test.format, srv.URL)
			if test.with != "" {
				importStmt = importStmt + fmt.Sprintf(` WITH %s`, test.with)
			}
			if test.expectedError != "" {
				sqlDB.ExpectErr(t, test.expectedError, importStmt)
			} else {
				sqlDB.Exec(t, importStmt)
				sqlDB.CheckQueryResults(t, `SELECT * FROM t`, test.expectedResults)
			}
		})
	}
	t.Run("current-timestamp", func(t *testing.T) {
		data = "1\n2\n3\n4\n5\n6"
		testCases := []struct {
			name        string
			defaultExpr string
			colType     string
			truncate    time.Duration
		}{
			{
				name:        "current_date",
				defaultExpr: "current_date()",
				colType:     "DATE",
				truncate:    24 * time.Hour,
			},
			{
				name:        "current_timestamp",
				defaultExpr: "current_timestamp()",
				colType:     "TIMESTAMP",
			},
			{
				name:        "current_timestamp_with_precision",
				defaultExpr: "current_timestamp(3)",
				colType:     "TIMESTAMP",
				truncate:    time.Millisecond,
			},
			{
				name:        "current_timestamp_as_int",
				defaultExpr: "current_timestamp()::int",
				colType:     "INT",
			},
			{
				name:        "localtimestamp",
				defaultExpr: "localtimestamp()::TIMESTAMPTZ",
				colType:     "TIMESTAMPTZ",
			},
			{
				name:        "localtimestamp_with_precision",
				defaultExpr: "localtimestamp(3)",
				colType:     "TIMESTAMP",
				truncate:    time.Millisecond,
			},
			{
				name:        "localtimestamp_with_expr_precision",
				defaultExpr: "localtimestamp(1+2+3)",
				colType:     "TIMESTAMP",
			},
			{
				name:        "now",
				defaultExpr: "now()",
				colType:     "TIMESTAMP",
			},
			{
				name:        "now-case-insensitive",
				defaultExpr: "NoW()",
				colType:     "DATE",
			},
			{
				name:        "pg_catalog.now",
				defaultExpr: "pg_catalog.now()",
				colType:     "DATE",
			},
			{
				name:        "statement_timestamp",
				defaultExpr: "statement_timestamp()",
				colType:     "TIMESTAMP",
			},
			{
				name:        "transaction_timestamp",
				defaultExpr: "transaction_timestamp()",
				colType:     "TIMESTAMP",
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				defer sqlDB.Exec(t, `DROP TABLE t`)
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t(a INT, b %s DEFAULT %s)`, test.colType, test.defaultExpr))
				minTs := timeutil.Now()
				sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a) CSV DATA ("%s")`, srv.URL))
				maxTs := timeutil.Now()
				if test.truncate != 0 {
					minTs = minTs.Truncate(test.truncate)
					maxTs = maxTs.Truncate(test.truncate)
				}

				var numBadRows int
				if test.colType == "INT" {
					minTsInt := minTs.Unix()
					maxTsInt := maxTs.Unix()
					sqlDB.QueryRow(t,
						`SELECT count(*) FROM t WHERE  b !=(SELECT b FROM t WHERE a=1) OR b IS NULL or b < $1 or b > $2`,
						minTsInt,
						maxTsInt,
					).Scan(&numBadRows)
				} else {
					sqlDB.QueryRow(t,
						`SELECT count(*) FROM t WHERE  b !=(SELECT b FROM t WHERE a=1) OR b IS NULL or b < $1 or b > $2`,
						minTs,
						maxTs,
					).Scan(&numBadRows)
				}
				require.Equal(t, 0, numBadRows)
			})
		}
	})
	t.Run("unique_rowid", func(t *testing.T) {
		const M = int(1e9 + 7) // Remainder for unique_rowid addition.
		testCases := []struct {
			name       string
			create     string
			targetCols []string
			insert     string
			rowIDCols  []string
		}{
			{
				name:       "multiple_unique_rowid",
				create:     "a INT DEFAULT unique_rowid(), b INT, c STRING, d INT DEFAULT unique_rowid()",
				targetCols: []string{"b", "c"},
				insert:     "INSERT INTO t (b, c) VALUES (3, 'CAT'), (4, 'DOG')",
				rowIDCols:  []string{selectNotNull("a"), selectNotNull("d")},
			},
			{
				name:       "unique_rowid_with_pk",
				create:     "a INT DEFAULT unique_rowid(), b INT PRIMARY KEY, c STRING",
				targetCols: []string{"b", "c"},
				insert:     "INSERT INTO t (b, c) VALUES (-3, 'CAT'), (-4, 'DOG')",
				rowIDCols:  []string{selectNotNull("a")},
			},
			{
				// unique_rowid()+unique_rowid() won't work as the rowid produced by import
				// has its leftmost bit set to 1, and adding them causes overflow. A way to
				// get around is to have each unique_rowid() modulo a number, M. Here M = 1e9+7
				// is used here given that it's big enough and is a prime, which is
				// generally effective in avoiding collisions.
				name: "rowid+rowid",
				create: fmt.Sprintf(
					`a INT DEFAULT (unique_rowid() %% %d) + (unique_rowid() %% %d), b INT PRIMARY KEY, c STRING`, M, M),
				targetCols: []string{"b", "c"},
				rowIDCols:  []string{selectNotNull("a")},
			},
		}
		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				defer sqlDB.Exec(t, `DROP TABLE t`)
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t(%s)`, test.create))
				if test.insert != "" {
					sqlDB.Exec(t, test.insert)
				}
				sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (%s) CSV DATA (%s)`,
					strings.Join(test.targetCols, ", "),
					strings.Join(testFiles.files, ", ")))
				var numDistinctRows int
				sqlDB.QueryRow(t,
					fmt.Sprintf(`SELECT DISTINCT COUNT (*) FROM (%s)`,
						strings.Join(test.rowIDCols, " UNION ")),
				).Scan(&numDistinctRows)
				var numRows int
				sqlDB.QueryRow(t, `SELECT COUNT (*) FROM t`).Scan(&numRows)
				require.Equal(t, numDistinctRows, len(test.rowIDCols)*numRows)
			})
		}
	})
	t.Run("random-functions", func(t *testing.T) {
		testCases := []struct {
			name       string
			create     string
			targetCols []string
			randomCols []string
			data       string
		}{
			{
				name:       "random-multiple",
				create:     "a INT, b FLOAT PRIMARY KEY DEFAULT random(), c STRING, d FLOAT DEFAULT random()",
				targetCols: []string{"a", "c"},
				randomCols: []string{selectNotNull("b"), selectNotNull("d")},
			},
			{
				name:       "gen_random_uuid",
				create:     "a INT, b STRING, c UUID PRIMARY KEY DEFAULT gen_random_uuid(), d UUID DEFAULT gen_random_uuid()",
				targetCols: []string{"a", "b"},
				randomCols: []string{selectNotNull("c"), selectNotNull("d")},
			},
			{
				name:       "mixed_random_uuid",
				create:     "a INT, b STRING, c UUID PRIMARY KEY DEFAULT gen_random_uuid(), d FLOAT DEFAULT random()",
				targetCols: []string{"a", "b"},
				randomCols: []string{selectNotNull("c")},
			},
			{
				name:       "random_with_targeted",
				create:     "a INT, b FLOAT DEFAULT random(), d FLOAT DEFAULT random()",
				targetCols: []string{"a", "b"},
				randomCols: []string{selectNotNull("d")},
				data:       "1,0.37\n2,0.455\n3,3.14\n4,0.246\n5,0.42",
			},
			// TODO (anzoteh96): create a testcase for AVRO once we manage to extract
			// targeted columns from the AVRO schema.
		}
		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				defer sqlDB.Exec(t, `DROP TABLE t`)
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t(%s)`, test.create))
				fileName := strings.Join(testFiles.files, ", ")
				if test.data != "" {
					data = test.data
					fileName = fmt.Sprintf(`%q`, srv.URL)
				}
				// Let's do 3 IMPORTs for each test case to ensure that the values produced
				// do not overlap.
				for i := 0; i < 3; i++ {
					sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (%s) CSV DATA (%s)`,
						strings.Join(test.targetCols, ", "),
						fileName))
				}
				var numDistinctRows int
				sqlDB.QueryRow(t,
					fmt.Sprintf(`SELECT DISTINCT COUNT (*) FROM (%s)`,
						strings.Join(test.randomCols, " UNION ")),
				).Scan(&numDistinctRows)
				var numRows int
				sqlDB.QueryRow(t, `SELECT COUNT (*) FROM t`).Scan(&numRows)
				require.Equal(t, numDistinctRows, len(test.randomCols)*numRows)
			})
		}
	})
}

// This is a regression test for #61203. We test that the random() keys are
// unique on a larger data set. This would previously fail with a primary key
// collision error since we would generate duplicate UUIDs.
//
// Note: that although there is no guarantee that UUIDs do not collide, the
// probability of such a collision is vanishingly low.
func TestUniqueUUID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is slow under race since it explicitly tried to import a large
	// amount of data.
	skip.UnderRace(t, "slow under race")

	const (
		nodes     = 3
		dataDir   = "userfile://defaultdb.my_files/export"
		dataFiles = dataDir + "/*"
	)
	ctx := context.Background()
	args := base.TestServerArgs{}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	connDB := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(connDB)

	dataSize := parallelImporterReaderBatchSize * 100

	sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE data AS SELECT * FROM generate_series(1, %d);`, dataSize))
	sqlDB.Exec(t, `EXPORT INTO CSV $1 FROM TABLE data;`, dataDir)

	// Ensure that UUIDs do not collide when importing 20000 rows.
	sqlDB.Exec(t, `CREATE TABLE r1 (a UUID PRIMARY KEY DEFAULT gen_random_uuid(), b INT);`)
	sqlDB.Exec(t, `IMPORT INTO r1 (b) CSV DATA ($1);`, dataFiles)

	// Ensure that UUIDs do not collide when importing into a table with several UUID calls.
	sqlDB.Exec(t, `CREATE TABLE r2 (a UUID PRIMARY KEY DEFAULT gen_random_uuid(), b INT, c UUID DEFAULT gen_random_uuid());`)
	sqlDB.Exec(t, `IMPORT INTO r2 (b) CSV DATA ($1);`, dataFiles)

	// Ensure that random keys do not collide.
	sqlDB.Exec(t, `CREATE TABLE r3 (a FLOAT PRIMARY KEY DEFAULT random(), b INT);`)
	sqlDB.Exec(t, `IMPORT INTO r3 (b) CSV DATA ($1);`, dataFiles)
}

func TestImportDefaultNextVal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer setImportReaderParallelism(1)()
	skip.UnderStressRace(t, "test hits a timeout before a successful run")

	const nodes = 3
	numFiles := 1
	rowsPerFile := 1000
	rowsPerRaceFile := 16
	testFiles := makeCSVData(t, numFiles, rowsPerFile, numFiles, rowsPerRaceFile)

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "csv")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	sqlDB := sqlutils.MakeSQLRunner(conn)

	type seqMetadata struct {
		start                     int
		increment                 int
		expectedImportChunkAllocs int
		// We process fewer rows under race.
		expectedImportChunkAllocsUnderRace int
	}

	t.Run("nextval", func(t *testing.T) {
		testCases := []struct {
			name            string
			create          string
			targetCols      []string
			seqToNumNextval map[string]seqMetadata
			insertData      string
		}{
			{
				name:       "simple-nextval",
				create:     "a INT, b INT DEFAULT nextval('myseq'), c STRING",
				targetCols: []string{"a", "c"},
				// 1000 rows means we will allocate 3 chunks of 10, 100, 1000.
				// The 2 inserts will add 6 more nextval calls.
				// First insert: 1->3
				// Import: 3->1113
				// Second insert 1113->1116
				seqToNumNextval: map[string]seqMetadata{"myseq": {1, 1, 1116, 116}},
				insertData:      `(1, 'cat'), (2, 'him'), (3, 'meme')`,
			},
			{
				name:       "simple-nextval-with-increment-and-start",
				create:     "a INT, b INT DEFAULT nextval('myseq'), c STRING",
				targetCols: []string{"a", "c"},
				// 1000 rows means we will allocate 3 chunks of 10, 100, 1000.
				// The 2 inserts will add 6 more nextval calls.
				// First insert: 100->120
				// Import: 120->11220
				// Second insert: 11220->11250
				seqToNumNextval: map[string]seqMetadata{"myseq": {100, 10, 11250, 1250}},
				insertData:      `(1, 'cat'), (2, 'him'), (3, 'meme')`,
			},
			{
				name:       "two-nextval-diff-seq",
				create:     "a INT, b INT DEFAULT nextval('myseq') + nextval('myseq2'), c STRING",
				targetCols: []string{"a", "c"},
				seqToNumNextval: map[string]seqMetadata{
					"myseq":  {1, 1, 1116, 116},
					"myseq2": {1, 1, 1116, 116},
				},
				insertData: `(1, 'cat'), (2, 'him'), (3, 'meme')`,
			},
			// TODO(adityamaru): Unskip once #56387 is fixed.
			//{
			//	name:                      "two-nextval-same-seq",
			//	create:                    "a INT, b INT DEFAULT nextval('myseq') + nextval('myseq'),
			//	c STRING",
			//	targetCols:                []string{"a", "c"},
			//	seqToNumNextval:           map[string]int{"myseq": 1, "myseq2": 1},
			//	expectedImportChunkAllocs: 1110,
			//},
			{
				name:       "two-nextval-cols-same-seq",
				create:     "a INT, b INT DEFAULT nextval('myseq'), c STRING, d INT DEFAULT nextval('myseq')",
				targetCols: []string{"a", "c"},
				// myseq will allocate 10, 100, 1000, 10000 for the 2000 rows.
				// 2 inserts will consume 12 more nextval calls.
				// First insert: 1->6
				// Import: 6->11116
				// Second insert: 11116->11122
				seqToNumNextval: map[string]seqMetadata{"myseq": {1, 1, 11122, 122}},
				insertData:      `(1, 'cat'), (2, 'him'), (3, 'meme')`,
			},
		}

		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				defer sqlDB.Exec(t, `DROP TABLE t`)
				for seqName := range test.seqToNumNextval {
					sqlDB.Exec(t, fmt.Sprintf(`DROP SEQUENCE IF EXISTS %s`, seqName))
					sqlDB.Exec(t, fmt.Sprintf(`CREATE SEQUENCE %s START %d INCREMENT %d`, seqName,
						test.seqToNumNextval[seqName].start, test.seqToNumNextval[seqName].increment))
				}
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (%s)`, test.create))
				sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO t (%s) VALUES %s`,
					strings.Join(test.targetCols, ", "), test.insertData))
				sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (%s) CSV DATA (%s)`,
					strings.Join(test.targetCols, ", "), strings.Join(testFiles.files, ", ")))
				sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO t (%s) VALUES %s`,
					strings.Join(test.targetCols, ", "), test.insertData))

				for seqName := range test.seqToNumNextval {
					var seqVal int
					sqlDB.QueryRow(t, fmt.Sprintf(`SELECT last_value from %s`, seqName)).Scan(&seqVal)
					expectedVal := test.seqToNumNextval[seqName].expectedImportChunkAllocs
					if util.RaceEnabled {
						expectedVal = test.seqToNumNextval[seqName].expectedImportChunkAllocsUnderRace
					}
					require.Equal(t, expectedVal, seqVal)
				}
			})
		}
	})
}

func TestImportDefaultWithResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer setImportReaderParallelism(1)()
	const batchSize = 5
	defer TestingSetParallelImporterReaderBatchSize(batchSize)()
	defer row.TestingSetDatumRowConverterBatchSize(2 * batchSize)()

	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL: &execinfra.TestingKnobs{
					BulkAdderFlushesEveryBatch: true,
				},
			},
		})
	registry := s.JobRegistry().(*jobs.Registry)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	testCases := []struct {
		name       string
		create     string
		targetCols string
		format     string
		sequence   string
	}{
		{
			name:       "nextval",
			create:     "a INT, b STRING, c INT PRIMARY KEY DEFAULT nextval('mysequence')",
			targetCols: "a, b",
			sequence:   "mysequence",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			defer fmt.Sprintf(`DROP SEQUENCE IF EXISTS %s`, test.sequence)
			defer sqlDB.Exec(t, `DROP TABLE t`)

			sqlDB.Exec(t, fmt.Sprintf(`CREATE SEQUENCE %s`, test.sequence))
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE t (%s)`, test.create))

			jobCtx, cancelImport := context.WithCancel(ctx)
			jobIDCh := make(chan jobspb.JobID)
			var jobID jobspb.JobID = -1

			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				// Arrange for our special job resumer to be
				// returned the very first time we start the import.
				jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
					resumer := raw.(*importResumer)
					resumer.testingKnobs.alwaysFlushJobProgress = true
					resumer.testingKnobs.afterImport = func(summary roachpb.RowCount) error {
						return nil
					}
					if jobID == -1 {
						return &cancellableImportResumer{
							ctx:     jobCtx,
							jobIDCh: jobIDCh,
							wrapped: resumer,
						}
					}
					return resumer
				},
			}

			expectedNumRows := 10*batchSize + 1
			testBarrier, csvBarrier := newSyncBarrier()
			csv1 := newCsvGenerator(0, expectedNumRows, &intGenerator{}, &strGenerator{})
			csv1.addBreakpoint(7*batchSize, func() (bool, error) {
				defer csvBarrier.Enter()()
				return false, nil
			})

			// Convince distsql to use our "external" storage implementation.
			storage := newGeneratedStorage(csv1)
			s.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ExternalStorage = storage.externalStorageFactory()

			// Execute import; ignore any errors returned
			// (since we're aborting the first import run.).
			go func() {
				_, _ = sqlDB.DB.ExecContext(ctx,
					fmt.Sprintf(`IMPORT INTO t (%s) CSV DATA ($1)`, test.targetCols), storage.getGeneratorURIs()[0])
			}()
			jobID = <-jobIDCh

			// Wait until we are blocked handling breakpoint.
			unblockImport := testBarrier.Enter()
			// Wait until we have recorded some job progress.
			js := queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool {
				return js.prog.ResumePos[0] > 0
			})

			// Pause the job;
			if err := registry.PauseRequested(ctx, nil, jobID, ""); err != nil {
				t.Fatal(err)
			}
			// Send cancellation and unblock breakpoint.
			cancelImport()
			unblockImport()

			// Get number of sequence value chunks which have been reserved.
			js = queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool {
				return jobs.StatusPaused == js.status
			})
			// We expect two chunk entries since our breakpoint is at 7*batchSize.
			// [1, 10] and [11, 100]
			var id int32
			sqlDB.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name='%s'`,
				test.sequence)).Scan(&id)
			seqDetailsOnPause := js.prog.SequenceDetails
			chunksOnPause := seqDetailsOnPause[0].SeqIdToChunks[id].Chunks
			require.Equal(t, len(chunksOnPause), 2)
			require.Equal(t, chunksOnPause[0].ChunkStartVal, int64(1))
			require.Equal(t, chunksOnPause[0].ChunkSize, int64(10))
			require.Equal(t, chunksOnPause[1].ChunkStartVal, int64(11))
			require.Equal(t, chunksOnPause[1].ChunkSize, int64(100))

			// Just to be doubly sure, check the sequence value before and after
			// resumption to make sure it hasn't changed.
			var seqValOnPause int64
			sqlDB.QueryRow(t, fmt.Sprintf(`SELECT last_value FROM %s`, test.sequence)).Scan(&seqValOnPause)

			// Unpause the job and wait for it to complete.
			if err := registry.Unpause(ctx, nil, jobID); err != nil {
				t.Fatal(err)
			}
			js = queryJobUntil(t, sqlDB.DB, jobID, func(js jobState) bool { return jobs.StatusSucceeded == js.status })
			// No additional chunks should have been allocated on job resumption since
			// we already have enough chunks of the sequence values to cover all the
			// rows.
			seqDetailsOnSuccess := js.prog.SequenceDetails
			require.Equal(t, seqDetailsOnPause, seqDetailsOnSuccess)

			var seqValOnSuccess int64
			sqlDB.QueryRow(t, fmt.Sprintf(`SELECT last_value FROM %s`,
				test.sequence)).Scan(&seqValOnSuccess)
			require.Equal(t, seqValOnPause, seqValOnSuccess)
		})
	}
}

func TestImportComputed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodes = 3

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "csv")
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	avroField := []map[string]interface{}{
		{
			"name": "a",
			"type": "int",
		},
		{
			"name": "b",
			"type": "int",
		},
	}
	avroRows := []map[string]interface{}{
		{"a": 1, "b": 2}, {"a": 3, "b": 4},
	}
	avroData := createAvroData(t, "t", avroField, avroRows)
	pgdumpData := `
CREATE TABLE users (a INT, b INT, c INT AS (a + b) STORED);
INSERT INTO users (a, b) VALUES (1, 2), (3, 4);
`
	defer srv.Close()
	tests := []struct {
		into       bool
		name       string
		data       string
		create     string
		targetCols string
		format     string
		// We expect exactly one of expectedResults and expectedError.
		expectedResults [][]string
		expectedError   string
	}{
		{
			into:            true,
			name:            "addition",
			data:            "35,23\n67,10",
			create:          "a INT, b INT, c INT AS (a + b) STORED",
			targetCols:      "a, b",
			format:          "CSV",
			expectedResults: [][]string{{"35", "23", "58"}, {"67", "10", "77"}},
		},
		{
			into:          true,
			name:          "cannot-be-targeted",
			data:          "1,2,3\n3,4,5",
			create:        "a INT, b INT, c INT AS (a + b) STORED",
			targetCols:    "a, b, c",
			format:        "CSV",
			expectedError: `cannot write directly to computed column "c"`,
		},
		{
			into:            true,
			name:            "with-default",
			data:            "35\n67",
			create:          "a INT, b INT DEFAULT 42, c INT AS (a + b) STORED",
			targetCols:      "a",
			format:          "CSV",
			expectedResults: [][]string{{"35", "42", "77"}, {"67", "42", "109"}},
		},
		{
			into:            true,
			name:            "target-cols-reordered",
			data:            "1,2\n3,4",
			create:          "a INT, b INT AS (a + c) STORED, c INT",
			targetCols:      "a, c",
			format:          "CSV",
			expectedResults: [][]string{{"1", "3", "2"}, {"3", "7", "4"}},
		},
		{
			into:            true,
			name:            "import-into-csv-expression-index",
			data:            "1,2\n3,4",
			create:          "a INT, b INT, INDEX ((a + b))",
			targetCols:      "a, b",
			format:          "CSV",
			expectedResults: [][]string{{"1", "2"}, {"3", "4"}},
		},
		{
			into:            true,
			name:            "import-into-avro",
			data:            avroData,
			create:          "a INT, b INT, c INT AS (a + b) STORED",
			targetCols:      "a, b",
			format:          "AVRO",
			expectedResults: [][]string{{"1", "2", "3"}, {"3", "4", "7"}},
		},
		{
			into:            true,
			name:            "import-into-avro-expression-index",
			data:            avroData,
			create:          "a INT, b INT, INDEX ((a + b))",
			targetCols:      "a, b",
			format:          "AVRO",
			expectedResults: [][]string{{"1", "2"}, {"3", "4"}},
		},
		{
			into:            false,
			name:            "pgdump",
			data:            pgdumpData,
			format:          "PGDUMP",
			expectedResults: [][]string{{"1", "2", "3"}, {"3", "4", "7"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer sqlDB.Exec(t, `DROP TABLE IF EXISTS users`)
			data = test.data
			var importStmt string
			if test.into {
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE users (%s)`, test.create))
				importStmt = fmt.Sprintf(`IMPORT INTO users (%s) %s DATA (%q)`,
					test.targetCols, test.format, srv.URL)
			} else {
				importStmt = fmt.Sprintf(`IMPORT %s (%q)`, test.format, srv.URL)
			}
			if test.expectedError != "" {
				sqlDB.ExpectErr(t, test.expectedError, importStmt)
			} else {
				sqlDB.Exec(t, importStmt)
				sqlDB.CheckQueryResults(t, `SELECT * FROM users`, test.expectedResults)
			}
		})
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/sql/importer
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

	tableDesc, err := MakeTestingSimpleTableDescriptor(ctx, &semaCtx, st, create, descpb.ID(100), keys.PublicSchemaIDForBackup, descpb.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	cols := make(tree.NameList, len(tableDesc.Columns))
	for i, col := range tableDesc.Columns {
		cols[i] = tree.Name(col.Name)
	}
	r, err := newMysqloutfileReader(&semaCtx, roachpb.MySQLOutfileOptions{
		RowSeparator:   '\n',
		FieldSeparator: '\t',
	}, kvCh, 0, 1,
		tableDesc.ImmutableCopy().(catalog.TableDescriptor), nil /* targetCols */, &evalCtx)
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
// pkg: github.com/cockroachdb/cockroach/pkg/sql/importer
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

	tableDesc, err := MakeTestingSimpleTableDescriptor(ctx, &semaCtx, st, create, descpb.ID(100), keys.PublicSchemaIDForBackup,
		descpb.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}

	kvCh := make(chan row.KVBatch)
	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	cols := make(tree.NameList, len(tableDesc.Columns))
	for i, col := range tableDesc.Columns {
		cols[i] = tree.Name(col.Name)
	}
	r, err := newPgCopyReader(&semaCtx, roachpb.PgCopyOptions{
		Delimiter:  '\t',
		Null:       `\N`,
		MaxRowSize: 4096,
	}, kvCh, 0, 1,
		tableDesc.ImmutableCopy().(catalog.TableDescriptor), nil /* targetCols */, &evalCtx)
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

// FakeResumer calls optional callbacks during the job lifecycle.
type fakeResumer struct {
	OnResume     func(context.Context) error
	FailOrCancel func(context.Context) error
}

var _ jobs.Resumer = fakeResumer{}

func (d fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d fakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

// TestImportControlJobRBAC tests that a root user can control any job, but
// a non-admin user can only control jobs which are created by them.
// TODO(adityamaru): Verifying the state of the job after the control command
// has been issued would also be nice, but it makes the test flaky.
func TestImportControlJobRBAC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	rootDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	registry := tc.Server(0).JobRegistry().(*jobs.Registry)

	// Create non-root user.
	rootDB.Exec(t, `CREATE USER testuser`)
	rootDB.Exec(t, `ALTER ROLE testuser CONTROLJOB`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), "TestImportPrivileges-testuser",
		url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer testuser.Close()

	done := make(chan struct{})
	defer close(done)

	jobs.RegisterConstructor(jobspb.TypeImport, func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeResumer{
			OnResume: func(ctx context.Context) error {
				<-done
				return nil
			},
			FailOrCancel: func(ctx context.Context) error {
				<-done
				return nil
			},
		}
	})

	startLeasedJob := func(t *testing.T, record jobs.Record) *jobs.StartableJob {
		job, err := jobs.TestingCreateAndStartJob(ctx, registry, tc.Server(0).DB(), record)
		require.NoError(t, err)
		return job
	}

	defaultRecord := jobs.Record{
		// Job does not accept an empty Details field, so arbitrarily provide
		// ImportDetails.
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
	}

	for _, tc := range []struct {
		name         string
		controlQuery string
	}{
		{
			"pause",
			`PAUSE JOB $1`,
		},
		{
			"cancel",
			`CANCEL JOB $1`,
		},
		{
			"resume",
			`RESUME JOB $1`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Start import job as root.
			rootJobRecord := defaultRecord
			rootJobRecord.Username = security.RootUserName()
			rootJob := startLeasedJob(t, rootJobRecord)

			// Test root can control root job.
			rootDB.Exec(t, tc.controlQuery, rootJob.ID())
			require.NoError(t, err)

			// Start import job as non-admin user.
			nonAdminJobRecord := defaultRecord
			nonAdminJobRecord.Username = security.TestUserName()
			userJob := startLeasedJob(t, nonAdminJobRecord)

			// Test testuser can control testuser job.
			_, err := testuser.Exec(tc.controlQuery, userJob.ID())
			require.NoError(t, err)

			// Start second import job as root.
			rootJob2 := startLeasedJob(t, rootJobRecord)

			// Start second import job as non-admin user.
			userJob2 := startLeasedJob(t, nonAdminJobRecord)

			// Test root can control testuser job.
			rootDB.Exec(t, tc.controlQuery, userJob2.ID())
			require.NoError(t, err)

			// Test testuser CANNOT control root job.
			_, err = testuser.Exec(tc.controlQuery, rootJob2.ID())
			require.True(t, testutils.IsError(err, "only admins can control jobs owned by other admins"))
		})
	}
}

// TestImportWorkerFailure tests that IMPORT retries after the failure of a
// worker node.
func TestImportWorkerFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 73546, "flaky test")
	defer log.Scope(t).Close(t)

	allowResponse := make(chan struct{})
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
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
	sqlDB.Exec(t, `CREATE TABLE t (i INT8 PRIMARY KEY)`)
	query := fmt.Sprintf(`IMPORT INTO t CSV DATA (%s) WITH sstsize = '1B'`, csvURLs)

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
	var jobID jobspb.JobID
	sqlDB.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)

	// Shut down a node.
	tc.StopServer(1)

	close(allowResponse)
	// We expect the statement to retry since it should have encountered a
	// retryable error.
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// But the job should be restarted and succeed eventually.
	jobutils.WaitForJob(t, sqlDB, jobID)
	sqlDB.CheckQueryResults(t,
		`SELECT * FROM t ORDER BY i`,
		sqlDB.QueryStr(t, `SELECT * FROM generate_series(0, $1)`, count-1),
	)
}

// TestImportMVCCChecksums verifies that MVCC checksums are correctly
// computed by issuing a secondary index change that runs a CPut on the
// index. See #23984.
func TestImportMVCCChecksums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	sqlDB.Exec(t, `CREATE TABLE d.t (a INT8 PRIMARY KEY, b INT8, c INT8, INDEX (b) STORING (c))`)
	sqlDB.Exec(t, `IMPORT INTO d.t CSV DATA ($1)`, srv.URL)
	sqlDB.Exec(t, `UPDATE d.t SET c = 2 WHERE a = 1`)
}

func TestImportMysql(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

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
					if got, expected := res[i][1],
						fmt.Sprintf("%v", everythingTestRows[i].c); got != expected {
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

// TODO (anzoteh96): this should have been in TestImportMysql, but the
// entire test was skipped. We should move this into TestImportMysql once
// it's unskipped.
func TestImportIntoMysql(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	data := `INSERT INTO t VALUES (1, 2), (3, 4)`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()
	defer sqlDB.Exec(t, "DROP TABLE t")
	sqlDB.Exec(t, "CREATE TABLE t (a INT, b INT)")
	sqlDB.ExpectErr(t,
		"MYSQLDUMP file format is currently unsupported by IMPORT INTO",
		fmt.Sprintf(`IMPORT INTO t (a, b) MYSQLDUMP DATA (%q)`, srv.URL))
}

func TestImportDelimited(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "mysqlout")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	testRows, configs := getMysqlOutfileTestdata(t)
	checkQueryResults := func(validationQuery string) {
		for idx, row := range sqlDB.QueryStr(t, validationQuery) {
			expected, actual := testRows[idx].s, row[1]
			if expected == injectNull {
				expected = "NULL"
			}

			if expected != actual {
				t.Fatalf("expected row i=%s string to be %q, got %q", row[0], expected, actual)
			}
		}
	}

	for i, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			var opts []interface{}

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
			// Test if IMPORT INTO works here by testing that they produce the same
			// results as IMPORT TABLE.
			t.Run("import-into", func(t *testing.T) {
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE into%d`, i))
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE into%d (i INT8 PRIMARY KEY, s text, b bytea)`, i))
				intoCmd := fmt.Sprintf(`IMPORT INTO into%d (i, s, b) DELIMITED DATA ($1)`, i)
				if len(flags) > 0 {
					intoCmd += " WITH " + strings.Join(flags, ", ")
				}
				sqlDB.Exec(t, intoCmd, opts...)
				checkQueryResults(fmt.Sprintf(`SELECT i, s, b FROM into%d ORDER BY i`, i))
			})
			t.Run("import-into-target-cols-reordered", func(t *testing.T) {
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE into%d`, i))
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE into%d (b bytea, i INT8 PRIMARY KEY, s text)`, i))
				intoCmd := fmt.Sprintf(`IMPORT INTO into%d (i, s, b) DELIMITED DATA ($1)`, i)
				if len(flags) > 0 {
					intoCmd += " WITH " + strings.Join(flags, ", ")
				}
				sqlDB.Exec(t, intoCmd, opts...)
				checkQueryResults(fmt.Sprintf(`SELECT i, s, b FROM into%d ORDER BY i`, i))
			})
		})
	}
}

func TestImportPgCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "pgcopy")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	testRows, configs := getPgCopyTestdata(t)

	checkQueryResults := func(validationQuery string) {
		for idx, row := range sqlDB.QueryStr(t, validationQuery) {
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
	}

	for i, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			var opts []interface{}

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
			// Test if IMPORT INTO works here by testing that they produce the same
			// results as IMPORT TABLE.
			t.Run("import-into", func(t *testing.T) {
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE into%d`, i))
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE into%d (i INT8 PRIMARY KEY, s text, b bytea)`, i))
				intoCmd := fmt.Sprintf(`IMPORT INTO into%d (i, s, b) PGCOPY DATA ($1)`, i)
				if len(flags) > 0 {
					intoCmd += " WITH " + strings.Join(flags, ", ")
				}
				sqlDB.Exec(t, intoCmd, opts...)
				checkQueryResults(fmt.Sprintf(`SELECT * FROM into%d ORDER BY i`, i))
			})
			t.Run("import-into-target-cols-reordered", func(t *testing.T) {
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE into%d`, i))
				sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE into%d (b bytea, s text, i INT8 PRIMARY KEY)`, i))
				intoCmd := fmt.Sprintf(`IMPORT INTO into%d (i, s, b) PGCOPY DATA ($1)`, i)
				if len(flags) > 0 {
					intoCmd += " WITH " + strings.Join(flags, ", ")
				}
				sqlDB.Exec(t, intoCmd, opts...)
				checkQueryResults(fmt.Sprintf(`SELECT i, s, b FROM into%d ORDER BY i`, i))
			})
		})
	}
}

func TestImportPgDump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
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
		{`single table dump`, expectSimple, `IMPORT TABLE simple FROM PGDUMP ($1) WITH ignore_unsupported_statements`, simple},
		{`second table dump`, expectSecond, `IMPORT TABLE second FROM PGDUMP ($1) WITH ignore_unsupported_statements`, second},
		{`simple from multi`, expectSimple, `IMPORT TABLE simple FROM PGDUMP ($1) WITH ignore_unsupported_statements`, multitable},
		{`second from multi`, expectSecond, `IMPORT TABLE second FROM PGDUMP ($1) WITH ignore_unsupported_statements`, multitable},
		{`all from multi`, expectAll, `IMPORT PGDUMP ($1) WITH ignore_unsupported_statements`, multitable},
	} {
		t.Run(c.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second`)
			sqlDB.Exec(t, c.query, c.args...)

			if c.expected == expectSimple || c.expected == expectAll {
				// Verify table schema because PKs and indexes are at the bottom of pg_dump.
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE simple`, [][]string{{
					"simple", `CREATE TABLE public.simple (
	i INT8 NOT NULL,
	s STRING NULL,
	b BYTES NULL,
	CONSTRAINT simple_pkey PRIMARY KEY (i ASC),
	UNIQUE INDEX simple_b_s_idx (b ASC, s ASC),
	INDEX simple_s_idx (s ASC)
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
					"second", `CREATE TABLE public.second (
	i INT8 NOT NULL,
	s STRING NULL,
	CONSTRAINT second_pkey PRIMARY KEY (i ASC)
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
					"seqtable", `CREATE TABLE public.seqtable (
	a INT8 NULL DEFAULT nextval('public.a_seq'::REGCLASS),
	b INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT seqtable_pkey PRIMARY KEY (rowid ASC)
)`,
				}})
				sqlDB.CheckQueryResults(t, `SHOW CREATE SEQUENCE a_seq`, [][]string{{
					"a_seq", `CREATE SEQUENCE public.a_seq MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
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

	t.Run("glob-multi", func(t *testing.T) {
		sqlDB.ExpectErr(t, "SQL dump files must be imported individually", `IMPORT PGDUMP 'nodelocal://0/pgdump/*.sql'`)
	})

	t.Run("target-cols-reordered", func(t *testing.T) {
		data := `
				CREATE TABLE "t" ("a" INT, "b" INT DEFAULT 42, "c" INT);
				INSERT INTO "t" ("c", "a") VALUES ('1', '2'), ('3', '4');
			`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()
		defer sqlDB.Exec(t, "DROP TABLE t")
		sqlDB.Exec(t, "IMPORT PGDUMP ($1)", srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * from t`, [][]string{{"2", "42", "1"}, {"4", "42", "3"}})
	})
	t.Run("import-into-not-supported", func(t *testing.T) {
		data := `INSERT INTO t VALUES (1, 2), (3, 4)`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()
		defer sqlDB.Exec(t, "DROP TABLE t")
		sqlDB.Exec(t, "CREATE TABLE t (a INT, b INT)")
		sqlDB.ExpectErr(t,
			"PGDUMP file format is currently unsupported by IMPORT INTO",
			fmt.Sprintf(`IMPORT INTO t (a, b) PGDUMP DATA (%q)`, srv.URL))
	})
	t.Run("more-target-cols-than-data", func(t *testing.T) {
		data := `
CREATE TABLE public.t (c STRING, a STRING, b STRING, d STRING);
COPY public.t (a, b, c) FROM stdin;
a	b	c
\.
INSERT INTO public.t (a, b, c) VALUES ('a', 'b', 'c');
			`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()
		defer sqlDB.Exec(t, "DROP TABLE t")
		sqlDB.Exec(t, `
IMPORT TABLE t FROM PGDUMP ($1) WITH ignore_unsupported_statements`, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * from t`,
			[][]string{{"c", "a", "b", "NULL"}, {"c", "a", "b", "NULL"}})
	})
}

func TestImportPgDumpIgnoredStmts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	data := `
				-- Statements that CRDB cannot parse.
				CREATE TRIGGER conditions_set_updated_at BEFORE UPDATE ON conditions FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

				REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM PUBLIC;
				REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM database;

				GRANT ALL ON SEQUENCE knex_migrations_id_seq TO database;
				GRANT SELECT ON SEQUENCE knex_migrations_id_seq TO opentrials_readonly;

				COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
				COMMENT ON FUNCTION f() is 'f';
				CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

				ALTER AGGREGATE myavg(integer) RENAME TO my_average;
				ALTER DOMAIN zipcode SET NOT NULL;

				-- Valid statement.
				CREATE TABLE foo (id INT);

				CREATE FUNCTION public.isnumeric(text) RETURNS boolean
				    LANGUAGE sql
				    AS $_$
				SELECT $1 ~ '^[0-9]+$'
				$_$;
				ALTER FUNCTION public.isnumeric(text) OWNER TO roland;

        ALTER TABLE "database"."table" ALTER COLUMN "Id" ADD GENERATED BY DEFAULT AS IDENTITY (
            SEQUENCE NAME "database"."sequencename"
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1
        );
        COPY db.table (col1, col2, col3, col4) FROM '$$PATH$$/3057.dat';
        GRANT USAGE ON SCHEMA "schemaname" TO davidt WITH GRANT OPTION;


				-- Valid statements.
				INSERT INTO foo VALUES (1), (2), (3);
				CREATE TABLE t (i INT8);

				-- Statements that CRDB can parse, but IMPORT does not support.
				-- These are processed during the schema pass of IMPORT.
				COMMENT ON TABLE t IS 'This should be skipped';
				COMMENT ON DATABASE t IS 'This should be skipped';
				COMMENT ON COLUMN t IS 'This should be skipped';


				-- Statements that CRDB can parse, but IMPORT does not support.
				-- These are processed during the data ingestion pass of IMPORT.
				SELECT pg_catalog.set_config('search_path', '', false);
				DELETE FROM geometry_columns WHERE f_table_name = 'nyc_census_blocks' AND f_table_schema = 'public';
			`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()
	t.Run("ignore-unsupported", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE foo; USE foo;")
		sqlDB.Exec(t, "IMPORT PGDUMP ($1) WITH ignore_unsupported_statements", srv.URL)
		// Check that statements that are not expected to be ignored, are still
		// processed.
		sqlDB.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"1"}, {"2"}, {"3"}})
	})

	t.Run("dont-ignore-unsupported", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE foo1; USE foo1;")
		sqlDB.ExpectErr(t, "syntax error", "IMPORT PGDUMP ($1)", srv.URL)
	})

	t.Run("require-both-unsupported-options", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE foo2; USE foo2;")
		ignoredLog := `userfile:///ignore`
		sqlDB.ExpectErr(t, "cannot log unsupported PGDUMP stmts without `ignore_unsupported_statements` option",
			"IMPORT PGDUMP ($1) WITH log_ignored_statements=$2", srv.URL, ignoredLog)
	})

	t.Run("log-unsupported-stmts", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE foo3; USE foo3;")
		ignoredLog := `userfile:///ignore`
		defer testingSetMaxLogIgnoredImportStatements(10 /* maxLogSize */)()
		var importJobID int
		var unused interface{}
		sqlDB.QueryRow(t, "IMPORT PGDUMP ($1) WITH ignore_unsupported_statements, "+
			"log_ignored_statements=$2", srv.URL, ignoredLog).Scan(&importJobID, &unused, &unused,
			&unused, &unused, &unused)
		// Check that statements which are not expected to be ignored, are still
		// processed.
		sqlDB.CheckQueryResults(t, "SELECT * FROM foo", [][]string{{"1"}, {"2"}, {"3"}})

		// Read the unsupported log and verify its contents.
		store, err := cloud.ExternalStorageFromURI(ctx, ignoredLog,
			base.ExternalIODirConfig{},
			tc.Server(0).ClusterSettings(),
			blobs.TestEmptyBlobClientFactory,
			security.RootUserName(),
			tc.Server(0).InternalExecutor().(*sql.InternalExecutor), tc.Server(0).DB())
		require.NoError(t, err)
		defer store.Close()

		// We expect there to be two log files since we have 13 unsupported statements.
		dirName := fmt.Sprintf("import%d", importJobID)
		checkFiles := func(expectedFileContent []string, logSubdir string) {
			var files []string
			require.NoError(t, store.List(ctx, "", "", func(f string) error {
				ok, err := path.Match(fmt.Sprintf("*/%s/*", logSubdir), f)
				if ok {
					files = append(files, f)
				}
				return err
			}))
			for i, file := range files {
				require.Equal(t, file, path.Join(dirName, logSubdir, fmt.Sprintf("%d.log", i)))
				content, err := store.ReadFile(ctx, file)
				require.NoError(t, err)
				descBytes, err := ioctx.ReadAll(ctx, content)
				require.NoError(t, err)
				require.Equal(t, []byte(expectedFileContent[i]), descBytes)
			}
		}

		schemaFileContents := []string{
			`create trigger: could not be parsed
revoke privileges on sequence: could not be parsed
revoke privileges on sequence: could not be parsed
grant privileges on sequence: could not be parsed
grant privileges on sequence: could not be parsed
comment on extension: could not be parsed
comment on function: could not be parsed
create extension if not exists with: could not be parsed
alter aggregate: could not be parsed
alter domain: could not be parsed
`,
			`create function: could not be parsed
alter function: could not be parsed
alter table alter column add: could not be parsed
copy from unsupported format: could not be parsed
grant privileges on schema with: could not be parsed
COMMENT ON TABLE t IS 'This should be skipped': unsupported by IMPORT
COMMENT ON DATABASE t IS 'This should be skipped': unsupported by IMPORT
COMMENT ON COLUMN t IS 'This should be skipped': unsupported by IMPORT
unsupported function call: set_config in stmt: SELECT set_config('search_path', '', false): unsupported by IMPORT
`,
		}
		checkFiles(schemaFileContents, pgDumpUnsupportedSchemaStmtLog)

		ingestionFileContents := []string{
			`unsupported 3 fn args in select: ['search_path' '' false]: unsupported by IMPORT
unsupported *tree.Delete statement: DELETE FROM geometry_columns WHERE (f_table_name = 'nyc_census_blocks') AND (f_table_schema = 'public'): unsupported by IMPORT
`,
		}
		checkFiles(ingestionFileContents, pgDumpUnsupportedDataStmtLog)
	})
}

// TestImportPgDumpGeo tests that a file with SQLFn classes can be
// imported. These are functions like AddGeometryColumn which create and
// execute SQL when called (!). They are, for example, used by shp2pgsql
// (https://manpages.debian.org/stretch/postgis/shp2pgsql.1.en.html).
func TestImportPgDumpGeo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodes = 1
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "pgdump")
	args := base.TestServerArgs{ExternalIODir: baseDir}

	t.Run("geo_shp2pgsql.sql", func(t *testing.T) {
		tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(ctx)
		conn := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(conn)

		sqlDB.Exec(t, `CREATE DATABASE importdb; SET DATABASE = importdb`)
		sqlDB.Exec(t, "IMPORT PGDUMP 'nodelocal://0/geo_shp2pgsql.sql' WITH ignore_unsupported_statements")

		sqlDB.Exec(t, `CREATE DATABASE execdb; SET DATABASE = execdb`)
		geoSQL, err := ioutil.ReadFile(filepath.Join(baseDir, "geo_shp2pgsql.sql"))
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.Exec(t, string(geoSQL))

		// Verify both created tables are identical.
		importCreate := sqlDB.QueryStr(t, "SELECT create_statement FROM [SHOW CREATE importdb.nyc_census_blocks]")
		// Families are slightly different due to rowid showing up in exec but
		// not import (possibly due to the ALTER TABLE statement that makes
		// gid a primary key), so add that into import to match exec.
		importCreate[0][0] = strings.Replace(importCreate[0][0], "boroname, geom", "boroname, rowid, geom", 1)
		// The rowid column is implicitly created as ALTER PRIMARY KEY only comes into effect later.
		// As such, insert the line.
		importCreate[0][0] = strings.Replace(
			importCreate[0][0],
			"boroname VARCHAR(32) NULL",
			"boroname VARCHAR(32) NULL,\n\trowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid()",
			1,
		)
		sqlDB.CheckQueryResults(t, "SELECT create_statement FROM [SHOW CREATE execdb.nyc_census_blocks]", importCreate)

		importCols := "blkid, popn_total, popn_white, popn_black, popn_nativ, popn_asian, popn_other, boroname"
		importSelect := sqlDB.QueryStr(t, fmt.Sprintf(
			"SELECT (%s) FROM importdb.nyc_census_blocks ORDER BY PRIMARY KEY importdb.nyc_census_blocks",
			importCols,
		))
		sqlDB.CheckQueryResults(t, fmt.Sprintf(
			"SELECT (%s) FROM execdb.nyc_census_blocks ORDER BY PRIMARY KEY execdb.nyc_census_blocks",
			importCols,
		), importSelect)
	})

	t.Run("geo_ogr2ogr.sql", func(t *testing.T) {
		tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(ctx)
		conn := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(conn)

		sqlDB.Exec(t, `CREATE DATABASE importdb; SET DATABASE = importdb`)
		sqlDB.Exec(t, "IMPORT PGDUMP 'nodelocal://0/geo_ogr2ogr.sql' WITH ignore_unsupported_statements")

		sqlDB.Exec(t, `CREATE DATABASE execdb; SET DATABASE = execdb`)
		geoSQL, err := ioutil.ReadFile(filepath.Join(baseDir, "geo_ogr2ogr.sql"))
		if err != nil {
			t.Fatal(err)
		}
		// We cannot process DELETE FROM geometry_columns statement, so ignore it.
		replacedSQL := regexp.MustCompile("DELETE FROM[^;]*").ReplaceAll(geoSQL, []byte(""))
		sqlDB.Exec(t, string(replacedSQL))

		// Verify both created tables are identical.
		importCreate := sqlDB.QueryStr(t, `SELECT create_statement FROM [SHOW CREATE importdb."HydroNode"]`)
		sqlDB.CheckQueryResults(t, `SELECT create_statement FROM [SHOW CREATE execdb."HydroNode"]`, importCreate)
	})
}

func TestImportPgDumpDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	// If the target table for a DROP exists, we throw an error.
	t.Run("table exists", func(t *testing.T) {
		// Set up table `t` exists for testing.
		sqlDB.Exec(t, `DROP TABLE IF EXISTS t; CREATE TABLE t (a INT);`)

		// Import PGDump data which includes DROP TABLE.
		data = `DROP TABLE t; CREATE TABLE t (a INT); INSERT INTO t VALUES (4);`
		sqlDB.ExpectErr(t, `drop table "t" and then retry the import`, `IMPORT PGDUMP ($1)`, srv.URL)

		// Also expect error on existing table with IF EXISTS.
		data = `DROP TABLE IF EXISTS t; CREATE TABLE t (a INT); INSERT INTO t VALUES (4);`
		sqlDB.ExpectErr(t, `drop table "t" and then retry the import`, `IMPORT PGDUMP ($1)`, srv.URL)

		// Cleanup.
		sqlDB.Exec(t, `DROP TABLE t`)
	})

	// If the target table for a DROP does not exist, we ignore the statement.
	t.Run("table does not exist", func(t *testing.T) {
		// Set up table `t` does not exist for testing.
		sqlDB.Exec(t, `DROP TABLE IF EXISTS t;`)

		// No error should be thrown with DROP statement.
		data = `DROP TABLE t; CREATE TABLE t (a INT); INSERT INTO t VALUES (4);`
		expected := [][]string{{"4"}}

		sqlDB.Exec(t, `IMPORT PGDUMP ($1)`, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, expected)

		// Drop the table `t` that pgdump imported.
		// Now table `t` does not exist for the IF EXISTS example.
		sqlDB.Exec(t, `DROP TABLE t;`)

		// Also expect no errors and successful import with IF EXISTS.
		data = `DROP TABLE IF EXISTS t; CREATE TABLE t (a INT); INSERT INTO t VALUES (4);`
		sqlDB.Exec(t, `IMPORT PGDUMP ($1)`, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, expected)

		// Cleanup.
		sqlDB.Exec(t, `DROP TABLE t`)
	})

	t.Run("multiple tables and drops", func(t *testing.T) {
		// Set up.
		sqlDB.Exec(t, `DROP TABLE IF EXISTS t, u;`)

		// Import table `t` successfully.
		data = `DROP TABLE t; CREATE TABLE t (a INT)`
		sqlDB.Exec(t, `IMPORT PGDUMP ($1)`, srv.URL)

		// Table `u` does not exist, so create it successfully.
		// Table `t` exists, so an error is thrown for table `t`.
		data = `DROP TABLE u;
		CREATE TABLE u (a INT);
		INSERT INTO u VALUES (55);
		DROP TABLE t;`
		sqlDB.ExpectErr(t, `drop table "t" and then retry the import`, `IMPORT PGDUMP ($1)`, srv.URL)

		// Since the PGDump failed on error, table `u` should not exist.
		sqlDB.ExpectErr(t, `does not exist`, `SELECT * FROM u`)
	})
}

func TestImportPgDumpSchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodes = 1
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "pgdump")
	args := base.TestServerArgs{ExternalIODir: baseDir}

	// Simple schema test which creates 3 schemas with a single `test` table in
	// each schema.
	t.Run("schema.sql", func(t *testing.T) {
		tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(ctx)
		conn := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(conn)

		sqlDB.Exec(t, `CREATE DATABASE schemadb; SET DATABASE = schemadb`)
		sqlDB.Exec(t, "IMPORT PGDUMP 'nodelocal://0/schema.sql' WITH ignore_unsupported_statements")

		// Check that we have imported 4 schemas.
		expectedSchemaNames := [][]string{{"bar"}, {"baz"}, {"foo"}, {"public"}}
		sqlDB.CheckQueryResults(t,
			`SELECT schema_name FROM [SHOW SCHEMAS] WHERE owner IS NOT NULL ORDER BY schema_name`,
			expectedSchemaNames)

		// Check that we have a test table in each schema with the expected content.
		expectedContent := [][]string{{"1", "abc"}, {"2", "def"}}
		expectedTableName := "test"
		expectedTableName2 := "test2"
		expectedSeqName := "testseq"
		sqlDB.CheckQueryResults(t, `SELECT schema_name,
	table_name FROM [SHOW TABLES] ORDER BY (schema_name, table_name)`,
			[][]string{
				{"bar", expectedTableName},
				{"bar", expectedTableName2},
				{"bar", expectedSeqName},
				{"baz", expectedTableName},
				{"foo", expectedTableName},
				{"public", expectedTableName},
			})

		for _, schemaCollection := range expectedSchemaNames {
			for _, schema := range schemaCollection {
				sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %s.%s`, schema, expectedTableName),
					expectedContent)
			}
		}

		// There should be two jobs, the import and a job updating the parent
		// database descriptor.
		sqlDB.CheckQueryResults(t, `SELECT job_type, status FROM [SHOW JOBS] ORDER BY job_type`,
			[][]string{{"IMPORT", "succeeded"}, {"SCHEMA CHANGE", "succeeded"}})

		// Attempt to rename one of the imported schema's so as to verify that
		// parent database descriptor has been updated with information about the
		// imported schemas.
		sqlDB.Exec(t, `ALTER SCHEMA foo RENAME TO biz`)

		// Ensure that FK relationship works fine with UDS.
		sqlDB.Exec(t, `INSERT INTO bar.test VALUES (100, 'a')`)
		sqlDB.ExpectErr(t, "violates foreign key constraint \"testfk\"", `INSERT INTO bar.test2 VALUES (101, 'a')`)
	})

	t.Run("target-table-schema.sql", func(t *testing.T) {
		tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(ctx)
		conn := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(conn)

		sqlDB.Exec(t, `CREATE DATABASE schemadb; SET DATABASE = schemadb`)
		sqlDB.ExpectErr(t, "does not exist: \"schemadb.bar.test\"",
			"IMPORT TABLE schemadb.bar.test FROM PGDUMP ('nodelocal://0/schema."+
				"sql') WITH ignore_unsupported_statements")

		// Create the user defined schema so that we can get past the "not found"
		// error.
		// We still expect an error as we do not support importing a target table in
		// a UDS.
		sqlDB.Exec(t, `CREATE SCHEMA bar`)
		sqlDB.ExpectErr(t, "cannot use IMPORT with a user defined schema",
			"IMPORT TABLE schemadb.bar.test FROM PGDUMP ('nodelocal://0/schema."+
				"sql') WITH ignore_unsupported_statements")

		// We expect the import of a target table in the public schema to work.
		for _, target := range []string{"schemadb.public.test", "schemadb.test", "test"} {
			sqlDB.Exec(t, fmt.Sprintf("IMPORT TABLE %s FROM PGDUMP ('nodelocal://0/schema."+
				"sql') WITH ignore_unsupported_statements", target))

			// Check that we have a test table in each schema with the expected content.
			expectedContent := [][]string{{"1", "abc"}, {"2", "def"}}
			expectedTableName := "test"
			sqlDB.CheckQueryResults(t, `SELECT schema_name,
	table_name FROM [SHOW TABLES] ORDER BY (schema_name, table_name)`,
				[][]string{{"public", expectedTableName}})

			// Check that the target table in the public schema was imported correctly.
			sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %s`, expectedTableName), expectedContent)

			sqlDB.Exec(t, `DROP TABLE schemadb.public.test`)
		}
		sqlDB.CheckQueryResults(t,
			`SELECT schema_name FROM [SHOW SCHEMAS] WHERE owner <> 'NULL' ORDER BY schema_name`,
			[][]string{{"bar"}, {"public"}})
	})

	t.Run("inject-error-ensure-cleanup", func(t *testing.T) {
		defer gcjob.SetSmallMaxGCIntervalForTest()()
		tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
		defer tc.Stopper().Stop(ctx)
		conn := tc.ServerConn(0)
		sqlDB := sqlutils.MakeSQLRunner(conn)

		beforeImport, err := tree.MakeDTimestampTZ(tc.Server(0).Clock().Now().GoTime(), time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < tc.NumServers(); i++ {
			tc.Server(i).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs =
				map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
					jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
						r := raw.(*importResumer)
						r.testingKnobs.afterImport = func(_ roachpb.RowCount) error {
							return errors.New("testing injected failure")
						}
						return r
					},
				}
		}

		sqlDB.Exec(t, `CREATE DATABASE failedimportpgdump; SET DATABASE = failedimportpgdump`)
		// Hit a failure during import.
		sqlDB.ExpectErr(
			t, `testing injected failure`, `IMPORT PGDUMP 'nodelocal://0/schema.sql' WITH ignore_unsupported_statements`,
		)
		// Nudge the registry to quickly adopt the job.
		tc.Server(0).JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

		dbID := sqlutils.QueryDatabaseID(t, sqlDB.DB, "failedimportpgdump")
		// The public schema in the database also uses an ID.
		publicSchemaID := dbID + 1
		// In the case of the test, the ID of the 3 schemas that will be cleaned up
		// due to the failed import will be consecutive IDs after the ID of the
		// empty database it was created in.
		schemaIDs := []descpb.ID{descpb.ID(publicSchemaID + 1), descpb.ID(publicSchemaID + 2), descpb.ID(publicSchemaID + 3)}
		// The table IDs are allocated after the schemas are created. There is one
		// extra table in the "public" schema.
		tableIDs := []descpb.ID{
			descpb.ID(publicSchemaID + 4), descpb.ID(publicSchemaID + 5), descpb.ID(publicSchemaID + 6),
			descpb.ID(publicSchemaID + 7),
		}

		// At this point we expect to see three jobs related to the cleanup.
		// - SCHEMA CHANGE GC job for the table cleanup.
		// - SCHEMA CHANGE job to drop the schemas.
		// - SCHEMA CHANGE job to update the database descriptor with dropped
		// schemas.

		// Ensure that a GC job was created, and wait for it to finish.
		doneGCQuery := fmt.Sprintf(
			"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '%s' AND status = '%s' AND created > %s",
			"SCHEMA CHANGE GC", jobs.StatusSucceeded, beforeImport.String(),
		)

		doneSchemaDropQuery := fmt.Sprintf(
			"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '%s' AND status = '%s' AND description"+
				" LIKE '%s'", "SCHEMA CHANGE", jobs.StatusSucceeded, "dropping schemas%")

		doneDatabaseUpdateQuery := fmt.Sprintf(
			"SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '%s' AND status = '%s' AND description"+
				" LIKE '%s'", "SCHEMA CHANGE", jobs.StatusSucceeded, "updating parent database%")

		sqlDB.CheckQueryResultsRetry(t, doneGCQuery, [][]string{{"1"}})
		sqlDB.CheckQueryResultsRetry(t, doneSchemaDropQuery, [][]string{{"1"}})
		sqlDB.CheckQueryResultsRetry(t, doneDatabaseUpdateQuery, [][]string{{"1"}})

		for _, schemaID := range schemaIDs {
			// Expect that the schema descriptor is deleted.
			if err := sql.TestingDescsTxn(ctx, tc.Server(0), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
				_, err = col.Direct().MustGetSchemaDescByID(ctx, txn, schemaID)
				if !testutils.IsError(err, "descriptor not found") {
					return err
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}

		for _, tableID := range tableIDs {
			// Expect that the table descriptor is deleted.
			if err := sql.TestingDescsTxn(ctx, tc.Server(0), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
				_, err = col.Direct().MustGetTableDescByID(ctx, txn, tableID)
				if !testutils.IsError(err, "descriptor not found") {
					return err
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}

		// As a final sanity check that the schemas have been removed.
		sqlDB.CheckQueryResults(t, `SELECT schema_name FROM [SHOW SCHEMAS] WHERE owner IS NOT NULL`,
			[][]string{{"public"}})

		// Check that the database descriptor has been updated with the removed schemas.
		sqlDB.ExpectErr(t, "unknown schema \"foo\"", `ALTER SCHEMA foo RENAME TO biz`)
	})
}

func TestImportCockroachDump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, "IMPORT PGDUMP ($1) WITH ignore_unsupported_statements", "nodelocal://0/cockroachdump/dump.sql")
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
		{"t", `CREATE TABLE public.t (
	i INT8 NOT NULL,
	t STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	INDEX t_t_idx (t ASC)
)`},
	})
	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE a", [][]string{
		{"a", `CREATE TABLE public.a (
	i INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	CONSTRAINT fk_i_ref_t FOREIGN KEY (i) REFERENCES public.t(i) NOT VALID
)`},
	})
}

func TestCreateStatsAfterImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = time.Microsecond

	const nodes = 1
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t)
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`)

	sqlDB.Exec(t, "IMPORT PGDUMP ($1) WITH ignore_unsupported_statements", "nodelocal://0/cockroachdump/dump.sql")

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
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "avro")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	simpleOcf := fmt.Sprintf("nodelocal://0/%s", "simple.ocf")
	simpleSchemaURI := fmt.Sprintf("nodelocal://0/%s", "simple-schema.json")
	simpleJSON := fmt.Sprintf("nodelocal://0/%s", "simple-sorted.json")
	simplePrettyJSON := fmt.Sprintf("nodelocal://0/%s", "simple-sorted.pjson")
	simpleBinRecords := fmt.Sprintf("nodelocal://0/%s", "simple-sorted-records.avro")

	tests := []struct {
		name   string
		sql    string
		create string
		args   []interface{}
		err    bool
	}{
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
			name:   "import-json-records",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH data_as_json_records, schema_uri=$2",
			create: "CREATE TABLE simple (i INT8, s text, b bytea)",
			args:   []interface{}{simpleJSON, simpleSchemaURI},
		},
		{
			name:   "import-json-records-into-table-ignores-extra-fields",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH data_as_json_records, schema_uri=$2",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY)",
			args:   []interface{}{simpleJSON, simpleSchemaURI},
		},
		{
			name:   "import-json-pretty-printed-records",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH data_as_json_records, schema_uri=$2",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY)",
			args:   []interface{}{simplePrettyJSON, simpleSchemaURI},
		},
		{
			name:   "import-avro-fragments",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH data_as_binary_records, records_terminated_by='', schema_uri=$2",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY)",
			args:   []interface{}{simpleBinRecords, simpleSchemaURI},
		},
		{
			name:   "fail-import-expect-ocf-got-json",
			sql:    "IMPORT INTO simple AVRO DATA ($2)",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY)",
			args:   []interface{}{simpleJSON},
			err:    true,
		},
		{
			name:   "relaxed-import-sets-missing-fields",
			sql:    "IMPORT INTO simple AVRO DATA ($1)",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea, z int)",
			args:   []interface{}{simpleOcf},
		},
		{
			name:   "strict-import-errors-missing-fields",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH strict_validation",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea, z int)",
			args:   []interface{}{simpleOcf},
			err:    true,
		},
		{
			name:   "strict-import-errors-extra-fields",
			sql:    "IMPORT INTO simple AVRO DATA ($1) WITH strict_validation",
			create: "CREATE TABLE simple (i INT8 PRIMARY KEY)",
			args:   []interface{}{simpleOcf},
			err:    true,
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

	t.Run("user-defined-schemas", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE SCHEMA myschema`)
		sqlDB.Exec(t, `CREATE TABLE myschema.simple (i INT8 PRIMARY KEY, s text, b bytea)`)
		sqlDB.Exec(t, `IMPORT INTO myschema.simple (i, s, b) AVRO DATA ($1)`, simpleOcf)
		var numRows int
		sqlDB.QueryRow(t, `SELECT count(*) FROM myschema.simple`).Scan(&numRows)
		require.True(t, numRows > 0)
	})
}

// TestImportClientDisconnect ensures that an import job can complete even if
// the client connection which started it closes. This test uses a helper
// subprocess to force a closed client connection without needing to rely
// on the driver to close a TCP connection. See TestImportClientDisconnectHelper
// for the subprocess.
func TestImportClientDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := serverutils.StartNewTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	// TODO(dt): add this to testcluster interface and uncomment.
	// tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)

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
	runner.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v STRING)")

	// Kick off the import on a new connection which we're going to close.
	done := make(chan struct{})
	ctxToCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer close(done)
		connCfg, err := pgx.ParseConfig(pgURL.String())
		assert.NoError(t, err)
		db, err := pgx.ConnectConfig(ctx, connCfg)
		assert.NoError(t, err)
		defer func() { _ = db.Close(ctx) }()
		_, err = db.Exec(ctxToCancel, `IMPORT INTO foo CSV DATA ($1)`, srv.URL)
		assert.Equal(t, context.Canceled, errors.Unwrap(err))
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
	defer log.Scope(t).Close(t)

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

func waitForJobResult(
	t *testing.T, tc serverutils.TestClusterInterface, id jobspb.JobID, expected jobs.Status,
) {
	// Force newly created job to be adopted and verify its result.
	tc.Server(0).JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	testutils.SucceedsSoon(t, func() error {
		var unused int64
		return tc.ServerConn(0).QueryRow(
			"SELECT job_id FROM [SHOW JOBS] WHERE job_id = $1 AND status = $2",
			id, expected).Scan(&unused)
	})
}

func TestDetachedImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "avro")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	connDB := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(connDB)

	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)
	sqlDB.Exec(t, "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea)")

	simpleOcf := fmt.Sprintf("nodelocal://0/%s", "simple.ocf")

	importIntoQuery := `IMPORT INTO simple AVRO DATA ($1)`
	importIntoQueryDetached := importIntoQuery + " WITH DETACHED"

	// DETACHED import w/out transaction is okay.
	var jobID jobspb.JobID
	sqlDB.QueryRow(t, importIntoQueryDetached, simpleOcf).Scan(&jobID)
	waitForJobResult(t, tc, jobID, jobs.StatusSucceeded)

	sqlDB.Exec(t, "DROP table simple")
	sqlDB.Exec(t, "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea)")

	// Running import under transaction requires DETACHED option.
	importWithoutDetached := func(txn *gosql.Tx) error {
		return txn.QueryRow(importIntoQuery, simpleOcf).Scan(&jobID)
	}
	err := crdb.ExecuteTx(ctx, connDB, nil, importWithoutDetached)
	require.True(t,
		testutils.IsError(err, "IMPORT cannot be used inside a transaction without DETACHED option"))

	// We can execute IMPORT under transaction with detached option.
	importWithDetached := func(txn *gosql.Tx) error {
		return txn.QueryRow(importIntoQueryDetached, simpleOcf).Scan(&jobID)
	}
	err = crdb.ExecuteTx(ctx, connDB, nil, importWithDetached)
	require.NoError(t, err)
	waitForJobResult(t, tc, jobID, jobs.StatusSucceeded)

	sqlDB.Exec(t, "DROP table simple")
	sqlDB.Exec(t, "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea)")

	// Detached import should fail when the table already exists.
	sqlDB.QueryRow(t, importIntoQueryDetached, simpleOcf).Scan(&jobID)
	waitForJobResult(t, tc, jobID, jobs.StatusSucceeded)
	sqlDB.QueryRow(t, importIntoQueryDetached, simpleOcf).Scan(&jobID)
	waitForJobResult(t, tc, jobID, jobs.StatusFailed)

	sqlDB.Exec(t, "DROP table simple")
	sqlDB.Exec(t, "CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea)")

	// Detached import into should fail when there are key collisions.
	sqlDB.QueryRow(t, importIntoQueryDetached, simpleOcf).Scan(&jobID)
	waitForJobResult(t, tc, jobID, jobs.StatusSucceeded)
	sqlDB.QueryRow(t, importIntoQueryDetached, simpleOcf).Scan(&jobID)
	waitForJobResult(t, tc, jobID, jobs.StatusFailed)
}

func TestImportRowErrorLargeRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			return
		}
		_, _ = w.Write([]byte("firstrowvalue\nsecondrow,is,notok,"))
		// Write 8MB field as the last field of the second
		// row.
		bigData := randutil.RandBytes(rng, 8<<20)
		_, _ = w.Write(bigData)
		_, _ = w.Write([]byte("\n"))
	}))
	defer srv.Close()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	connDB := tc.ServerConn(0)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(connDB)
	// Our input file has an 8MB row
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.raft.command.max_size = '4MiB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)
	sqlDB.Exec(t, "CREATE TABLE simple (s string)")
	defer sqlDB.Exec(t, "DROP table simple")

	importIntoQuery := `IMPORT INTO simple CSV DATA ($1)`
	// Without truncation this would fail with:
	// pq: job 715036628973879297: could not mark as reverting: job-update: command is too large: 33561185 bytes (max: 4194304)
	sqlDB.ExpectErr(t, ".*error parsing row 2: expected 1 fields, got 4.*-- TRUNCATED", importIntoQuery, srv.URL)
}

func TestImportJobEventLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)

	defer jobs.TestingSetProgressThresholds()()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := testutils.TestDataPath(t, "avro")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	args.Knobs = base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}
	params := base.TestClusterArgs{ServerArgs: args}
	tc := serverutils.StartNewTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)

	var forceFailure bool
	for i := 0; i < tc.NumServers(); i++ {
		tc.Server(i).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeImport: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*importResumer)
				r.testingKnobs.afterImport = func(_ roachpb.RowCount) error {
					if forceFailure {
						return errors.New("testing injected failure")
					}
					return nil
				}
				return r
			},
		}
	}

	connDB := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(connDB)

	simpleOcf := fmt.Sprintf("nodelocal://0/%s", "simple.ocf")

	// First, let's test the happy path. Start a job, allow it to succeed and check
	// the event log for the entries.
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)
	beforeImport := timeutil.Now()
	createQuery := `CREATE TABLE simple (i INT8 PRIMARY KEY, s text, b bytea)`
	importQuery := `IMPORT INTO simple AVRO DATA ($1)`

	var jobID int64
	var unused interface{}
	sqlDB.Exec(t, createQuery)
	sqlDB.QueryRow(t, importQuery, simpleOcf).Scan(&jobID, &unused, &unused, &unused, &unused,
		&unused)

	expectedStatus := []string{string(jobs.StatusSucceeded), string(jobs.StatusRunning)}
	jobstest.CheckEmittedEvents(t, expectedStatus, beforeImport.UnixNano(), jobID, "import", "IMPORT")

	sqlDB.Exec(t, `DROP TABLE simple`)

	// Now let's test the events that are emitted when a job fails.
	forceFailure = true
	beforeSecondImport := timeutil.Now()
	sqlDB.Exec(t, createQuery)
	sqlDB.ExpectErrSucceedsSoon(t, "testing injected failure", importQuery, simpleOcf)

	row := sqlDB.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE status = 'failed'")
	row.Scan(&jobID)

	expectedStatus = []string{
		string(jobs.StatusFailed), string(jobs.StatusReverting),
		string(jobs.StatusRunning),
	}
	jobstest.CheckEmittedEvents(t, expectedStatus, beforeSecondImport.UnixNano(), jobID, "import", "IMPORT")
}

func TestImportDefautIntSizeSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := serverutils.StartNewTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	intSizes := []int32{4, 8}

	data := `
CREATE TABLE default_int (
    a INTEGER PRIMARY KEY,
    b INT
);
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "import pgdump",
			query: `IMPORT PGDUMP ($1)`,
		},
		{
			name:  "import table from pgdump",
			query: `IMPORT TABLE default_int FROM PGDUMP ($1)`,
		},
		{
			name:  "import mysqldump",
			query: `IMPORT MYSQLDUMP ($1)`,
		},
		{
			name:  "import table from mysqldump",
			query: `IMPORT TABLE default_int FROM MYSQLDUMP ($1)`,
		},
	}

	for _, test := range tests {
		for _, defaultIntSize := range intSizes {
			t.Run(fmt.Sprintf("%s_intsize=%d", test.name, defaultIntSize), func(t *testing.T) {
				// Drop previously imported table, if it exists.
				sqlDB.Exec(t, `DROP TABLE IF EXISTS default_int`)

				// Set default int size
				sqlDB.Exec(t, fmt.Sprintf("set cluster setting sql.defaults.default_int_size = %d;", defaultIntSize))
				sqlDB.Exec(t, fmt.Sprintf("set default_int_size = %d;", defaultIntSize))

				sqlDB.Exec(t, test.query, srv.URL)

				// Verify that the columns have the expected data type
				colDataTypes := sqlDB.QueryStr(t, "SELECT data_type FROM [SHOW COLUMNS FROM default_int] WHERE column_name != 'rowid'")
				for _, d := range colDataTypes {
					require.Equal(t, fmt.Sprintf("INT%d", defaultIntSize), d[0])
				}
			})
		}
	}
}

func TestUDTChangeDuringImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()

	// Write some data to the test file.
	f, err := ioutil.TempFile(baseDir, "data")
	require.NoError(t, err)
	_, err = f.Write([]byte("1,hello\n2,hi\n"))
	require.NoError(t, err)

	importStmt := "IMPORT INTO t (a, b) CSV DATA ($1)"
	importArgs := fmt.Sprintf("nodelocal://0/%s", filepath.Base(f.Name()))

	testCases := []struct {
		name                string
		query               string
		expectTypeChangeErr string
		expectImportErr     bool
	}{
		{
			"add-value",
			"ALTER TYPE d.greeting ADD VALUE 'cheers'",
			"",
			true,
		},
		{
			"rename-value",
			"ALTER TYPE d.greeting RENAME VALUE 'howdy' TO 'hola';",
			"",
			true,
		},
		{
			"add-value-in-txn",
			"BEGIN; ALTER TYPE d.greeting ADD VALUE 'cheers'; COMMIT;",
			"",
			true,
		},
		// Dropping a value does change the modification time on the descriptor,
		// even though the enum value removal is forbidden during an import.
		// As a result of this, the import is expected to fail.
		{
			"drop-value",
			"ALTER TYPE d.greeting DROP VALUE 'howdy';",
			"could not validate enum value removal",
			true,
		},
		// Dropping a type does not change the modification time on the descriptor,
		// and so the import is expected to succeed.
		{
			"drop-type",
			"DROP TYPE d.greeting",
			"cannot drop type \"greeting\"",
			false,
		},
		{
			"use-in-table",
			"CREATE TABLE d.foo (i INT PRIMARY KEY, j d.greeting)",
			"",
			false,
		},
		{
			"grant",
			"CREATE USER u; GRANT USAGE ON TYPE d.greeting TO u;",
			"",
			false,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			requestReceived := make(chan struct{})
			allowResponse := make(chan struct{})
			tc := serverutils.StartNewTestCluster(
				t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
					ExternalIODir: baseDir,
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
						Store: &kvserver.StoreTestingKnobs{
							TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
							TestingRequestFilter: func(ctx context.Context, br roachpb.BatchRequest) *roachpb.Error {
								for _, ru := range br.Requests {
									switch ru.GetInner().(type) {
									case *roachpb.AddSSTableRequest:
										<-requestReceived
									}
								}
								return nil
							},
						},
					},
				}})
			defer tc.Stopper().Stop(ctx)
			conn := tc.ServerConn(0)
			sqlDB := sqlutils.MakeSQLRunner(conn)

			// Create a database with a type.
			sqlDB.Exec(t, `
CREATE DATABASE d;
USE d;
CREATE TYPE d.greeting AS ENUM ('hello', 'howdy', 'hi');
CREATE TABLE t (a INT, b greeting);
`)

			// Start the import.
			errCh := make(chan error)
			defer close(errCh)
			go func() {
				_, err := sqlDB.DB.ExecContext(ctx, importStmt, importArgs)
				errCh <- err
			}()

			// Wait for the import to start.
			requestReceived <- struct{}{}

			if test.expectTypeChangeErr != "" {
				sqlDB.ExpectErr(t, test.expectTypeChangeErr, test.query)
			} else {
				sqlDB.Exec(t, test.query)
			}

			// Allow the import to finish.
			close(requestReceived)
			close(allowResponse)

			err := <-errCh
			if test.expectImportErr {
				testutils.IsError(err,
					"unsafe to import since the type has changed during the course of the import")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
