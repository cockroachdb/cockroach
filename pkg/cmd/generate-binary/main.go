// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This connects to a postgres server and crafts postgres-protocol message
// to encode its arguments into postgres' text and binary encodings. The
// result is printed as JSON "test cases" on standard out. If no arguments
// are provided, a set of default values for the specified data type will
// be sent. If arguments are provided, they will be sent as the values.
//
// The target postgres server must accept plaintext (non-ssl) connections from
// the postgres:postgres account. A suitable server can be started with:
//
// `docker run -p 127.0.0.1:5432:5432 postgres:11`
//
// The output of this file generates pkg/sql/pgwire/testdata/encodings.json.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cmd/cmp-protocol/pgconnect"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
)

var (
	postgresAddr = flag.String("addr", "localhost:5432", "Postgres server address")
	postgresUser = flag.String("user", "postgres", "Postgres user")

	funcMap = template.FuncMap{
		"json": func(v interface{}) (string, error) {
			b, err := json.Marshal(v)
			return string(b), err
		},
		"binary": toString,
	}
	tmpl = template.Must(template.New("json").Funcs(funcMap).Parse(outputJSON))
)

func main() {
	flag.Parse()

	var data []entry
	ctx := context.Background()

	stmts := os.Args[1:]

	if len(stmts) == 0 {
		// Sort hard coded inputs by key name.
		var formats []string
		for format := range inputs {
			formats = append(formats, format)
		}
		sort.Strings(formats)

		for _, format := range formats {
			list := inputs[format]
			for _, input := range list {
				sql := fmt.Sprintf(format, input)
				stmts = append(stmts, sql)
			}
		}
	}

	for _, expr := range stmts {
		sql := fmt.Sprintf("SELECT %s", expr)
		text, err := pgconnect.Connect(ctx, sql, *postgresAddr, *postgresUser, pgwirebase.FormatText)
		if err != nil {
			log.Fatalf("text: %s: %v", sql, err)
		}
		binary, err := pgconnect.Connect(ctx, sql, *postgresAddr, *postgresUser, pgwirebase.FormatBinary)
		if err != nil {
			log.Fatalf("binary: %s: %v", sql, err)
		}
		sql = fmt.Sprintf("SELECT pg_typeof(%s)::int", expr)
		id, err := pgconnect.Connect(ctx, sql, *postgresAddr, *postgresUser, pgwirebase.FormatText)
		if err != nil {
			log.Fatalf("oid: %s: %v", sql, err)
		}
		data = append(data, entry{
			SQL:    expr,
			Oid:    string(id),
			Text:   text,
			Binary: binary,
		})
	}

	// This code "manually" produces JSON to avoid the inconvenience where the
	// json package insists on serializing byte arrays as base64-encoded
	// strings, and integer arrays with each member on a separate line. We want
	// integer array-looking output with all members on the same line.
	if err := tmpl.Execute(os.Stdout, data); err != nil {
		log.Fatal(err)
	}
}

type entry struct {
	SQL    string
	Oid    string
	Text   []byte
	Binary []byte
}

func toString(b []byte) string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, e := range b {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprint(&buf, e)
	}
	buf.WriteString("]")
	return buf.String()
}

const outputJSON = `[
{{- range $idx, $ele := .}}
	{{- if gt $idx 0 }},{{end}}
	{
		"SQL": {{.SQL | json}},
		"Oid": {{.Oid}},
		"Text": {{printf "%q" .Text}},
		"TextAsBinary": {{.Text | binary}},
		"Binary": {{.Binary | binary}}
	}
{{- end}}
]
`

var inputs = map[string][]string{
	"'%s'::decimal": {
		"NaN",
		"-000.000",
		"-0000021234.23246346000000",
		"-1.2",
		".0",
		".1",
		".1234",
		".12345",
		"0",
		"0.",
		"0.0",
		"0.000006",
		"0.0000124000",
		"0.00005",
		"0.0004",
		"0.003",
		"0.00300",
		"0.02",
		"0.038665987681445668",
		"0.1",
		"00.00",
		"1",
		"1.000000000000006",
		"1.00000000000005",
		"1.0000000000004",
		"1.000000000003",
		"1.00000000002",
		"1.0000000001",
		"1.000000009",
		"1.00000008",
		"1.0000007",
		"1.000006",
		"1.00005",
		"1.0004",
		"1.003",
		"1.02",
		"1.1",
		"10000.000006",
		"10000.00005",
		"10000.0004",
		"10000.003",
		"10000.02",
		"10000.1",
		"1000000",
		"123",
		"12345",
		"12345.1",
		"12345.1234",
		"12345.12345",
		"2.2289971159100284",
		"3409589268520956934250.234098732045120934701239846",
		"42",
		"42.0",
		"420000",
		"420000.0",
		"6000500000000.0000000",
		"10000",
		"800000000",
		"9E+4",
		"99E100",
	},

	"'%s'::float8": {
		// The Go binary encoding of NaN differs from Postgres by a 1 at the
		// end. Go also uses Inf instead of Infinity (used by Postgres) for text
		// float encodings. These deviations are still correct, and it's not worth
		// special casing them into the code, so they are commented out here.
		//"NaN",
		"Inf",
		"-Inf",
		"-000.000",
		"-0000021234.23246346000000",
		"-1.2",
		".0",
		".1",
		".1234",
		".12345",
		fmt.Sprint(math.MaxFloat32),
		fmt.Sprint(math.SmallestNonzeroFloat32),
		fmt.Sprint(math.MaxFloat64),
		fmt.Sprint(math.SmallestNonzeroFloat64),
	},

	"'%s'::float4": {
		// The Go binary encoding of NaN differs from Postgres by a 1 at the
		// end. Go also uses Inf instead of Infinity (used by Postgres) for text
		// float encodings. These deviations are still correct, and it's not worth
		// special casing them into the code, so they are commented out here.
		//"NaN",
		"Inf",
		"-Inf",
		"-000.000",
		"-0000021234.2",
		"-1.2",
		".0",
		".1",
		".1234",
		".12345",
		"3.40282e+38",
		"1.4013e-45",
	},

	"'%s'::int2": {
		"0",
		"1",
		"-1",
		"-32768",
		"32767",
	},

	"'%s'::int4": {
		"0",
		"1",
		"-1",
		"-32768",
		"32767",
		"-2147483648",
		"2147483647",
	},

	"'%s'::int8": {
		"0",
		"1",
		"-1",
		"-32768",
		"32767",
		"-2147483648",
		"2147483647",
		"-9223372036854775808",
		"9223372036854775807",
	},

	"'%s'::char(8)": {
		"hello",
		"hello123",
	},

	`'%s'::char(8) COLLATE "en_US"`: {
		"hello",
		"hello123",
	},

	"'%s'::timestamp": {
		"1999-01-08 04:05:06+00",
		"1999-01-08 04:05:06+00:00",
		"1999-01-08 04:05:06+10",
		"1999-01-08 04:05:06+10:00",
		"1999-01-08 04:05:06+10:30",
		"1999-01-08 04:05:06",
		"2004-10-19 10:23:54",
		"0001-01-01 00:00:00",
		"0004-10-19 10:23:54",
		"0004-10-19 10:23:54 BC",
		"4004-10-19 10:23:54",
		"9004-10-19 10:23:54",
	},

	"'%s'::timestamptz": {
		"1999-01-08 04:05:06+00",
		"1999-01-08 04:05:06+00:00",
		"1999-01-08 04:05:06+10",
		"1999-01-08 04:05:06+10:00",
		"1999-01-08 04:05:06+10:30",
		"1999-01-08 04:05:06",
		"2004-10-19 10:23:54",
		"0001-01-01 00:00:00",
		"0004-10-19 10:23:54",
		"0004-10-19 10:23:54 BC",
		"4004-10-19 10:23:54",
		"9004-10-19 10:23:54",
	},

	"'%s'::timetz": {
		"04:05:06+00",
		"04:05:06+00:00",
		"04:05:06+10",
		"04:05:06+10:00",
		"04:05:06+10:30",
		"04:05:06",
		"10:23:54",
		"00:00:00",
		"10:23:54",
		"10:23:54 BC",
		"10:23:54",
		"10:23:54+1:2:3",
		"10:23:54+1:2",
	},

	"'%s'::date": {
		"1999-01-08",
		"0009-01-08",
		"9999-01-08",
		"1999-12-30",
		"1996-02-29",
		"0001-01-01",
		"0001-12-31 BC",
		"0001-01-01 BC",
		"3592-12-31 BC",
		"4713-01-01 BC",
		"4714-11-24 BC",
		"5874897-12-31",
		"2000-01-01",
		"2000-01-02",
		"1999-12-31",
		"infinity",
		"-infinity",
		"epoch",
	},

	"'%s'::time": {
		"00:00:00",
		"12:00:00.000001",
		"23:59:59.999999",
	},

	"'%s'::interval": {
		"10y10mon",
		"10mon10d",
		"1y1mon",
		"1y1m",
		"1y",
		"1mon",
		"21d2h",
		"1w",
		"1d",
		"23:12:34",
		"21 days",
		"3h",
		"2h",
		"1h",
		"1m",
		"1s",
		"-23:00:00",
		"-10d",
		"-1mon",
		"-1mon10s",
		"-1y",
		"-1y1mon",
		"-1y1mon10s",
		"1ms",
		".2ms",
		".003ms",
		"-6s2ms",
		"-1d6s2ms",
		"-1d -6s2ms",
		"-1mon1m",
		"-1mon -1m",
		"-1d1m",
		"-1d -1m",
		"-1y1m",
		"-1y -1m",
		"3y4mon5d6ms",
		"296537y20d15h30m7s",
		"-2965y -20d -15h -30m -7s",
		"00:00:00",
		"-00:00:00",
	},

	"'%s'::inet": {
		"0.0.0.0",
		"0.0.0.0/20",
		"0.0.0.0/0",
		"255.255.255.255",
		"255.255.255.255/10",
		"::0/0",
		"::0/64",
		"::0",
		"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
		"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0",
		"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/10",
		"0.0.0.1",
		"111::fff/120",
		"127.0.0.1/10",
		"192.168.1.2",
		"192.168.1.2/16",
		"192.168.1.2/10",
		"2001:4f8:3:ba::/64",
		"2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128",
		"::ffff:1.2.3.1/120",
		"::ffff:1.2.3.1/128",
		"::ffff:1.2.3.1/120",
		"::ffff:1.2.3.1/20",
		"::1",
		"192/10",
		"192.168/23",
		"192.168./10",
	},

	"'%s'::jsonb": {
		`123`,
		`"hello"`,
		`{}`,
		`[]`,
		`0`,
		`0.0000`,
		`""`,
		`"\uD83D\uDE80"`,
		`{"\uD83D\uDE80": "hello"}`,
		`[1, 2, 3]`,
		`{"foo": 123}`,
		`{"foo": {"bar": true}}`,
		`true`,
		`false`,
		`null`,
		`[[[[true, false, null]]]]`,
		`["\u0001", "\u0041", "\u26a3", "\ud83e\udd37"]`,
	},

	"'%s'::uuid[]": {
		"{00000000-0000-0000-0000-000000000000}",
		"{9753b405-88c0-4e93-b6c3-4e49fff11b57}",
		"{be18196d-b20a-4df2-8a2b-259c22842ee8,e0794335-6d39-47d9-b836-1f2ff349bf5d}",
	},

	"'%s'::decimal[]": {
		"{-000.000,-0000021234.23246346000000,-1.2,.0,.1,.1234}",
		"{.12345,0,0.,0.0,0.000006}",
		"{0.0000124000,0.00005,0.0004,0.003,0.00300,0.02,0.038665987681445668}",
		"{0.1,00.00,1}",
		"{1.000000000000006,1.00000000000005,1.0000000000004,1.000000000003,1.00000000002,1.0000000001,1.000000009,1.00000008,1.0000007,1.000006,1.00005,1.0004,1.003,1.02,1.1}",
		"{10000.000006}",
		"{10000.00005}",
		"{10000.0004}",
		"{10000.003,10000.02,10000.1,1000000,123}",
		"{12345,12345.1,12345.1234,12345.12345}",
		"{2.2289971159100284,3409589268520956934250.234098732045120934701239846,42}",
	},

	"B'%s'": {
		"",
		"0",
		"1",
		"010",
		"00000000",
		"000000001",
		"0010101000011010101111100100011001110101100001010101",
		"00101010000110101011111001000110011101011000000101010000110101011111001000110011101011000010101011010101",
		"0010101000011010101111100100011001110101001010100001101010111110010001100111010110000100101010000110101011111001000110011101011000010101010101010000101010000110101011111001000110011101010010101000011010101111100100011001110101100001001010100001101010111110010001100111010110000101010101010100101010000110101011111001000110011101011000010010101000011010101111100100011011111111111111111111111111111111111111111111111111111111111111110111010111111111111111111111111111111111111111111111111111111111111111111000010101010000000000000000000000000000000000000000000000000000000000000000101011000010010101000011010101111100100011001110101100001010101010101101010000110101011111001000110011101011000010010101000011010101111100100011011111111111111111111111111111111111111111111111111111111111111110111010111111111111111111111111111111111111111111111111111111111111111111000010101010000000000000000000000000000000000000000000000000000000000000000101011000010010101000011010101111100100011001110101100001010101010101",
		"000000000000000000000000000000000000000000000000000000000000000",
		"0000000000000000000000000000000000000000000000000000000000000000",
		"00000000000000000000000000000000000000000000000000000000000000000",
		"000000000000000000000000000000000000000000000000000000000000001",
		"0000000000000000000000000000000000000000000000000000000000000001",
		"00000000000000000000000000000000000000000000000000000000000000001",
		"100000000000000000000000000000000000000000000000000000000000000",
		"1000000000000000000000000000000000000000000000000000000000000000",
		"10000000000000000000000000000000000000000000000000000000000000000",
		"111111111111111111111111111111111111111111111111111111111111111",
		"1111111111111111111111111111111111111111111111111111111111111111",
		"11111111111111111111111111111111111111111111111111111111111111111",
	},

	"array[%s]::text[]": {
		`NULL`,
		`NULL,NULL`,
		`1,NULL,2`,
		`''`,
		`'test'`,
		`'test with spaces'`,
		`e'\f'`,
		// byte order mark
		`e'\uFEFF'`,
		// snowman
		`e'\u2603'`,
	},

	"array[%s]": {
		`''`,
		`'\x0d53e338548082'::BYTEA`,
		`'test with spaces'::BYTEA`,
		`'name'::NAME`,
	},

	"%s": {
		`array[1,NULL]::int8[]`,
		`array[0.1,NULL]::float8[]`,
		`array[1,NULL]::numeric[]`,
		`array['test',NULL]::text[]`,
		`array['test',NULL]::name[]`,
		`array[]::int4[]`,
	},

	"(%s,null)": {
		`1::int8,2::int8,3::int8,4::int8`,
		`'test with spaces'::BYTEA`,
		`'test with spaces'::TEXT`,
		`'name'::NAME`,
		`'false'::JSONB`,
		`'{"a": []}'::JSONB`,
		`1::int4`,
		`1::int2`,
		`1::char(2)`,
		`1::char(1)`,
		`1::varchar(4)`,
		`1::text`,
		`1::char(2) COLLATE "en_US"`,
		`1::char(1) COLLATE "en_US"`,
		`1::varchar(4) COLLATE "en_US"`,
		`1::text COLLATE "en_US"`,
	},
}
