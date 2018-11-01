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

// This connects to a postgres server and crafts postgres-protocol message
// to encode its arguments into postgres' text and binary encodings. The
// result is printed as JSON "test cases" on standard out. If no arguments
// are provided, a set of default values for the specified data type will
// be sent. If arguments are provided, they will be sent as the values.
//
// The target postgres server must accept plaintext (non-ssl) connections from
// the postgres:postgres account. A suitable server can be started with:
//
// `docker run -p 127.0.0.1:5432:5432 postgres`
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
		binary, err := pgconnect.Connect(ctx, sql, *postgresAddr, *postgresUser, pgwirebase.FormatBinary)
		if err != nil {
			log.Println(err)
		}
		data = append(data, entry{
			SQL:    expr,
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
	Text   string
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
		"Binary": {{.Binary | binary}}
	}
{{- end}}
]
`

var inputs = map[string][]string{
	"'%s'::decimal": {
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
	},

	"'%s'::timestamp": {
		"1999-01-08 04:05:06",
		"2004-10-19 10:23:54",
		"0001-01-01 00:00:00",
		"0004-10-19 10:23:54",
		"4004-10-19 10:23:54",
		"9004-10-19 10:23:54",
	},

	"'%s'::date": {
		"1999-01-08",
		"0009-01-08",
		"9999-01-08",
		"1999-12-30",
		"1996-02-29",
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
	},
}
