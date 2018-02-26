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

// This connects to a postgres server and crafts postgres-protocol message to
// encode its arguments into postgres' binary encoding. The result is printed
// as JSON "test cases" on standard out. If no arguments are provided, a set
// of default values for the specified data type will be sent. If arguments
// are provided, they will be sent as the values.
//
// The target postgres server must accept plaintext (non-ssl) connections from
// the postgres:postgres account. A suitable server can be started with:
//
// `docker run -p 127.0.0.1:5432:5432 postgres`
package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

var postgresAddr = flag.String("addr", "localhost:5432", "Postgres server address")

func main() {
	flag.Parse()

	var inputs []string
	var generator generateEnc
	if flags := len(flag.Args()); flags == 0 {
		println("usage: generate <type> [<inputs>]")
		os.Exit(1)
	} else {
		var err error
		generator, inputs, err = getType(flag.Args()[0])
		if err != nil {
			panic(err)
		}
		if flags > 1 {
			inputs = flag.Args()[1:]
		}
	}

	// This code "manually" produces JSON to avoid the inconvenience where the
	// json package insists on serializing byte arrays as base64-encoded
	// strings, and integer arrays with each member on a separate line. We want
	// integer array-looking output with all members on the same line.
	var buf bytes.Buffer
	buf.WriteString("[\n")
	for i, input := range inputs {
		enc, err := generator(*postgresAddr, input)
		if err != nil {
			panic(fmt.Errorf("%s: %s", input, err))
		}
		if i > 0 {
			buf.WriteString(`,
`)
		}
		buf.WriteString(`	{
		"In": `)
		fmt.Fprintf(&buf, "%q", input)
		buf.WriteString(`,
		"Expect": [`)
		for i, e := range enc {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprint(&buf, e)
		}
		buf.WriteString(`]
	}`)
	}
	buf.WriteString(`
]`)
	fmt.Println(buf.String())
}

func getType(typ string) (generateEnc, []string, error) {
	typ = strings.ToLower(typ)
	vals, ok := defaultVals[typ]
	if !ok {
		return nil, nil, fmt.Errorf("unknown data type %q", typ)
	}
	return makeEncodingFunc(typ), vals, nil
}

type generateEnc func(addr, val string) ([]byte, error)

var defaultVals = map[string][]string{
	"decimal":     decimalInputs,
	"timestamp":   timestampInputs,
	"timestamptz": timestampInputs,
	"date":        dateInputs,
	"time":        timeInputs,
	"inet":        inetInputs,
	"jsonb":       jsonbInputs,
	"uuid[]":      arrayUUIDInputs,
	"decimal[]":   arrayDecimalInputs,
}

var decimalInputs = []string{
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
}

var timestampInputs = []string{
	"1999-01-08 04:05:06",
	"2004-10-19 10:23:54",
	"0001-01-01 00:00:00",
	"0004-10-19 10:23:54",
	"4004-10-19 10:23:54",
	"9004-10-19 10:23:54",
}

var dateInputs = []string{
	"1999-01-08",
	"0009-01-08",
	"9999-01-08",
	"1999-12-30",
	"1996-02-29",
}

var timeInputs = []string{
	"00:00:00",
	"12:00:00.000001",
	"23:59:59.999999",
}

var inetInputs = []string{
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
}

var jsonbInputs = []string{
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
}

var arrayUUIDInputs = []string{
	"{00000000-0000-0000-0000-000000000000}",
	"{9753b405-88c0-4e93-b6c3-4e49fff11b57}",
	"{be18196d-b20a-4df2-8a2b-259c22842ee8,e0794335-6d39-47d9-b836-1f2ff349bf5d}",
}

var arrayDecimalInputs = []string{
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
}

func makeEncodingFunc(typName string) generateEnc {
	return func(addr, val string) ([]byte, error) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		read := func() ([]byte, error) {
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			return buf[:n], err
		}

		connect := []byte{0, 0, 0, 41, 0, 3, 0, 0, 117, 115, 101, 114, 0, 112, 111, 115, 116, 103, 114, 101, 115, 0, 100, 97, 116, 97, 98, 97, 115, 101, 0, 112, 111, 115, 116, 103, 114, 101, 115, 0, 0}
		if _, err := conn.Write(connect); err != nil {
			return nil, err
		}
		if b, err := read(); err != nil {
			panic(fmt.Errorf("%s: %q", err, b))
		}

		q := fmt.Sprintf("SELECT '%s'::%s", val, typName)
		var buf bytes.Buffer
		buf.WriteByte(0) // destination prepared statement
		buf.WriteString(q)
		buf.WriteByte(0) // string NUL
		buf.WriteByte(0) // MSB number of parameters
		buf.WriteByte(0) // LSB number of parameters
		{
			b := append([]byte{'P', 0, 0, 0, byte(4 + buf.Len())}, buf.Bytes()...)
			b = append(b, []byte{68, 0, 0, 0, 6, 83, 0, 83, 0, 0, 0, 4}...) // DESCRIBE
			if _, err := conn.Write(b); err != nil {
				return nil, err
			}
		}
		if b, err := read(); err != nil {
			panic(fmt.Errorf("%s: %q", err, b))
		}

		bindExec := []byte{66, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83, 0, 0, 0, 4}
		if _, err := conn.Write(bindExec); err != nil {
			return nil, err
		}
		if b, err := read(); err != nil {
			return b, err
		} else if !bytes.Equal(b[:6], []byte{50, 0, 0, 0, 4, 68}) {
			return nil, fmt.Errorf("unexpected: %v, %s", b, b)
		} else {
			idx := 12
			end := bytes.LastIndex(b, []byte{67, 0, 0, 0})
			return b[idx:end], nil
		}
	}
}
