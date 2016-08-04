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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

// This program prints the expected decimal (numeric) byte results for
// TestBinaryDecimal. This program can be executed if there is a Postgres
// server listening on localhost:5432 with the postgres:postgres account. The
// results should be pasted into that file.
//
// A default set of inputs is provided. These can be overridden with further
// command line arguments.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
)

var postgresAddr = flag.String("addr", "localhost:5432", "Postgres server address")

func main() {
	flag.Parse()

	inputs := []string{
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
	if args := flag.Args(); len(args) > 0 {
		inputs = args
	}

	var buf bytes.Buffer
	buf.WriteString("[\n")
	for i, input := range inputs {
		input = strings.Trim(input, `"`)
		enc, err := getEncoding(*postgresAddr, input)
		if err != nil {
			log.Fatal(fmt.Errorf("%s: %s", input, err))
		}
		if i > 0 {
			buf.WriteString(`,
`)
		}
		buf.WriteString(`	{
		"In": "`)
		buf.WriteString(input)
		buf.WriteString(`",
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

func getEncoding(addr, num string) ([]byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	read := func() []byte {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			panic(err)
		}
		b := buf[:n]
		return b
	}

	var connect = []byte{0, 0, 0, 41, 0, 3, 0, 0, 117, 115, 101, 114, 0, 112, 111, 115, 116, 103, 114, 101, 115, 0, 100, 97, 116, 97, 98, 97, 115, 101, 0, 112, 111, 115, 116, 103, 114, 101, 115, 0, 0}
	if _, err := conn.Write(connect); err != nil {
		return nil, err
	}
	read()

	q := fmt.Sprintf("SELECT %s::numeric", num)
	var buf bytes.Buffer
	buf.WriteByte(0) // destination prepared statement
	buf.WriteString(q)
	buf.WriteByte(0) // string NUL
	buf.WriteByte(0) // MSB number of parameters
	buf.WriteByte(0) // LSB number of parameters
	b := append([]byte{'P', 0, 0, 0, byte(4 + buf.Len())}, buf.Bytes()...)
	b = append(b, []byte{68, 0, 0, 0, 6, 83, 0, 83, 0, 0, 0, 4}...) // DESCRIBE
	if _, err := conn.Write(b); err != nil {
		return nil, err
	}
	read()

	var bindExec = []byte{66, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83, 0, 0, 0, 4}
	if _, err := conn.Write(bindExec); err != nil {
		return nil, err
	}
	b = read()
	if !bytes.Equal(b[:6], []byte{50, 0, 0, 0, 4, 68}) {
		return nil, fmt.Errorf("unexpected: %v, %s", b, b)
	}
	idx := 12
	end := bytes.Index(b, []byte{67, 0, 0, 0})
	return b[idx:end], nil
}
