// Copyright 2018 The Cockroach Authors.
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

// cmp-protocol connects to postgres and cockroach servers and compares
// the binary and text pgwire encodings of SQL statements. Statements can
// be specified in arguments (./cmp-protocol "select 1" "select 2") or will
// be generated randomly until a difference is found.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/cmp-protocol/pgconnect"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var (
	pgAddr = flag.String("pg", "localhost:5432", "postgres address")
	pgUser = flag.String("pg-user", "postgres", "postgres user")
	crAddr = flag.String("cr", "localhost:26257", "cockroach address")
	crUser = flag.String("cr-user", "root", "cockroach user")
)

func main() {
	flag.Parse()

	stmtCh := make(chan string)
	if args := os.Args[1:]; len(args) > 0 {
		go func() {
			for _, arg := range os.Args[1:] {
				stmtCh <- arg
			}
			close(stmtCh)
		}()
	} else {
		go func() {
			rng := rand.New(rand.NewSource(0)) // time.Now().Unix()))
			for {
				typ := sqlbase.RandColumnType(rng)
				sem := typ.SemanticType
				switch sem {
				case sqlbase.ColumnType_DECIMAL, // trailing zeros differ, ok
					sqlbase.ColumnType_COLLATEDSTRING, // pg complains about utf8
					sqlbase.ColumnType_INT2VECTOR,
					sqlbase.ColumnType_OIDVECTOR,
					// tested manually below:
					sqlbase.ColumnType_ARRAY,
					sqlbase.ColumnType_TUPLE:
					continue
				}
				datum := sqlbase.RandDatum(rng, typ, false /* null ok */)
				if datum == tree.DNull {
					continue
				}
				for _, format := range []string{
					"SELECT %s;",
					"SELECT ARRAY[%s];",
					"SELECT (%s, NULL);",
				} {
					input := fmt.Sprintf(format, datum)
					stmtCh <- input
					fmt.Printf("\nTYP: %v, DATUM: %v\n", sem, datum)
				}
			}
		}()
	}

	for input := range stmtCh {
		fmt.Println("INPUT", input)
		if err := compare(os.Stdout, input, *pgAddr, *crAddr, *pgUser, *crUser); err != nil {
			fmt.Printf("sql: %s\n%+v\n", input, err)
			os.Exit(1)
		}
	}
}

func compare(w io.Writer, input, pg, cr, pgUser, crUser string) error {
	ctx := context.Background()
	for _, code := range []pgwirebase.FormatCode{
		pgwirebase.FormatText,
		pgwirebase.FormatBinary,
	} {
		// https://github.com/cockroachdb/cockroach/issues/31847
		if code == pgwirebase.FormatBinary && strings.HasPrefix(input, "SELECT (") {
			continue
		}
		results := map[string][]byte{}
		for _, s := range []struct {
			user string
			addr string
		}{
			{user: pgUser, addr: pg},
			{user: crUser, addr: cr},
		} {
			user := s.user
			addr := s.addr
			res, err := pgconnect.Connect(ctx, input, addr, user, code)
			if err != nil {
				return errors.Wrapf(err, "addr: %s, code: %s", addr, code)
			}
			fmt.Printf("INPUT: %s, ADDR: %s, CODE: %s, res: %q, res: %v\n", input, addr, code, res, res)
			for k, v := range results {
				if !bytes.Equal(res, v) {
					return errors.Errorf("format: %s\naddr: %s\nstr: %q\nbytes: %[3]v\n!=\naddr: %s\nstr: %q\nbytes: %[5]v\n", code, k, v, addr, res)
				}
			}
			results[addr] = res
		}
	}
	return nil
}
