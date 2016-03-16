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
// Author: David Taylor (david@cockroachlabs.com)

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/sql/pgbench"
	_ "github.com/cockroachdb/pq"
)

var usage = func() {
	fmt.Fprintln(os.Stderr, "Creates the schema and initial data used by the `pgbench` tool")
	fmt.Fprintf(os.Stderr, "\nUsage: %s <db URL>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	accounts := flag.Int("accounts", 100000, "number of accounts to create")

	createDb := flag.Bool("createdb", false, "attempt to create named db, dropping first if exists (must be able to connect to default db to do so).")

	flag.Parse()
	flag.Usage = usage
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	var db *sql.DB
	var err error

	if *createDb {
		name := ""
		parsed, parseErr := url.Parse(flag.Arg(0))
		if parseErr != nil {
			panic(parseErr)
		} else if len(parsed.Path) < 2 { // first char is '/'
			panic("URL must include db name")
		} else {
			name = parsed.Path[1:]
		}

		db, err = pgbench.CreateAndConnect(*parsed, name)
	} else {
		db, err = sql.Open("postgres", flag.Arg(0))
	}
	if err != nil {
		panic(err)
	}

	defer db.Close()

	if err := pgbench.SetupBenchDB(db, *accounts, false); err != nil {
		panic(err)
	}
}
