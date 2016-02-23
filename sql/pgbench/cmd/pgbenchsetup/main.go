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

	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/sql/pgbench"
)

var usage = func() {
	fmt.Fprintln(os.Stderr, "Creates the schema and initial data used by the `pgbench` tool")
	fmt.Fprintf(os.Stderr, "\nUsage: %s <db URL>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	accounts := flag.Int("accounts", 100000, "number of accounts to create")

	createDb := flag.Bool("createdb", false, "attempt to create named db, dropping first if exists")

	flag.Parse()
	flag.Usage = usage
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	if *createDb {
		name := ""
		parsed, err := url.Parse(flag.Arg(0))
		if err != nil {
			panic(err)
		} else if len(parsed.Path) < 2 { // first char is '/'
			panic("URL must include db name")
		} else {
			name = parsed.Path[1:]
		}

		// Create throw-away connection to create the DB.
		db, err := sql.Open("postgres", flag.Arg(0))
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)); err != nil {
			panic(fmt.Sprintf("Could not drop db %s: %s\n", name, err.Error()))
		}
		if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s", name)); err != nil {
			panic(fmt.Sprintf("Could not create db %s: %s\n", name, err.Error()))
		} else {
			fmt.Printf("Created database %s\n", name)
		}
	}

	db, err := sql.Open("postgres", flag.Arg(0))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := pgbench.SetupBenchDB(db, *accounts, false); err != nil {
		panic(err)
	}
}
