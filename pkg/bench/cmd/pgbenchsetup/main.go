// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/pkg/bench"
	_ "github.com/lib/pq"
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

	var db *gosql.DB
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

		db, err = bench.CreateAndConnect(*parsed, name)
	} else {
		db, err = gosql.Open("postgres", flag.Arg(0))
	}
	if err != nil {
		panic(err)
	}

	defer db.Close()

	if err := bench.SetupBenchDB(db, *accounts, false); err != nil {
		panic(err)
	}
}
