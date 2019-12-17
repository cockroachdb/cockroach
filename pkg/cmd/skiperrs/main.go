// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// skiperrs connects to a postgres-compatible server with its URL specified as
// the first argument. It then splits stdin into SQL statements and executes
// them on the connection. Errors are printed but do not stop execution.
package main

import (
	gosql "database/sql"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/cr2pg/sqlstream"
	"github.com/lib/pq"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("usage: %s <url>\n", os.Args[0])
		os.Exit(1)
	}
	url := os.Args[1]

	connector, err := pq.NewConnector(url)
	if err != nil {
		log.Fatal(err)
	}
	db := gosql.OpenDB(connector)
	defer db.Close()

	stream := sqlstream.NewStream(os.Stdin)
	for {
		stmt, err := stream.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		if _, err := db.Exec(stmt.String()); err != nil {
			fmt.Println(err)
		}
	}
}
