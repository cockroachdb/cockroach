// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

func Example_convert_url() {
	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	c.RunWithArgs([]string{`convert-url`})
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`})

	// Output:
	// convert-url
	// # WARNING: no URL specified via --url; using a random URL as example.
	//
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://root@localhost:26257/defaultdb
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=defaultdb user=root host=localhost port=26257
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://localhost:26257/defaultdb?user=root
	//
	// convert-url --url postgres://foo@bar
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://foo@bar:26257/defaultdb
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=defaultdb user=foo host=bar port=26257
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://bar:26257/defaultdb?user=foo
	//
}
