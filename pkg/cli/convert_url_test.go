// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
)

func Example_convert_url() {
	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	c.RunWithArgs([]string{`convert-url`})
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--cluster`, `app`})
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--format`, `dsn`})

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
	// # Direct URL to CockroachDB:
	// postgresql://root@localhost:26257/defaultdb
	// convert-url --url postgres://foo@bar --cluster app
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://foo@bar:26257/defaultdb?options=-ccluster%3Dapp
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=defaultdb user=foo host=bar port=26257 options=-ccluster=app
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://bar:26257/defaultdb?options=-ccluster%3Dapp&user=foo
	//
	// # Direct URL to CockroachDB:
	// postgresql://foo@bar:26257/defaultdb?options=-ccluster%3Dapp
	// convert-url --url postgres://foo@bar --format dsn
	// database=defaultdb user=foo host=bar port=26257
}

func Example_convert_url_with_inline() {
	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	tmpdir, err := os.MkdirTemp("", "*")
	if err != nil {
		return
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			fmt.Println("could not remove temporary directory:", err)
		}
	}()

	oldWD, err := os.Getwd()
	if err != nil {
		fmt.Println("could not get current working directory:", err)
		return
	}
	if err := os.Chdir(tmpdir); err != nil {
		fmt.Println("could not change working directory:", err)
		return
	}
	defer func() {
		if err := os.Chdir(oldWD); err != nil {
			fmt.Println("could not restore working directory:", err)
		}
	}()

	if err := os.Mkdir("certs", 0755); err != nil {
		fmt.Println("could not create certs directory:", err)
		return
	}
	if err := os.WriteFile("certs/ca.crt", []byte("caCertContents"), 0644); err != nil {
		fmt.Println("could not write CA cert:", err)
	}
	if err := os.WriteFile("certs/client.foo.key", []byte("clientKeyContents"), 0600); err != nil {
		fmt.Println("could not write client key:", err)
	}
	if err := os.WriteFile("certs/client.foo.crt", []byte("clientCertContents"), 0644); err != nil {
		fmt.Println("could not write client cert:", err)
	}

	// Happy path
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--certs-dir`, `certs/`,
		`--user`, `foo`,
		`--password`, `secret`,
		`--database`, `mydb`,
		`--cluster`, `app`,
		`--format`, `crdb`,
		`--inline`,
	})
	// Inline defaults to --format crdb
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--certs-dir`, `certs/`,
		`--user`, `foo`,
		`--password`, `secret`,
		`--database`, `mydb`,
		`--cluster`, `app`,
		`--inline`,
	})
	// Inline fails on other formats
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--format`, `jdbc`,
		`--inline`,
	})

	// Output:
	// convert-url --url postgres://bar --certs-dir certs/ --user foo --password secret --database mydb --cluster app --format crdb --inline
	// postgresql://foo:secret@bar:26257/mydb?options=-ccluster%3Dapp&sslcert=clientCertContents&sslinline=true&sslkey=clientKeyContents&sslmode=verify-full&sslrootcert=caCertContents
	// convert-url --url postgres://bar --certs-dir certs/ --user foo --password secret --database mydb --cluster app --inline
	// postgresql://foo:secret@bar:26257/mydb?options=-ccluster%3Dapp&sslcert=clientCertContents&sslinline=true&sslkey=clientKeyContents&sslmode=verify-full&sslrootcert=caCertContents
	// convert-url --url postgres://bar --format jdbc --inline
	// ERROR: --inline only supports --format=crdb
}
