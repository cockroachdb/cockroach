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
	// # Connection URL for CRDB streams (Physical/Logical Cluster Replication):
	// postgresql://foo@bar:26257/defaultdb?options=-ccluster%3Dsystem
}

func Example_convert_url_with_flags() {
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

	args := []string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--certs-dir`, `certs/`,
		`--username`, `foo`,
		`--password`, `secret`,
		`--database`, `mydb`,
		`--cluster`, `app`,
		`--inline`,
	}

	c.RunWithArgs(args)

	// Output:
	// convert-url --url postgres://bar --certs-dir certs/ --username foo --password secret --database mydb --cluster app --inline
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://foo:secret@bar:26257/mydb?options=-ccluster%3Dapp&sslcert=certs%2Fclient.foo.crt&sslkey=certs%2Fclient.foo.key&sslmode=verify-full&sslrootcert=certs%2Fca.crt
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=mydb user=foo host=bar port=26257 password=secret sslcert=certs/client.foo.crt sslkey=certs/client.foo.key sslrootcert=certs/ca.crt sslmode=verify-full options=-ccluster=app
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://bar:26257/mydb?options=-ccluster%3Dapp&password=secret&sslcert=certs%2Fclient.foo.crt&sslkey=certs%2Fclient.foo.key&sslmode=verify-full&sslrootcert=certs%2Fca.crt&user=foo
	//
	// # Connection URL for CRDB streams (Physical/Logical Cluster Replication):
	// postgresql://foo:secret@bar:26257/mydb?options=-ccluster%3Dapp&sslcert=clientCertContents&sslinline=true&sslkey=clientKeyContents&sslmode=verify-full&sslrootcert=caCertContents
}
