// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
)

func printHeader(name string) {
	fmt.Println()
	fmt.Println("---------------------------------------------")
	fmt.Println(name)
	fmt.Println("---------------------------------------------")
}

func Example_convert_url() {
	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	c.RunWithArgs([]string{`convert-url`})

	printHeader("all formats with cluster")
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--cluster`, `app`})

	printHeader("format: dsn")
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--format`, `dsn`})

	printHeader("format: pq")
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--format`, `pq`})

	printHeader("format: jdbc")
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--format`, `jdbc`})

	printHeader("format: crdb with credentials")
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--user`, `foo`,
		`--password`, `secret`,
		`--format`, `crdb`,
	})

	printHeader("format: crdb with password in URL")
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo:s3cret@qux`, `--format`, `crdb`})

	printHeader("invalid format")
	c.RunWithArgs([]string{`convert-url`, `--url`, `postgres://foo@bar`, `--format`, `invalid`})

	printHeader("flag overrides")
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://foo@qux/origdb`,
		`--user`, `baz`,
		`--database`, `newdb`,
	})

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
	//
	// ---------------------------------------------
	// all formats with cluster
	// ---------------------------------------------
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
	//
	// ---------------------------------------------
	// format: dsn
	// ---------------------------------------------
	// convert-url --url postgres://foo@bar --format dsn
	// database=defaultdb user=foo host=bar port=26257
	//
	// ---------------------------------------------
	// format: pq
	// ---------------------------------------------
	// convert-url --url postgres://foo@bar --format pq
	// postgresql://foo@bar:26257/defaultdb
	//
	// ---------------------------------------------
	// format: jdbc
	// ---------------------------------------------
	// convert-url --url postgres://foo@bar --format jdbc
	// jdbc:postgresql://bar:26257/defaultdb?user=foo
	//
	// ---------------------------------------------
	// format: crdb with credentials
	// ---------------------------------------------
	// convert-url --url postgres://bar --user foo --password secret --format crdb
	// postgresql://foo:secret@bar:26257/defaultdb
	//
	// ---------------------------------------------
	// format: crdb with password in URL
	// ---------------------------------------------
	// convert-url --url postgres://foo:s3cret@qux --format crdb
	// postgresql://foo:s3cret@qux:26257/defaultdb
	//
	// ---------------------------------------------
	// invalid format
	// ---------------------------------------------
	// convert-url --url postgres://foo@bar --format invalid
	// ERROR: invalid argument "invalid" for "--format" flag: must be one of [pq dsn jdbc crdb]
	//
	// ---------------------------------------------
	// flag overrides
	// ---------------------------------------------
	// convert-url --url postgres://foo@qux/origdb --user baz --database newdb
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://baz@qux:26257/newdb
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=newdb user=baz host=qux port=26257
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://qux:26257/newdb?user=baz
	//
	// # Direct URL to CockroachDB:
	// postgresql://baz@qux:26257/newdb
}

func Example_convert_url_with_inline() {
	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	cleanup, err := setupTestCertsDir()
	if err != nil {
		fmt.Println("could not set up test certs directory:", err)
		return
	}
	defer cleanup()

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

	printHeader("inline defaults to crdb")
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

	printHeader("inline with explicit cert flags")
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--ca-cert`, `certs/ca.crt`,
		`--cert`, `certs/client.foo.crt`,
		`--key`, `certs/client.foo.key`,
		`--user`, `foo`,
		`--format`, `crdb`,
		`--inline`,
	})

	printHeader("inline fails on other formats")
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--format`, `jdbc`,
		`--inline`,
	})

	// Output:
	// convert-url --url postgres://bar --certs-dir certs/ --user foo --password secret --database mydb --cluster app --format crdb --inline
	// postgresql://foo:secret@bar:26257/mydb?options=-ccluster%3Dapp&sslcert=clientCertContents&sslinline=true&sslkey=clientKeyContents&sslmode=verify-full&sslrootcert=caCertContents
	//
	// ---------------------------------------------
	// inline defaults to crdb
	// ---------------------------------------------
	// convert-url --url postgres://bar --certs-dir certs/ --user foo --password secret --database mydb --cluster app --inline
	// postgresql://foo:secret@bar:26257/mydb?options=-ccluster%3Dapp&sslcert=clientCertContents&sslinline=true&sslkey=clientKeyContents&sslmode=verify-full&sslrootcert=caCertContents
	//
	// ---------------------------------------------
	// inline with explicit cert flags
	// ---------------------------------------------
	// convert-url --url postgres://bar --ca-cert certs/ca.crt --cert certs/client.foo.crt --key certs/client.foo.key --user foo --format crdb --inline
	// postgresql://foo@bar:26257/defaultdb?sslcert=clientCertContents&sslinline=true&sslkey=clientKeyContents&sslmode=verify-full&sslrootcert=caCertContents
	//
	// ---------------------------------------------
	// inline fails on other formats
	// ---------------------------------------------
	// convert-url --url postgres://bar --format jdbc --inline
	// ERROR: --inline only supports --format=crdb
}

func Example_convert_url_with_certs() {
	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	cleanup, err := setupTestCertsDir()
	if err != nil {
		fmt.Println("could not set up test certs directory:", err)
		return
	}
	defer cleanup()

	// Individual cert flags without --certs-dir.
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://foo@bar`,
		`--ca-cert`, `path/to/ca.crt`,
		`--cert`, `path/to/client.crt`,
		`--key`, `path/to/client.key`,
	})

	printHeader("certs-dir with ca-cert override")
	c.RunWithArgs([]string{
		`convert-url`,
		`--url`, `postgres://bar`,
		`--user`, `foo`,
		`--certs-dir`, `certs/`,
		`--ca-cert`, `custom/ca.crt`,
	})

	// Output:
	// convert-url --url postgres://foo@bar --ca-cert path/to/ca.crt --cert path/to/client.crt --key path/to/client.key
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://foo@bar:26257/defaultdb?sslcert=path%2Fto%2Fclient.crt&sslkey=path%2Fto%2Fclient.key&sslmode=verify-full&sslrootcert=path%2Fto%2Fca.crt
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=defaultdb user=foo host=bar port=26257 sslcert=path/to/client.crt sslkey=path/to/client.key sslrootcert=path/to/ca.crt sslmode=verify-full
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://bar:26257/defaultdb?sslcert=path%2Fto%2Fclient.crt&sslkey=path%2Fto%2Fclient.key&sslmode=verify-full&sslrootcert=path%2Fto%2Fca.crt&user=foo
	//
	// # Direct URL to CockroachDB:
	// postgresql://foo@bar:26257/defaultdb?sslcert=path%2Fto%2Fclient.crt&sslkey=path%2Fto%2Fclient.key&sslmode=verify-full&sslrootcert=path%2Fto%2Fca.crt
	//
	// ---------------------------------------------
	// certs-dir with ca-cert override
	// ---------------------------------------------
	// convert-url --url postgres://bar --user foo --certs-dir certs/ --ca-cert custom/ca.crt
	// # Connection URL for libpq (C/C++), psycopg (Python), lib/pq & pgx (Go), node-postgres (JS) and most pq-compatible drivers:
	// postgresql://foo@bar:26257/defaultdb?sslcert=certs%2Fclient.foo.crt&sslkey=certs%2Fclient.foo.key&sslmode=verify-full&sslrootcert=custom%2Fca.crt
	//
	// # Connection DSN (Data Source Name) for Postgres drivers that accept DSNs - most drivers and also ODBC:
	// database=defaultdb user=foo host=bar port=26257 sslcert=certs/client.foo.crt sslkey=certs/client.foo.key sslrootcert=custom/ca.crt sslmode=verify-full
	//
	// # Connection URL for JDBC (Java and JVM-based languages):
	// jdbc:postgresql://bar:26257/defaultdb?sslcert=certs%2Fclient.foo.crt&sslkey=certs%2Fclient.foo.key&sslmode=verify-full&sslrootcert=custom%2Fca.crt&user=foo
	//
	// # Direct URL to CockroachDB:
	// postgresql://foo@bar:26257/defaultdb?sslcert=certs%2Fclient.foo.crt&sslkey=certs%2Fclient.foo.key&sslmode=verify-full&sslrootcert=custom%2Fca.crt
}

// setupTestCertsDir sets up a temp working directory with test certs.
func setupTestCertsDir() (cleanup func(), retErr error) {
	tmpdir, err := os.MkdirTemp("", "*")
	if err != nil {
		return nil, err
	}
	oldWD, err := os.Getwd()
	if err != nil {
		_ = os.RemoveAll(tmpdir)
		return nil, err
	}
	if err := os.Chdir(tmpdir); err != nil {
		_ = os.RemoveAll(tmpdir)
		return nil, err
	}
	cleanupFn := func() {
		if err := os.Chdir(oldWD); err != nil {
			fmt.Println("could not restore working directory:", err)
		}
		if err := os.RemoveAll(tmpdir); err != nil {
			fmt.Println("could not remove temporary directory:", err)
		}
	}
	defer func() {
		if retErr != nil {
			cleanupFn()
		}
	}()
	if err := os.Mkdir("certs", 0755); err != nil {
		return nil, err
	}
	for _, f := range []struct {
		name     string
		contents string
		perm     os.FileMode
	}{
		{"certs/ca.crt", "caCertContents", 0644},
		{"certs/client.foo.key", "clientKeyContents", 0600},
		{"certs/client.foo.crt", "clientCertContents", 0644},
	} {
		if err := os.WriteFile(f.name, []byte(f.contents), f.perm); err != nil {
			return nil, err
		}
	}
	return cleanupFn, nil
}
