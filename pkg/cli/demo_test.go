// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func Example_demo() {
	c := NewCLITest(TestCLIParams{NoServer: true})
	defer c.Cleanup()

	defer democluster.TestingForceRandomizeDemoPorts()()

	testData := [][]string{
		{`demo`, `-e`, `show database`},
		{`demo`, `-e`, `show database`, `--no-example-database`},
		{`demo`, `-e`, `show application_name`},
		{`demo`, `--format=table`, `-e`, `show database`},
		{`demo`, `-e`, `select 1 as "1"`, `-e`, `select 3 as "3"`},
		{`demo`, `--echo-sql`, `-e`, `select 1 as "1"`},
		{`demo`, `--set=errexit=0`, `-e`, `select nonexistent`, `-e`, `select 123 as "123"`},
		{`demo`, `startrek`, `-e`, `SELECT database_name, owner FROM [show databases]`},
		{`demo`, `startrek`, `-e`, `SELECT database_name, owner FROM [show databases]`, `--format=table`},
		// Test that if we start with --insecure we cannot perform
		// commands that require a secure cluster.
		{`demo`, `-e`, `CREATE USER test WITH PASSWORD 'testpass'`},
		{`demo`, `--insecure`, `-e`, `CREATE USER test WITH PASSWORD 'testpass'`},
		{`demo`, `--geo-partitioned-replicas`, `--disable-demo-license`},
	}
	setCLIDefaultsForTests()
	// We must reset the security asset loader here, otherwise the dummy
	// asset loader that is set by default in tests will not be able to
	// find the certs that demo sets up.
	security.ResetAssetLoader()
	for _, cmd := range testData {
		// `demo` sets up a server and log file redirection, which asserts
		// that the logging subsystem has not been initialized yet. Fake
		// this to be true.
		log.TestingResetActive()
		c.RunWithArgs(cmd)
	}

	// Output:
	// demo -e show database
	// database
	// movr
	// demo -e show database --no-example-database
	// database
	// defaultdb
	// demo -e show application_name
	// application_name
	// $ cockroach demo
	// demo --format=table -e show database
	//   database
	// ------------
	//   movr
	// (1 row)
	// demo -e select 1 as "1" -e select 3 as "3"
	// 1
	// 1
	// 3
	// 3
	// demo --echo-sql -e select 1 as "1"
	// > select 1 as "1"
	// 1
	// 1
	// demo --set=errexit=0 -e select nonexistent -e select 123 as "123"
	// ERROR: column "nonexistent" does not exist
	// SQLSTATE: 42703
	// 123
	// 123
	// demo startrek -e SELECT database_name, owner FROM [show databases]
	// database_name	owner
	// defaultdb	root
	// postgres	root
	// startrek	demo
	// system	node
	// demo startrek -e SELECT database_name, owner FROM [show databases] --format=table
	//   database_name | owner
	// ----------------+--------
	//   defaultdb     | root
	//   postgres      | root
	//   startrek      | demo
	//   system        | node
	// (4 rows)
	// demo -e CREATE USER test WITH PASSWORD 'testpass'
	// CREATE ROLE
	// demo --insecure -e CREATE USER test WITH PASSWORD 'testpass'
	// ERROR: setting or updating a password is not supported in insecure mode
	// SQLSTATE: 28P01
	// demo --geo-partitioned-replicas --disable-demo-license
	// ERROR: enterprise features are needed for this demo (--geo-partitioned-replicas)
}
