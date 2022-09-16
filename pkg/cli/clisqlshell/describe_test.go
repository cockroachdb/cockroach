// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell_test

import "github.com/cockroachdb/cockroach/pkg/cli"

func Example_describe_unknown() {
	c := cli.NewCLITest(cli.TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `-e`, `\set echo`, `-e`, `\dz`})

	// Output:
	// sql -e \set echo -e \dz
	// ERROR: unsupported command: \dz with 0 arguments
	// HINT: Use the SQL SHOW statement to inspect your schema.
	// ERROR: -e: unsupported command: \dz with 0 arguments
	// HINT: Use the SQL SHOW statement to inspect your schema.
}

var describeCmdPrefix = []string{
	`sql`, `-e`, `\set display_format csv`, `-e`, `\set echo`, `-e`,
}

func Example_describe_l() {
	c := cli.NewCLITest(cli.TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `-e`, `create database mydb`})
	c.RunWithArgs(append(describeCmdPrefix, `\l`))

	// Output:
	// sql -e create database mydb
	// CREATE DATABASE
	// sql -e \set display_format csv -e \set echo -e \l
	// > SHOW DATABASES
	// database_name,owner,primary_region,secondary_region,regions,survival_goal
	// defaultdb,root,NULL,NULL,{},NULL
	// mydb,root,NULL,NULL,{},NULL
	// postgres,root,NULL,NULL,{},NULL
	// system,node,NULL,NULL,{},NULL
}

func Example_describe_du() {
	c := cli.NewCLITest(cli.TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `-e`, `CREATE USER my_user WITH CREATEDB; GRANT admin TO my_user;`})
	c.RunWithArgs(append(describeCmdPrefix, `\du`))
	// Output:
	// sql -e CREATE USER my_user WITH CREATEDB; GRANT admin TO my_user;
	// CREATE ROLE
	// GRANT
	// sql -e \set display_format csv -e \set echo -e \du
	// > SHOW USERS
	// username,options,member_of
	// admin,,{}
	// my_user,CREATEDB,{admin}
	// root,,{admin}
}
