// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec_test

import "github.com/cockroachdb/cockroach/pkg/cli"

func Example_json_sql_format() {
	c := cli.NewCLITest(cli.TestCLIParams{})
	defer c.Cleanup()

	testData := []string{
		`e'{"a": "bc"}'`,
		`e'{"a": "b\u0099c"}'`,
		`e'{"a": "b\\"c"}'`,
		`'"there are \"quotes\" in this json string"'`,
		`'""'`,
		`'{}'`,
	}

	for _, s := range testData {
		query := `SELECT ` + s + `::json`
		c.RunWithArgs([]string{"sql", "--format=sql", "-e", query})
	}

	// Output:
	// sql --format=sql -e SELECT e'{"a": "bc"}'::json
	// CREATE TABLE results (
	//   jsonb STRING
	// );
	//
	// INSERT INTO results VALUES ('{"a": "bc"}');
	// -- 1 row
	// sql --format=sql -e SELECT e'{"a": "b\u0099c"}'::json
	// CREATE TABLE results (
	//   jsonb STRING
	// );
	//
	// INSERT INTO results VALUES (e'{"a": "b\\u0099c"}');
	// -- 1 row
	// sql --format=sql -e SELECT e'{"a": "b\\"c"}'::json
	// CREATE TABLE results (
	//   jsonb STRING
	// );
	//
	// INSERT INTO results VALUES (e'{"a": "b\\"c"}');
	// -- 1 row
	// sql --format=sql -e SELECT '"there are \"quotes\" in this json string"'::json
	// CREATE TABLE results (
	//   jsonb STRING
	// );
	//
	// INSERT INTO results VALUES (e'"there are \\"quotes\\" in this json string"');
	// -- 1 row
	// sql --format=sql -e SELECT '""'::json
	// CREATE TABLE results (
	//   jsonb STRING
	// );
	//
	// INSERT INTO results VALUES ('""');
	// -- 1 row
	// sql --format=sql -e SELECT '{}'::json
	// CREATE TABLE results (
	//   jsonb STRING
	// );
	//
	// INSERT INTO results VALUES ('{}');
	// -- 1 row
}
