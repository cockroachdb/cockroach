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

func Example_sqlfmt() {
	c := NewCLITest(TestCLIParams{NoServer: true})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sqlfmt", "-e", ";"})
	c.RunWithArgs([]string{"sqlfmt", "-e", "delete from t"})
	c.RunWithArgs([]string{"sqlfmt", "-e", "delete from t", "-e", "update t set a = 1"})
	c.RunWithArgs([]string{"sqlfmt", "--print-width=10", "-e", "select 1,2,3 from a,b,c;;;select 4"})
	c.RunWithArgs([]string{"sqlfmt", "--print-width=10", "--align", "-e", "select 1,2,3 from a,b,c;;;select 4"})
	c.RunWithArgs([]string{"sqlfmt", "--print-width=10", "--tab-width=2", "--use-spaces", "-e", "select 1,2,3 from a,b,c;;;select 4"})
	c.RunWithArgs([]string{"sqlfmt", "-e", "select (1+2)+3"})
	c.RunWithArgs([]string{"sqlfmt", "--no-simplify", "-e", "select (1+2)+3"})

	// Output:
	// sqlfmt -e ;
	// sqlfmt -e delete from t
	// DELETE FROM t
	// sqlfmt -e delete from t -e update t set a = 1
	// DELETE FROM t;
	// UPDATE t SET a = 1;
	// sqlfmt --print-width=10 -e select 1,2,3 from a,b,c;;;select 4
	// SELECT
	// 	1,
	// 	2,
	// 	3
	// FROM
	// 	a,
	// 	b,
	// 	c;
	// SELECT 4;
	// sqlfmt --print-width=10 --align -e select 1,2,3 from a,b,c;;;select 4
	// SELECT 1,
	//        2,
	//        3
	//   FROM a,
	//        b,
	//        c;
	// SELECT 4;
	// sqlfmt --print-width=10 --tab-width=2 --use-spaces -e select 1,2,3 from a,b,c;;;select 4
	// SELECT
	//   1, 2, 3
	// FROM
	//   a, b, c;
	// SELECT 4;
	// sqlfmt -e select (1+2)+3
	// SELECT 1 + 2 + 3
	// sqlfmt --no-simplify -e select (1+2)+3
	// SELECT (1 + 2) + 3
}
