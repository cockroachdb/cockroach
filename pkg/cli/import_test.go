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

import "fmt"

func Example_import() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	pgdumpFilePath := "/testdata/import/simple.sql"

	c.Run(fmt.Sprintf("import table public.simple pgdump %s --skip-foreign-keys=true",
		pgdumpFilePath))
	// Output:
	// import table public.simple pgdump testdata/import/simple.sql --skip-foreign-keys=true
	//
}
