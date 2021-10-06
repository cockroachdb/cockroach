// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rttanalysis

import "testing"

func BenchmarkInternalExecutor(b *testing.B) {
	tests := []RoundTripBenchTestCase{
		{
			Name:  "has_schema_privilege_1_col",
			Setup: `CREATE SCHEMA s;`,
			Stmt:  `SELECT has_schema_privilege('s', 'CREATE')`,
		},
		{
			Name:  "has_schema_privilege_2_col",
			Setup: `CREATE SCHEMA s;`,
			Stmt:  `SELECT has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE')`,
		},
		{
			Name:  "has_schema_privilege_3_col",
			Setup: `CREATE SCHEMA s;`,
			Stmt:  `SELECT has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE')`,
		},
		{
			Name:  "has_schema_privilege_3_schemas_3_col",
			Setup: `CREATE SCHEMA s; CREATE SCHEMA s2; CREATE SCHEMA s3;`,
			Stmt:  `SELECT has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE')`,
		},
		{
			Name:  "has_schema_privilege_5_schemas_3_col",
			Setup: `CREATE SCHEMA s; CREATE SCHEMA s2; CREATE SCHEMA s3; CREATE SCHEMA s4; CREATE SCHEMA s5;`,
			Stmt:  `SELECT has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE'), has_schema_privilege('s', 'CREATE')`,
		},
	}

	RunRoundTripBenchmark(b, tests)
}
