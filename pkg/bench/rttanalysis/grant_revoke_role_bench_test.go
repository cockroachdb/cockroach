// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkGrantRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("GrantRole", []RoundTripBenchTestCase{
		{
			Name: "grant 1 role",
			Setup: `CREATE ROLE a;
CREATE ROLE b;`,
			Stmt:  "GRANT a TO b",
			Reset: "DROP ROLE a,b",
		},
		{
			Name: "grant 2 roles",
			Setup: `CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;`,
			Stmt:  "GRANT a,b TO c",
			Reset: "DROP ROLE a,b,c",
		},
	})
}

func BenchmarkShowGrants(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("ShowGrants", []RoundTripBenchTestCase{
		{
			Name: "grant 2 roles",
			Setup: `
CREATE DATABASE db;
CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;
GRANT a TO b;
GRANT b TO c;
GRANT ALL ON DATABASE db TO c;
GRANT DROP ON DATABASE db TO b;
GRANT CONNECT ON DATABASE db TO a;
`,
			Stmt: "SHOW GRANTS ON DATABASE db FOR c",
			Reset: `
DROP DATABASE db;
DROP ROLE a,b,c;
`,
		},
		{
			Name: "grant 3 roles",
			Setup: `
CREATE DATABASE db;
CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;
CREATE ROLE d;
GRANT a TO b;
GRANT b TO c;
GRANT c TO d;
GRANT ALL ON DATABASE db TO c;
GRANT DROP ON DATABASE db TO b;
GRANT CONNECT ON DATABASE db TO a;
`,
			Stmt: "SHOW GRANTS ON DATABASE db FOR d",
			Reset: `
DROP DATABASE db;
DROP ROLE a,b,c,d;
`,
		},
		{
			Name: "grant 4 roles",
			Setup: `
CREATE DATABASE db;
CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;
CREATE ROLE d;
CREATE ROLE e;
GRANT a TO b;
GRANT b TO c;
GRANT c TO d;
GRANT d TO e;
GRANT ALL ON DATABASE db TO c;
GRANT DROP ON DATABASE db TO b;
GRANT CONNECT ON DATABASE db TO a;
`,
			Stmt: "SHOW GRANTS ON DATABASE db FOR d",
			Reset: `
DROP DATABASE db;
DROP ROLE a,b,c,d,e;
`,
		},
	})
}

func BenchmarkRevokeRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("RevokeRole", []RoundTripBenchTestCase{
		{
			Name: "revoke 1 role",
			Setup: `CREATE ROLE a;
CREATE ROLE b;
GRANT a TO b`,
			Stmt:  "REVOKE a FROM b",
			Reset: "DROP ROLE a,b",
		},
		{
			Name: "revoke 2 roles",
			Setup: `CREATE ROLE a;
CREATE ROLE b;
CREATE ROLE c;
GRANT a,b TO c;`,
			Stmt:  "REVOKE a,b FROM c",
			Reset: "DROP ROLE a,b,c",
		},
	})
}
