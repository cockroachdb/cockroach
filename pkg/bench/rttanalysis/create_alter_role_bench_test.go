// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import "testing"

func BenchmarkCreateRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("CreateRole", []RoundTripBenchTestCase{
		{
			Name:  "create role with no options",
			Stmt:  "CREATE ROLE rolea",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "create role with 1 option",
			Stmt:  "CREATE ROLE rolea LOGIN",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "create role with 2 options",
			Stmt:  "CREATE ROLE rolea LOGIN CREATEROLE",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "create role with 3 options",
			Stmt:  "CREATE ROLE rolea LOGIN CREATEROLE VALID UNTIL '2021-01-01'",
			Reset: "DROP ROLE rolea",
		},
	})
}

func BenchmarkAlterRole(b *testing.B) { reg.Run(b) }
func init() {
	reg.Register("AlterRole", []RoundTripBenchTestCase{
		{
			Name:  "alter role with 1 option",
			Setup: "CREATE ROLE rolea",
			Stmt:  "ALTER ROLE rolea CREATEROLE",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "alter role with 2 options",
			Setup: "CREATE ROLE rolea",
			Stmt:  "ALTER ROLE rolea CREATEROLE LOGIN",
			Reset: "DROP ROLE rolea",
		},
		{
			Name:  "alter role with 3 options",
			Setup: "CREATE ROLE rolea",
			Stmt:  "ALTER ROLE rolea CREATEROLE LOGIN PASSWORD '123'",
			Reset: "DROP ROLE rolea",
		},
	})
}
