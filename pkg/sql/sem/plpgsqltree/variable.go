// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

type PLpgSQLDatum interface {
	plpgsqldatum()
}

type PLpgSQLVariable interface {
	PLpgSQLDatum
	pgpgsqlvariable()
}

// Scope contains all the variables defined in the DECLARE section of current statement block.
type VariableScope struct {
	Variables    []*PLpgSQLVariable
	VarNameToIdx map[string]int // mapping from variable
}
