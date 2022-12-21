// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
