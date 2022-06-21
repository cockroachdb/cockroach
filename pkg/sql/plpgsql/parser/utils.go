// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"

type varName struct {
	name   string
	lineNo int
}

type declareHeader struct {
	label    string
	initVars []plpgsqltree.PLpgSQLVariable
}

type forVariable struct {
	name   string
	lineNo int
	scalar plpgsqltree.PLpgSQLDatum
	row    plpgsqltree.PLpgSQLDatum
}

type loopBody struct {
	stmts            []plpgsqltree.PLpgSQLStatement
	endLabel         string
	endLabelLocation int
}

type plWord struct {
	ident  string
	quoted bool
}

type plCWord struct {
	idents []string
}

type plWDatum struct {
	datum  *plpgsqltree.PLpgSQLDatum
	ident  string
	quoted bool
	idents []string
}
