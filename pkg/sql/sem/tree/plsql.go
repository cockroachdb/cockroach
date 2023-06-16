// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/types"

type PLFuncBody struct {
	Declarations []PLDeclaration // Can be empty
	Statements   []PLStatement   // Can be empty
}

type PLDeclaration struct {
	Ident  Name
	Typ    *types.T
	Assign *PLSQLExpr // Optional
}

type PLAssignment struct {
	Ident  Name
	Assign *PLSQLExpr
}

type PLIf struct {
	Cond *PLSQLExpr
	Then []PLStatement
	Else []PLStatement // Optional
}

type PLLoop struct {
	Cond *PLSQLExpr // Optional
	Body []PLStatement
}

type PLForLoop struct {
	Start *PLSQLExpr
	End   *PLSQLExpr
	Body  []PLStatement
}

type PLExit struct{}

type PLContinue struct{}

type PLReturn struct {
	Expr *PLSQLExpr
}

type PLSQLExpr struct {
	SQL string
}

type PLStatement interface {
	isPLStatement()
}

func (*PLAssignment) isPLStatement() {}
func (*PLIf) isPLStatement()         {}
func (*PLLoop) isPLStatement()       {}
func (*PLForLoop) isPLStatement()    {}
func (*PLExit) isPLStatement()       {}
func (*PLContinue) isPLStatement()   {}
func (*PLReturn) isPLStatement()     {}

var _ PLStatement = &PLAssignment{}
var _ PLStatement = &PLIf{}
var _ PLStatement = &PLLoop{}
var _ PLStatement = &PLForLoop{}
var _ PLStatement = &PLExit{}
var _ PLStatement = &PLContinue{}
var _ PLStatement = &PLReturn{}
