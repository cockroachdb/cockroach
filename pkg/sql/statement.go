// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Statement contains a statement with optional expected result columns and metadata.
type Statement struct {
	AST           tree.Statement
	ExpectedTypes sqlbase.ResultColumns
	AnonymizedStr string
	queryID       ClusterWideID
}

func (s Statement) String() string {
	return s.AST.String()
}

// StatementList is a list of statements.
type StatementList []Statement

// NewStatementList creates a StatementList from a tree.StatementList.
func NewStatementList(stmts tree.StatementList) StatementList {
	sl := make(StatementList, len(stmts))
	for i, s := range stmts {
		sl[i] = Statement{AST: s}
	}
	return sl
}

func (l *StatementList) String() string { return tree.AsString(l) }

// Format implements the NodeFormatter interface.
func (l *StatementList) Format(ctx *tree.FmtCtx) {
	for i := range *l {
		if i > 0 {
			ctx.WriteString("; ")
		}
		ctx.FormatNode((*l)[i].AST)
	}
}
