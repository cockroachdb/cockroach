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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// StatementFilter2 is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
// If the callback returns an error, the session is terminated.
//
// WIP(andrei): rename
type StatementFilter2 func(ctx context.Context, query string, ev inputEvent) error

func queryOrPreparedToStatement(q queryOrPreparedStmt) (Statement, *tree.PlaceholderInfo) {
	if q.preparedStmt != nil {
		pinfo := q.pinfo
		stmt := Statement{
			AST:           q.preparedStmt.Statement,
			ExpectedTypes: q.preparedStmt.Columns,
			AnonymizedStr: q.preparedStmt.AnonymizedStr,
		}
		return stmt, pinfo
	}
	return Statement{AST: q.query}, nil
}
