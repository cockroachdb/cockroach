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

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// PLpgSQLStmtCounter is used to accurately report telemetry for plpgsql
// statements. We can not use the counters due to counters needing to
// be reset after every statement using reporter.ReportDiagnostics.
type PLpgSQLStmtCounter map[string]int

func (p *PLpgSQLStmtCounter) String() string {
	var buf strings.Builder
	counter := *p

	// Sort the counters to avoid flakes in test
	keys := make([]string, 0)
	for k := range counter {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		buf.WriteString(fmt.Sprintf("%s: %d\n", k, counter[k]))

	}
	return buf.String()
}

type plpgsqlVisitor struct {
	ctx  context.Context
	stmt PLpgSQLStatement
	// StmtCnt captures telemetry for plpgsql statements that can be returned
	// during parser test.
	StmtCnt PLpgSQLStmtCounter
	// Err captures errors while stmts are being walked.
	Err error
}

func MakePLpgSQLVisitor() plpgsqlVisitor {
	return plpgsqlVisitor{ctx: context.Background(), StmtCnt: PLpgSQLStmtCounter{}}
}

type walkableStmt interface {
	PLpgSQLStatement
	walkStmt(plpgsqlVisitor) PLpgSQLStatement
}

var _ walkableStmt = &PLpgSQLStmtIf{}

// WalkStmt walks plpgsql statements to ensure that telemetry is captured for nested statements.
func WalkStmt(v plpgsqlVisitor, stmt PLpgSQLStatement) (newStmt PLpgSQLStatement, changed bool) {
	wStmt, isWalkable := stmt.(walkableStmt)

	if !isWalkable {
		IncrementPlpgCounter(stmt, v)
		return stmt, false
	}

	newStmt = wStmt.walkStmt(v)
	return newStmt, true
}

// IncrementPlpgCounter
func IncrementPlpgCounter(stmt PLpgSQLStatement, v plpgsqlVisitor) {
	taggedStmt, ok := stmt.(TaggedPLpgSQLStatement)
	if !ok {
		v.Err = errors.AssertionFailedf("no tag found for stmt %q", stmt)
	}
	tag := taggedStmt.PlpgSQLStatementTag()
	telemetry.Inc(sqltelemetry.PlpgsqlStmtCounter(tag))

	//Capturing telemetry for tests
	_, ok = v.StmtCnt[tag]
	if !ok {
		v.StmtCnt[tag] = 1
	} else {
		v.StmtCnt[tag]++
	}
}
