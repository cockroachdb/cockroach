// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// PLpgSQLStmtCounter is used to accurately report telemetry for plpgsql
// statements test. We can not use the telemetry counters due to them needing
// to be reset after every statement using reporter.ReportDiagnostics.
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

type telemetryVisitor struct {
	ctx context.Context
	// StmtCnt captures telemetry for plpgsql statements that can be returned
	// during parser test.
	StmtCnt PLpgSQLStmtCounter
	Err     error
}

var _ plpgsqltree.StatementVisitor = &telemetryVisitor{}

// Visit implements the StatementVisitor interface
func (v *telemetryVisitor) Visit(
	stmt plpgsqltree.Statement,
) (newStmt plpgsqltree.Statement, recurse bool) {
	tag := stmt.PlpgSQLStatementTag()
	sqltelemetry.IncrementPlpgsqlStmtCounter(tag)

	//Capturing telemetry for tests
	_, ok := v.StmtCnt[tag]
	if !ok {
		v.StmtCnt[tag] = 1
	} else {
		v.StmtCnt[tag]++
	}
	v.Err = nil

	return stmt, true
}

// MakePLpgSQLTelemetryVisitor makes a plpgsql telemetry visitor, for capturing
// test telemetry
func MakePLpgSQLTelemetryVisitor() telemetryVisitor {
	return telemetryVisitor{ctx: context.Background(), StmtCnt: PLpgSQLStmtCounter{}}
}

// CountPLpgSQLStmt parses the function body and calls the Walk function for
// each plpgsql statement.
func CountPLpgSQLStmt(sql string) (PLpgSQLStmtCounter, error) {
	v := MakePLpgSQLTelemetryVisitor()
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plpgsqltree.Walk(&v, stmt.AST)

	return v.StmtCnt, v.Err
}
