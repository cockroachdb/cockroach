// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package utils

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	unimp "github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// PLpgSQLStmtCounter is used to accurately report telemetry for plpgsql
// statements test . We can not use the telemetry counters due to them needing
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

var _ plpgsqltree.PLpgSQLStmtVisitor = &telemetryVisitor{}

// Visit implements the PLpgSQLStmtVisitor interface
func (v *telemetryVisitor) Visit(stmt plpgsqltree.PLpgSQLStatement) {
	taggedStmt, ok := stmt.(plpgsqltree.TaggedPLpgSQLStatement)
	if !ok {
		v.Err = errors.AssertionFailedf("no tag found for stmt %q", stmt)
	}
	tag := taggedStmt.PlpgSQLStatementTag()
	sqltelemetry.IncrementPlpgsqlStmtCounter(tag)

	//Capturing telemetry for tests
	_, ok = v.StmtCnt[tag]
	if !ok {
		v.StmtCnt[tag] = 1
	} else {
		v.StmtCnt[tag]++
	}
	v.Err = nil

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

// ParseAndCollectTelemetryForPLpgSQLFunc takes a plpgsql function and parses and collects
// telemetry on the parsable statements.
func ParseAndCollectTelemetryForPLpgSQLFunc(stmt *tree.CreateFunction) error {
	// Assert that the function language is PLPGSQL.
	var funcBodyStr string
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case tree.FunctionBodyStr:
			funcBodyStr = string(opt)
		}
	}

	if _, err := CountPLpgSQLStmt(funcBodyStr); err != nil {
		return errors.Wrap(err, "plpgsql not supported in user-defined functions")
	}
	return unimp.New("plpgsql", "plpgsql not supported in user-defined functions")
}
