package plpgsqltree

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
	"strings"
)

// PlpgSQLStmtCounter is used to accurately report telemetry for plpgsql
// statements. We can not use the counters due to counters needing to
// be reset after every statement using reporter.ReportDiagnostics.
type PlpgSQLStmtCounter map[string]int

func (p *PlpgSQLStmtCounter) String() string {
	var buf strings.Builder
	for k, v := range *p {
		buf.WriteString(fmt.Sprintf("%s: %d\n", k, v))

	}
	return buf.String()
}

type plpgsqlVisitor struct {
	ctx  context.Context
	stmt PLpgSQLStatement
	// StmtCnt captures telemetry for plpgsql statements that can be returned
	// during parser test.
	StmtCnt PlpgSQLStmtCounter
	// Err captures errors while stmts are being walked.
	Err error
}

func MakePlpgSqlVisitor() plpgsqlVisitor {
	return plpgsqlVisitor{ctx: context.Background(), StmtCnt: PlpgSQLStmtCounter{}}
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

func walkElseIfStmt(v plpgsqlVisitor, stmt PLpgSQLStmtIfElseIfArm) (newStmt *PLpgSQLStmtIfElseIfArm, changed bool) {
	newExpr := stmt.walkStmt(v)
	return newExpr, true
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
