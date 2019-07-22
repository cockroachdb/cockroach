// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// CreateTestTableDescriptor converts a SQL string to a table for test purposes.
// Will fail on complex tables where that operation requires e.g. looking up
// other tables.
func CreateTestTableDescriptor(
	ctx context.Context,
	parentID, id sqlbase.ID,
	schema string,
	privileges *sqlbase.PrivilegeDescriptor,
) (sqlbase.TableDescriptor, error) {
	st := cluster.MakeTestingClusterSettings()
	stmt, err := parser.ParseOne(schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(st)
	desc, err := MakeTableDesc(
		ctx,
		nil, /* txn */
		nil, /* vt */
		st,
		stmt.AST.(*tree.CreateTable),
		parentID, id,
		hlc.Timestamp{}, /* creationTime */
		privileges,
		nil, /* affected */
		&semaCtx,
		&evalCtx,
	)
	return desc.TableDescriptor, err
}

func makeTestingExtendedEvalContext(st *cluster.Settings) extendedEvalContext {
	return extendedEvalContext{
		EvalContext: tree.MakeTestingEvalContext(st),
		Tracing:     &SessionTracing{},
	}
}

// StmtBufReader is an exported interface for reading a StmtBuf.
// Normally only the write interface of the buffer is exported, as it is used by
// the pgwire.
type StmtBufReader struct {
	buf *StmtBuf
}

// MakeStmtBufReader creates a StmtBufReader.
func MakeStmtBufReader(buf *StmtBuf) StmtBufReader {
	return StmtBufReader{buf: buf}
}

// CurCmd returns the current command in the buffer.
func (r StmtBufReader) CurCmd() (Command, error) {
	cmd, _ /* pos */, err := r.buf.CurCmd()
	return cmd, err
}

// AdvanceOne moves the cursor one position over.
func (r *StmtBufReader) AdvanceOne() {
	r.buf.AdvanceOne()
}

// SeekToNextBatch skips to the beginning of the next batch of commands.
func (r *StmtBufReader) SeekToNextBatch() error {
	return r.buf.seekToNextBatch()
}

// Exec is a test utility function that takes a localPlanner (of type
// interface{} so that external packages can call NewInternalPlanner and pass
// the result) and executes a sql statement through the DistSQLPlanner.
func (dsp *DistSQLPlanner) Exec(
	ctx context.Context, localPlanner interface{}, sql string, distribute bool,
) error {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return err
	}
	p := localPlanner.(*planner)
	p.stmt = &Statement{Statement: stmt}
	if err := p.makePlan(ctx); err != nil {
		return err
	}
	rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		return nil
	})
	execCfg := p.ExecCfg()
	recv := MakeDistSQLReceiver(
		ctx,
		rw,
		stmt.AST.StatementType(),
		execCfg.RangeDescriptorCache,
		execCfg.LeaseHolderCache,
		p.txn,
		func(ts hlc.Timestamp) {
			_ = execCfg.Clock.Update(ts)
		},
		p.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	evalCtx := p.ExtendedEvalContext()
	planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p.txn)
	planCtx.isLocal = !distribute
	planCtx.planner = p
	planCtx.stmtType = recv.stmtType

	dsp.PlanAndRun(ctx, evalCtx, planCtx, p.txn, p.curPlan.plan, recv)()
	return rw.Err()
}
