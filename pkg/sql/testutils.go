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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// CreateTestTableDescriptor converts a SQL string to a table for test purposes.
// Will fail on complex tables where that operation requires e.g. looking up
// other tables.
func CreateTestTableDescriptor(
	ctx context.Context,
	parentID, id sqlbase.ID,
	schema string,
	privileges *sqlbase.PrivilegeDescriptor,
) (*sqlbase.MutableTableDescriptor, error) {
	st := cluster.MakeTestingClusterSettings()
	stmt, err := parser.ParseOne(schema)
	if err != nil {
		return nil, err
	}
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(st)
	switch n := stmt.AST.(type) {
	case *tree.CreateTable:
		desc, err := MakeTableDesc(
			ctx,
			nil, /* txn */
			nil, /* vs */
			st,
			n,
			parentID, keys.PublicSchemaID, id,
			hlc.Timestamp{}, /* creationTime */
			privileges,
			nil, /* affected */
			&semaCtx,
			&evalCtx,
			&sessiondata.SessionData{}, /* sessionData */
			false,                      /* temporary */
		)
		return &desc, err
	case *tree.CreateSequence:
		desc, err := MakeSequenceTableDesc(
			n.Name.Table(),
			n.Options,
			parentID, keys.PublicSchemaID, id,
			hlc.Timestamp{}, /* creationTime */
			privileges,
			false, /* temporary */
			nil,   /* params */
		)
		return &desc, err
	default:
		return nil, errors.Errorf("unexpected AST %T", stmt.AST)
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
	if err := p.makeOptimizerPlan(ctx); err != nil {
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
		p.txn,
		func(ts hlc.Timestamp) {
			execCfg.Clock.Update(ts)
		},
		p.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	evalCtx := p.ExtendedEvalContext()
	planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p.txn, distribute)
	planCtx.planner = p
	planCtx.stmtType = recv.stmtType

	dsp.PlanAndRun(ctx, evalCtx, planCtx, p.txn, p.curPlan.main, recv)()
	return rw.Err()
}
