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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// CreateTestTableDescriptor converts a SQL string to a table for test purposes.
// Will fail on complex tables where that operation requires e.g. looking up
// other tables.
func CreateTestTableDescriptor(
	ctx context.Context,
	parentID, id descpb.ID,
	schema string,
	privileges *catpb.PrivilegeDescriptor,
	txn *kv.Txn,
	collection *descs.Collection,
) (*tabledesc.Mutable, error) {
	st := cluster.MakeTestingClusterSettings()
	stmt, err := parser.ParseOne(schema)
	if err != nil {
		return nil, err
	}
	semaCtx := tree.MakeSemaContext()
	evalCtx := eval.MakeTestingEvalContext(st)
	sessionData := &sessiondata.SessionData{
		LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
			EnableUniqueWithoutIndexConstraints: true,
		},
	}
	switch n := stmt.AST.(type) {
	case *tree.CreateTable:
		db := dbdesc.NewInitial(parentID, "test", username.RootUserName())
		desc, err := NewTableDesc(
			ctx,
			nil, /* txn */
			NewSkippingCacheSchemaResolver(collection, sessiondata.NewStack(sessionData), txn, nil),
			st,
			n,
			db,
			schemadesc.GetPublicSchema(),
			id,
			nil,             /* regionConfig */
			hlc.Timestamp{}, /* creationTime */
			privileges,
			make(map[descpb.ID]*tabledesc.Mutable),
			&semaCtx,
			&evalCtx,
			sessionData,
			tree.PersistencePermanent,
		)
		return desc, err
	case *tree.CreateSequence:
		desc, err := NewSequenceTableDesc(
			ctx,
			nil, /* planner */
			st,
			n.Name.Table(),
			n.Options,
			parentID, keys.PublicSchemaID, id,
			hlc.Timestamp{}, /* creationTime */
			privileges,
			tree.PersistencePermanent,
			false, /* isMultiRegion */
		)
		return desc, err
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

// Exec is a test utility function that takes a localPlanner (of type
// interface{} so that external packages can call NewInternalPlanner and pass
// the result) and executes a sql statement through the DistSQLPlanner.
func (dsp *DistSQLPlanner) Exec(
	ctx context.Context, localPlanner interface{}, stmt parser.Statement, distribute bool,
) error {
	p := localPlanner.(*planner)
	p.stmt = makeStatement(stmt, clusterunique.ID{} /* queryID */)
	if err := p.makeOptimizerPlan(ctx); err != nil {
		return err
	}
	defer p.curPlan.close(ctx)
	rw := NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		return nil
	})
	execCfg := p.ExecCfg()
	recv := MakeDistSQLReceiver(
		ctx,
		rw,
		stmt.AST.StatementReturnType(),
		execCfg.RangeDescriptorCache,
		p.txn,
		execCfg.Clock,
		p.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	distributionType := DistributionType(DistributionTypeNone)
	if distribute {
		distributionType = DistributionTypeSystemTenantOnly
	}
	evalCtx := p.ExtendedEvalContext()
	planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p, p.txn,
		distributionType)
	planCtx.stmtType = recv.stmtType

	dsp.PlanAndRun(ctx, evalCtx, planCtx, p.txn, p.curPlan.main, recv, nil /* finishedSetupFn */)
	return rw.Err()
}

// ExecLocalAll is basically a conn_executor free version of execWithDistSQLEngine
// hard coded for non-distributed statements (currently used by copy testing).
func (dsp *DistSQLPlanner) ExecLocalAll(
	ctx context.Context, execCfg ExecutorConfig, p *planner, res RestrictedCommandResult,
) error {
	recv := MakeDistSQLReceiver(
		ctx,
		res,
		p.stmt.AST.StatementReturnType(),
		execCfg.RangeDescriptorCache,
		p.txn,
		execCfg.Clock,
		p.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	distributionType := DistributionType(DistributionTypeNone)
	evalCtx := p.ExtendedEvalContext()
	planCtx := execCfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, p, p.txn,
		distributionType)
	planCtx.stmtType = recv.stmtType

	var evalCtxFactory func() *extendedEvalContext
	var factoryEvalCtx extendedEvalContext = extendedEvalContext{
		Tracing: &SessionTracing{},
	}
	evalCtxFactory = func() *extendedEvalContext {
		factoryEvalCtx.Context = evalCtx.Context
		factoryEvalCtx.Placeholders = &p.semaCtx.Placeholders
		factoryEvalCtx.Annotations = &p.semaCtx.Annotations
		return &factoryEvalCtx
	}
	return dsp.PlanAndRunAll(ctx, evalCtx, planCtx, p, recv, evalCtxFactory)
}
