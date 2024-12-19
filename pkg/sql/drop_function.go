// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type dropFunctionNode struct {
	zeroInputPlanNode
	toDrop       []*funcdesc.Mutable
	dropBehavior tree.DropBehavior
}

// DropFunction drops a function.
func (p *planner) DropFunction(ctx context.Context, n *tree.DropRoutine) (ret planNode, err error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP FUNCTION",
	); err != nil {
		return nil, err
	}

	if n.DropBehavior == tree.DropCascade {
		// TODO(chengxiong): remove this check when drop function cascade is supported.
		return nil, unimplemented.Newf("DROP FUNCTION...CASCADE", "drop function cascade not supported")
	}
	dropNode := &dropFunctionNode{
		toDrop:       make([]*funcdesc.Mutable, 0, len(n.Routines)),
		dropBehavior: n.DropBehavior,
	}
	routineType := tree.UDFRoutine
	if n.Procedure {
		routineType = tree.ProcedureRoutine
	}
	fnResolved := intsets.MakeFast()
	for _, fn := range n.Routines {
		ol, err := p.matchRoutine(ctx, &fn, !n.IfExists, routineType, true /* inDropContext */)
		if err != nil {
			return nil, err
		}
		if ol == nil {
			continue
		}
		fnID := funcdesc.UserDefinedFunctionOIDToID(ol.Oid)
		if fnResolved.Contains(int(fnID)) {
			continue
		}
		fnResolved.Add(int(fnID))
		mut, err := p.checkPrivilegesForDropFunction(ctx, fnID)
		if err != nil {
			return nil, err
		}
		if n.DropBehavior != tree.DropCascade && len(mut.DependedOnBy) > 0 {
			dependedOnByIDs := make([]descpb.ID, 0, len(mut.DependedOnBy))
			for _, ref := range mut.DependedOnBy {
				dependedOnByIDs = append(dependedOnByIDs, ref.ID)
			}
			depNames, err := p.getFullyQualifiedNamesFromIDs(ctx, dependedOnByIDs)
			if err != nil {
				return nil, err
			}
			return nil, pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop function %q because other objects ([%v]) still depend on it",
				mut.Name, strings.Join(depNames, ", "),
			)
		}
		dropNode.toDrop = append(dropNode.toDrop, mut)
	}

	if len(dropNode.toDrop) == 0 {
		return newZeroNode(nil), nil
	}
	// TODO(chengxiong): check if there is any backreference which requires
	// CASCADE drop behavior. This is needed when we start allowing UDF
	// references from other objects.
	return dropNode, nil
}

func (n *dropFunctionNode) startExec(params runParams) error {
	for _, fnMutable := range n.toDrop {
		if err := params.p.dropFunctionImpl(params.ctx, fnMutable); err != nil {
			return err
		}
	}
	return nil
}

func (n *dropFunctionNode) Next(params runParams) (bool, error) { return false, nil }
func (n *dropFunctionNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *dropFunctionNode) Close(ctx context.Context)           {}

// matchRoutine tries to resolve a user-defined function or procedure with the
// given signature from the current search path, only overloads with exactly the
// same argument types are considered a match. If required is true, an error is
// returned if the function is not found. An error is also returning if a
// builtin function is matched.
func (p *planner) matchRoutine(
	ctx context.Context,
	routineObj *tree.RoutineObj,
	required bool,
	routineType tree.RoutineType,
	inDropContext bool,
) (*tree.QualifiedOverload, error) {
	path := p.CurrentSearchPath()
	unresolvedName := routineObj.FuncName.ToUnresolvedObjectName().ToUnresolvedName()
	var name tree.UnresolvedRoutineName
	if routineType == tree.ProcedureRoutine {
		name = tree.MakeUnresolvedProcedureName(unresolvedName)
	} else {
		name = tree.MakeUnresolvedFunctionName(unresolvedName)
	}
	fnDef, err := p.ResolveFunction(ctx, name, &path)
	if err != nil {
		if !required && errors.Is(err, tree.ErrRoutineUndefined) {
			return nil, nil
		}
		return nil, err
	}

	ol, err := fnDef.MatchOverload(
		ctx, p, routineObj, &path, routineType, inDropContext, false, /* tryDefaultExprs */
	)
	if err != nil {
		if !required && errors.Is(err, tree.ErrRoutineUndefined) {
			return nil, nil
		}
		return nil, err
	}
	// Note that we don't check ol.HasSQLBody() here, because builtin functions
	// can't be dropped even if they are defined using a SQL string.
	if ol.Type == tree.BuiltinRoutine {
		return nil, errors.Errorf(
			"cannot drop function %s%s because it is required by the database system",
			fnDef.Name, ol.Signature(true /*Simplify*/),
		)
	}
	return &ol, nil
}

func (p *planner) checkPrivilegesForDropFunction(
	ctx context.Context, fnID descpb.ID,
) (*funcdesc.Mutable, error) {
	mutable, err := p.Descriptors().MutableByID(p.Txn()).Function(ctx, fnID)
	if err != nil {
		return nil, err
	}
	if err := p.canDropFunction(ctx, mutable); err != nil {
		return nil, err
	}
	return mutable, nil
}

func (p *planner) canDropFunction(ctx context.Context, fnDesc catalog.FunctionDescriptor) error {
	hasOwernship, err := p.HasOwnershipOnSchema(ctx, fnDesc.GetParentSchemaID(), fnDesc.GetParentID())
	if err != nil {
		return err
	}
	if hasOwernship {
		return nil
	}
	hasOwernship, err = p.HasOwnership(ctx, fnDesc)
	if err != nil {
		return err
	}
	if !hasOwernship {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "must be owner of function %s", fnDesc.GetName())
	}
	return nil
}

func (p *planner) dropFunctionImpl(ctx context.Context, fnMutable *funcdesc.Mutable) error {
	if fnMutable.Dropped() {
		return errors.Errorf("function %q is already being dropped", fnMutable.Name)
	}

	// Exit early with an error if the function is undergoing a declarative schema
	// change, before we try to get job IDs and update job statuses later. See
	// createOrUpdateSchemaChangeJob.
	if catalog.HasConcurrentDeclarativeSchemaChange(fnMutable) {
		return scerrors.ConcurrentSchemaChangeError(fnMutable)
	}

	// Remove backreference from tables/views/sequences referenced by this UDF.
	for _, id := range fnMutable.DependsOn {
		refMutable, err := p.Descriptors().MutableByID(p.txn).Table(ctx, id)
		if err != nil {
			return err
		}
		refMutable.DependedOnBy = removeMatchingReferences(
			refMutable.DependedOnBy,
			fnMutable.GetID(),
		)
		if err := p.writeSchemaChange(
			ctx, refMutable, descpb.InvalidMutationID,
			fmt.Sprintf("updating backreference of function %s(%d) in table %s(%d)",
				fnMutable.Name, fnMutable.ID, refMutable.Name, refMutable.ID,
			),
		); err != nil {
			return err
		}
	}

	// Remove this function from the dependencies
	for _, id := range fnMutable.DependsOnFunctions {
		refMutable, err := p.Descriptors().MutableByID(p.txn).Function(ctx, id)
		if err != nil {
			return err
		}
		if err := refMutable.RemoveFunctionReference(fnMutable.ID); err != nil {
			return err
		}
		if err := p.writeFuncDesc(ctx, refMutable); err != nil {
			return err
		}
	}

	// Remove backreference from types referenced by this UDF.
	jobDesc := fmt.Sprintf(
		"updating type backreference %v for function %s(%d)",
		fnMutable.DependsOnTypes, fnMutable.Name, fnMutable.ID,
	)
	if err := p.removeTypeBackReferences(
		ctx, fnMutable.DependsOnTypes, fnMutable.ID, jobDesc,
	); err != nil {
		return err
	}

	// Remove function signature from schema.
	scDesc, err := p.Descriptors().MutableByID(p.Txn()).Schema(ctx, fnMutable.ParentSchemaID)
	if err != nil {
		return err
	}
	scDesc.RemoveFunction(fnMutable.Name, fnMutable.ID)
	if err := p.writeSchemaDescChange(
		ctx, scDesc,
		fmt.Sprintf("removing function %s(%d) from schema %s(%d)", fnMutable.Name, fnMutable.ID, scDesc.Name, scDesc.ID),
	); err != nil {
		return err
	}

	// Mark the UDF as dropped.
	fnMutable.SetDropped()
	if err := p.writeDropFuncSchemaChange(ctx, fnMutable); err != nil {
		return err
	}
	fnName := tree.MakeQualifiedRoutineName(p.CurrentDatabase(), scDesc.GetName(), fnMutable.GetName())
	event := eventpb.DropFunction{FunctionName: fnName.FQString()}
	return p.logEvent(ctx, fnMutable.GetID(), &event)
}

func (p *planner) writeFuncDesc(ctx context.Context, funcDesc *funcdesc.Mutable) error {
	b := p.txn.NewBatch()
	if err := p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), funcDesc, b,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeFuncSchemaChange(ctx context.Context, funcDesc *funcdesc.Mutable) error {
	return p.writeFuncDesc(ctx, funcDesc)
}

func (p *planner) writeDropFuncSchemaChange(ctx context.Context, funcDesc *funcdesc.Mutable) error {
	_, recordExists := p.extendedEvalCtx.jobs.uniqueToCreate[funcDesc.ID]
	if recordExists {
		// For now being, we create jobs for functions only when functions are
		// dropped.
		return nil
	}
	jobRecord := jobs.Record{
		JobID:         p.extendedEvalCtx.ExecCfg.JobRegistry.MakeJobID(),
		Description:   "Drop Function",
		Username:      p.User(),
		DescriptorIDs: descpb.IDs{funcDesc.ID},
		Details: jobspb.SchemaChangeDetails{
			DroppedFunctions: descpb.IDs{funcDesc.ID},
			SessionData:      &p.SessionData().SessionData,
		},
		Progress: jobspb.TypeSchemaChangeProgress{},
	}
	p.extendedEvalCtx.jobs.uniqueToCreate[funcDesc.ID] = &jobRecord
	log.Infof(ctx, "queued drop function job %d for function %d", jobRecord.JobID, funcDesc.ID)
	return p.writeFuncDesc(ctx, funcDesc)
}

func (p *planner) removeDependentFunction(
	ctx context.Context, tbl *tabledesc.Mutable, fn *funcdesc.Mutable,
) error {
	// In the table whose index is being removed, filter out all back-references
	// that refer to the view that's being removed.
	tbl.DependedOnBy = removeMatchingReferences(tbl.DependedOnBy, fn.ID)
	// Then proceed to actually drop the view and log an event for it.
	return p.dropFunctionImpl(ctx, fn)
}
