// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type dropFuncNode struct {
	n  *tree.DropFunction
	fd map[descpb.ID]*funcdesc.Mutable
}

func (p *planner) DropFunction(ctx context.Context, n *tree.DropFunction) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"DROP FUNCTIOn",
	); err != nil {
		return nil, err
	}

	node := &dropFuncNode{
		n:  n,
		fd: make(map[descpb.ID]*funcdesc.Mutable),
	}
	if n.DropBehavior == tree.DropCascade {
		return nil, unimplemented.NewWithIssue(51480, "DROP FUNCTION CASCADE is not yet supported")
	}

	for _, name := range n.Names {
		// Resolve the desired func descriptor.
		funcDesc, err := p.ResolveMutableFuncDescriptor(ctx, name, !n.IfExists)
		if err != nil {
			return nil, err
		}
		if funcDesc == nil {
			continue
		}
		// If we've already seen this type, then skip it.
		if _, ok := node.fd[funcDesc.ID]; ok {
			continue
		}

		// Check if we can drop the type.
		if err := p.canDropFuncDesc(ctx, funcDesc, n.DropBehavior); err != nil {
			return nil, err
		}
		node.fd[funcDesc.ID] = funcDesc
	}

	return node, nil
}

func (p *planner) canModifyFunc(ctx context.Context, desc *funcdesc.Mutable) error {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}

	hasOwnership, err := p.HasOwnership(ctx, desc)
	if err != nil {
		return err
	}
	if !hasOwnership {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of func %s", tree.Name(desc.GetName()))
	}
	return nil
}

func (p *planner) canDropFuncDesc(
	ctx context.Context, desc *funcdesc.Mutable, behavior tree.DropBehavior,
) error {
	if err := p.canModifyFunc(ctx, desc); err != nil {
		return err
	}

	if behavior != tree.DropCascade {
		if err := p.checkReferencingObjects(ctx, "function", desc.Name, desc.ReferencingDescriptorIDs); err != nil {
			return err
		}
	}
	return nil
}

func (n *dropFuncNode) startExec(params runParams) error {
	return unimplemented.New("DROP FUNCTION", "DROP FUNCTION is currently unimplemented")
	// 	for _, fn := range n.fd {
	// 		if err := params.p.dropFuncImpl(params.ctx, fn, tree.AsStringWithFQNames(n.n, params.Ann()),
	// 			true /* queueJob */); err != nil {
	// 			return err
	// 		}
	// 		// Log a Drop Func event.
	// 		// TODO(knz): This logging is imperfect, see this issue:
	// 		// https://github.com/cockroachdb/cockroach/issues/57734
	// 		if err := params.p.logEvent(params.ctx, fn.ID, &eventpb.DropFunc{FuncName: fn.Name}); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
}

// dropFuncImpl does the work of dropping a type and everything that depends on it.
func (p *planner) dropFuncImpl(
	ctx context.Context, funcDesc *funcdesc.Mutable, jobDesc string, queueJob bool,
) error {
	if funcDesc.Dropped() {
		return errors.Errorf("func %q is already being dropped", funcDesc.Name)
	}

	// Add a draining name.
	funcDesc.DrainingNames = append(funcDesc.DrainingNames, descpb.NameInfo{
		ParentID:       funcDesc.ParentID,
		ParentSchemaID: funcDesc.ParentSchemaID,
		Name:           funcDesc.Name,
	})

	// Actually mark the type as dropped.
	funcDesc.State = descpb.DescriptorState_DROP
	if queueJob {
		// return p.writeTypeSchemaChange(ctx, funcDesc, jobDesc)
	}
	return p.writeFuncDesc(ctx, funcDesc)
}

func (p *planner) writeFuncDesc(ctx context.Context, funcDesc *funcdesc.Mutable) error {
	// Write the func out to a batch.
	b := p.txn.NewBatch()
	if err := p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), funcDesc, b,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (n *dropFuncNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *dropFuncNode) Values() tree.Datums            { return nil }
func (n *dropFuncNode) Close(_ context.Context)        {}
