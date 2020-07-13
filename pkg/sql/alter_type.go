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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type alterTypeNode struct {
	n    *tree.AlterType
	desc *sqlbase.MutableTypeDescriptor
}

// alterTypeNode implements planNode. We set n here to satisfy the linter.
var _ planNode = &alterTypeNode{n: nil}

func (p *planner) AlterType(ctx context.Context, n *tree.AlterType) (planNode, error) {
	// Resolve the type.
	desc, err := p.ResolveMutableTypeDescriptor(ctx, n.Type, true /* required */)
	if err != nil {
		return nil, err
	}
	// The implicit array types are not modifiable.
	if desc.Kind == sqlbase.TypeDescriptor_ALIAS {
		return nil, pgerror.Newf(
			pgcode.WrongObjectType,
			"%q is an implicit array type and cannot be modified",
			tree.AsStringWithFQNames(n.Type, &p.semaCtx.Annotations),
		)
	}
	// TODO (rohany): Check permissions here once we track them.
	return &alterTypeNode{
		n:    n,
		desc: desc,
	}, nil
}

func (n *alterTypeNode) startExec(params runParams) error {
	var err error
	switch t := n.n.Cmd.(type) {
	case *tree.AlterTypeAddValue:
		err = unimplemented.NewWithIssue(48670, "ALTER TYPE ADD VALUE unsupported")
	case *tree.AlterTypeRenameValue:
		err = unimplemented.NewWithIssue(48697, "ALTER TYPE RENAME VALUE unsupported")
	case *tree.AlterTypeRename:
		err = params.p.renameType(params, n, t.NewName)
	case *tree.AlterTypeSetSchema:
		err = unimplemented.NewWithIssue(48672, "ALTER TYPE SET SCHEMA unsupported")
	default:
		err = errors.AssertionFailedf("unknown alter type cmd %s", t)
	}
	if err != nil {
		return err
	}
	return n.desc.Validate(params.ctx, params.p.txn, params.ExecCfg().Codec)
}

func (p *planner) renameType(params runParams, n *alterTypeNode, newName string) error {
	// See if there is a name collision with the new name.
	exists, id, err := sqlbase.LookupObjectID(
		params.ctx,
		p.txn,
		p.ExecCfg().Codec,
		n.desc.ParentID,
		n.desc.ParentSchemaID,
		newName,
	)
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetDescriptorByID(params.ctx, p.txn, p.ExecCfg().Codec, id)
		if err != nil {
			return sqlbase.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlbase.MakeObjectAlreadyExistsError(desc.DescriptorProto(), newName)
	} else if err != nil {
		return err
	}

	// Rename the base descriptor.
	if err := p.performRenameTypeDesc(
		params.ctx,
		n.desc,
		newName,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Now rename the array type.
	newArrayName, err := findFreeArrayTypeName(
		params.ctx,
		p.txn,
		p.ExecCfg().Codec,
		n.desc.ParentID,
		n.desc.ParentSchemaID,
		newName,
	)
	if err != nil {
		return err
	}
	// TODO (rohany): This should use a method on the desc collection instead.
	arrayDesc, err := sqlbase.GetTypeDescFromID(params.ctx, p.txn, p.ExecCfg().Codec, n.desc.ArrayTypeID)
	if err != nil {
		return err
	}
	if err := p.performRenameTypeDesc(
		params.ctx,
		sqlbase.NewMutableExistingTypeDescriptor(*arrayDesc.TypeDesc()),
		newArrayName,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}
	return nil
}

func (p *planner) performRenameTypeDesc(
	ctx context.Context, desc *sqlbase.MutableTypeDescriptor, newName string, jobDesc string,
) error {
	// Record the rename details in the descriptor for draining.
	desc.DrainingNames = append(desc.DrainingNames, sqlbase.NameInfo{
		ParentID:       desc.ParentID,
		ParentSchemaID: desc.ParentSchemaID,
		Name:           desc.Name,
	})
	// Set the descriptor up with the new name.
	desc.Name = newName
	if err := p.writeTypeChange(ctx, desc, jobDesc); err != nil {
		return err
	}
	// Construct the new namespace key.
	b := p.txn.NewBatch()
	key := sqlbase.MakeObjectNameKey(
		ctx,
		p.ExecCfg().Settings,
		desc.ParentID,
		desc.ParentSchemaID,
		newName,
	).Key(p.ExecCfg().Codec)
	b.CPut(key, desc.ID, nil /* expected */)
	return p.txn.Run(ctx, b)
}

func (n *alterTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterTypeNode) Close(ctx context.Context)           {}
func (n *alterTypeNode) ReadingOwnWrites()                   {}
