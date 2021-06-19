// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

func (b *buildContext) canModifyType(ctx context.Context, desc *typedesc.Mutable) {
	hasAdmin, err := b.AuthAccessor.HasAdminRole(ctx)
	if err != nil {
		panic(err)
	}
	if hasAdmin {
		return
	}
	hasOwnership, err := b.AuthAccessor.HasOwnership(ctx, desc)
	if err != nil {
		panic(err)
	}
	if !hasOwnership {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of type %s", tree.Name(desc.GetName())))
	}
}

func (b *buildContext) canDropTypeDesc(
	ctx context.Context, typeDesc *typedesc.Mutable, behavior tree.DropBehavior,
) {
	b.canModifyType(ctx, typeDesc)
	if len(typeDesc.ReferencingDescriptorIDs) > 0 && behavior != tree.DropCascade {
		dependentNames := make([]*tree.TableName, 0, len(typeDesc.ReferencingDescriptorIDs))
		for _, descID := range typeDesc.ReferencingDescriptorIDs {
			name, err := b.Res.GetQualifiedTableNameByID(ctx, int64(descID), tree.ResolveAnyTableKind)
			if err != nil {
				panic(errors.Wrapf(err, "type %q has dependent objects", typeDesc.Name))
			}
			dependentNames = append(dependentNames, name)
		}
		panic(pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop type %q because other objects (%v) still depend on it",
			typeDesc.Name,
			dependentNames,
		))
	}
}

func (b *buildContext) dropTypeDesc(
	ctx context.Context, typeDesc *typedesc.Mutable, behavior tree.DropBehavior, ignoreAliases bool,
) {
	switch typeDesc.Kind {
	case descpb.TypeDescriptor_ALIAS:
		if ignoreAliases {
			return
		}
		// The implicit array types are not directly droppable.
		panic(pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"%q is an implicit array type and cannot be modified",
			typeDesc.GetName(),
		))
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		// Multi-region enums are not directly droppable.
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is a multi-region enum and cannot be modified directly",
				typeDesc.GetName(),
			),
			"try ALTER DATABASE DROP REGION %s", typeDesc.GetName()))
	case descpb.TypeDescriptor_ENUM:
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumDrop)
	}
	b.canDropTypeDesc(ctx, typeDesc, behavior)
	// Get the array type that needs to be dropped as well.
	mutArrayDesc, err := b.Descs.GetMutableTypeVersionByID(ctx, b.EvalCtx.Txn, typeDesc.ArrayTypeID)
	if err != nil {
		panic(err)
	}
	// Ensure that we can drop the array type as well.
	b.canDropTypeDesc(ctx, mutArrayDesc, behavior)
	// Create drop elements for both.
	typeDescElem := &scpb.Type{
		TypeID: typeDesc.GetID(),
	}
	b.addNode(scpb.Target_DROP, typeDescElem)
	mutArrayDescElem := &scpb.Type{
		TypeID: mutArrayDesc.GetID(),
	}
	b.addNode(scpb.Target_DROP, mutArrayDescElem)
}

func (b *buildContext) dropType(ctx context.Context, n *tree.DropType) {
	if n.DropBehavior == tree.DropCascade {
		panic(unimplemented.NewWithIssue(51480, "DROP TYPE CASCADE is not yet supported"))
	}
	for _, name := range n.Names {
		// Resolve the desired type descriptor.
		_, typeDesc, err := resolver.ResolveMutableType(ctx, b.Res, name, !n.IfExists)
		if err != nil {
			panic(err)
		}
		if typeDesc == nil {
			continue
		}
		b.dropTypeDesc(ctx, typeDesc, n.DropBehavior, false /* ignoreAliases */)
	}
}
