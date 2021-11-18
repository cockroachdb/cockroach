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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

func (b *buildContext) canModifyType(ctx context.Context, desc catalog.TypeDescriptor) {
	hasAdmin, err := b.AuthorizationAccessor().HasAdminRole(ctx)
	onErrPanic(err)
	if hasAdmin {
		return
	}
	hasOwnership, err := b.AuthorizationAccessor().HasOwnership(ctx, desc)
	onErrPanic(err)
	if !hasOwnership {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of type %s", tree.Name(desc.GetName())))
	}
}

func (b *buildContext) canDropTypeDesc(
	ctx context.Context, typ catalog.TypeDescriptor, behavior tree.DropBehavior,
) {
	b.canModifyType(ctx, typ)
	if len(typ.TypeDesc().ReferencingDescriptorIDs) > 0 && behavior != tree.DropCascade {
		dependentNames := make([]*tree.TableName, 0, len(typ.TypeDesc().ReferencingDescriptorIDs))
		for _, descID := range typ.TypeDesc().ReferencingDescriptorIDs {
			name, err := b.CatalogReader().GetQualifiedTableNameByID(ctx, int64(descID), tree.ResolveAnyTableKind)
			if err != nil {
				panic(errors.Wrapf(err, "type %q has dependent objects", typ.GetName()))
			}
			dependentNames = append(dependentNames, name)
		}
		panic(pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop type %q because other objects (%v) still depend on it",
			typ.GetName(),
			dependentNames,
		))
	}
}

func (b *buildContext) dropTypeDesc(
	ctx context.Context, typ catalog.TypeDescriptor, behavior tree.DropBehavior, ignoreAliases bool,
) {
	switch typ.GetKind() {
	case descpb.TypeDescriptor_ALIAS:
		if ignoreAliases {
			return
		}
		// The implicit array types are not directly droppable.
		panic(pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"%q is an implicit array type and cannot be modified",
			typ.GetName(),
		))
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		// Multi-region enums are not directly droppable.
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"%q is a multi-region enum and cannot be modified directly",
				typ.GetName(),
			),
			"try ALTER DATABASE DROP REGION %s", typ.GetName()))
	case descpb.TypeDescriptor_ENUM:
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumDrop)
	}
	b.canDropTypeDesc(ctx, typ, behavior)
	// Get the array type that needs to be dropped as well.
	mutArrayDesc := mustReadType(ctx, b, typ.GetArrayTypeID())
	// Ensure that we can drop the array type as well.
	b.canDropTypeDesc(ctx, mutArrayDesc, behavior)
	// Create drop elements for both.
	typeDescElem := &scpb.Type{
		TypeID: typ.GetID(),
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
		_, typ := b.CatalogReader().MayResolveType(ctx, *name)
		if typ == nil {
			if n.IfExists {
				continue
			}
			panic(sqlerrors.NewUndefinedTypeError(name))
		}
		onErrPanic(b.AuthorizationAccessor().CheckPrivilege(ctx, typ, privilege.DROP))
		b.dropTypeDesc(ctx, typ, n.DropBehavior, false /* ignoreAliases */)
		b.incrementSubWorkID()
	}
}
