// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// DropType implements DROP TYPE.
func DropType(b BuildCtx, n *tree.DropType) {
	if n.DropBehavior == tree.DropCascade {
		panic(unimplemented.NewWithIssue(51480, "DROP TYPE CASCADE is not yet supported"))
	}
	for _, name := range n.Names {
		_, typ := b.ResolveType(name, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if typ == nil {
			continue
		}
		// If the descriptor is already being dropped, nothing to do.
		if checkIfDescOrElementAreDropped(b, typ.GetID()) {
			return
		}
		dropType(b, typ, n.DropBehavior)
		b.IncrementSubWorkID()
	}
}

func dropType(b BuildCtx, typ catalog.TypeDescriptor, behavior tree.DropBehavior) {
	switch typ.GetKind() {
	case descpb.TypeDescriptor_ALIAS:
		// Ignore alias types.
		return
	case descpb.TypeDescriptor_ENUM:
		sqltelemetry.IncrementEnumCounter(sqltelemetry.EnumDrop)
	default:
		panic(errors.AssertionFailedf("unexpected kind %s for type %q", typ.GetKind(), typ.GetName()))
	}
	canDrop := func(desc catalog.TypeDescriptor) {
		b.MustOwn(desc)
		if desc.NumReferencingDescriptors() > 0 && behavior != tree.DropCascade {
			dependentNames := make([]string, 0, desc.NumReferencingDescriptors())
			for i := 0; i < desc.NumReferencingDescriptors(); i++ {
				id := desc.GetReferencingDescriptorID(i)
				name, err := b.CatalogReader().GetQualifiedTableNameByID(b, int64(id), tree.ResolveAnyTableKind)
				if err != nil {
					panic(errors.WithAssertionFailure(err))
				}
				dependentNames = append(dependentNames, name.String())
			}
			panic(pgerror.Newf(
				pgcode.DependentObjectsStillExist,
				"cannot drop type %q because other objects still depend on it: %s",
				desc.GetName(),
				strings.Join(dependentNames, ", "),
			))
		}
	}

	canDrop(typ)
	// Get the arrayType type that needs to be dropped as well.
	arrayType := b.MustReadType(typ.GetArrayTypeID())
	// Ensure that we can drop the arrayType type as well.
	canDrop(arrayType)
	// Create drop elements for both.
	b.EnqueueDrop(&scpb.Type{TypeID: typ.GetID()})
	b.EnqueueDrop(&scpb.Type{TypeID: arrayType.GetID()})
}
