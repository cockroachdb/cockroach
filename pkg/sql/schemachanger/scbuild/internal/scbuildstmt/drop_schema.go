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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// DropSchema implements DROP SCHEMA.
func DropSchema(b BuildCtx, n *tree.DropSchema) {
	for _, name := range n.Names {
		db, sc := b.ResolveSchema(name, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		if sc == nil {
			continue
		}
		if sc.GetName() == tree.PublicSchema {
			panic(pgerror.Newf(pgcode.InvalidSchemaName, "cannot drop schema %q", sc.GetName()))
		}
		if sc.SchemaKind() == catalog.SchemaPublic ||
			sc.SchemaKind() == catalog.SchemaVirtual ||
			sc.SchemaKind() == catalog.SchemaTemporary {
			panic(pgerror.Newf(pgcode.InvalidSchemaName,
				"cannot drop schema %q", sc.GetName))
		}
		dropSchema(b, db, sc, n.DropBehavior, false /* parent drop*/)
		b.IncrementSubWorkID()
	}
}

func dropSchema(
	b BuildCtx,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	behavior tree.DropBehavior,
	parentDrop bool,
) (nodeAdded bool, dropIDs catalog.DescriptorIDSet) {
	descsThatNeedElements := catalog.DescriptorIDSet{}
	_, objectIDs := b.CatalogReader().ReadObjectNamesAndIDs(b, db, sc)
	for _, id := range objectIDs {
		// For dependency tracking we will still track that these elements were
		// children even if we didn't add the drop elements ourselves here.
		dropIDs.Add(id)
		// If the object is already dropped, then we don't need to create elements
		// for them.
		if !checkIfDescOrElementAreDropped(b, id) {
			descsThatNeedElements.Add(id)
		}
	}
	if behavior != tree.DropCascade && !dropIDs.Empty() {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"schema %q is not empty and CASCADE was not specified", sc.GetName()))
	}
	{
		c := b.WithNewSourceElementID()
		var droppedTypes []catalog.TypeDescriptor
		for _, id := range descsThatNeedElements.Ordered() {
			desc := c.CatalogReader().MustReadDescriptor(b, id)
			switch t := desc.(type) {
			case catalog.TableDescriptor:
				if t.IsView() {
					dropView(c, t, behavior)
				} else if t.IsSequence() {
					dropSequence(c, t, behavior)
				} else if t.IsTable() {
					dropTable(c, t, behavior)
				} else {
					panic(errors.AssertionFailedf("table descriptor %q (%d) is neither table, sequence or view",
						t.GetName(), t.GetID()))
				}
			case catalog.TypeDescriptor:
				dropType(c, t, behavior)
				droppedTypes = append(droppedTypes, t)
			default:
				panic(errors.AssertionFailedf("expected table or type descriptor, instead %q (%d) is %q",
					t.GetName(), t.GetID(), t.DescriptorType()))
			}
		}
		// For dropped types validate if any unsupported cascade is needed.
		for _, typeDesc := range droppedTypes {
			var dependentNames []*tree.TableName
			for i := 0; i < typeDesc.NumReferencingDescriptors(); i++ {
				id := typeDesc.GetReferencingDescriptorID(i)
				foundDrop := false
				b.ForEachElementStatus(func(status, targetStatus scpb.Status, elem scpb.Element) {
					if targetStatus == scpb.Status_ABSENT && screl.GetDescID(elem) == id {
						foundDrop = true
					}
				})
				if !foundDrop && parentDrop {
					tableDesc := b.MustReadTable(id)
					if tableDesc.GetParentID() == sc.GetParentID() {
						// Parent will clean up this object.
						foundDrop = true
					}
				}
				if !foundDrop {
					name, err := b.CatalogReader().GetQualifiedTableNameByID(b, int64(id), tree.ResolveAnyTableKind)
					onErrPanic(err)
					dependentNames = append(dependentNames, name)
				}
			}
			if len(dependentNames) > 0 {
				prefix := tree.ObjectNamePrefix{
					CatalogName:     tree.Name(db.GetName()),
					SchemaName:      tree.Name(sc.GetName()),
					ExplicitSchema:  true,
					ExplicitCatalog: true,
				}
				fqName := tree.MakeTypeNameWithPrefix(prefix, typeDesc.GetName())
				panic(unimplemented.NewWithIssueDetailf(51480, "DROP TYPE CASCADE is not yet supported",
					"cannot drop type %q because other objects (%v) still depend on it",
					fqName.FQString(),
					dependentNames))
			}
		}
	}
	switch sc.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		return false, dropIDs
	case catalog.SchemaUserDefined:
		b.EnqueueDrop(&scpb.Schema{
			SchemaID:         sc.GetID(),
			DependentObjects: dropIDs.Ordered(),
		})
		b.EnqueueDrop(&scpb.DatabaseSchemaEntry{
			DatabaseID: sc.GetParentID(),
			SchemaID:   sc.GetID(),
		})
		b.EnqueueDrop(&scpb.SchemaComment{
			SchemaID: sc.GetID(),
			Comment:  scpb.PlaceHolderComment,
		})
		return true, dropIDs
	}
	panic(errors.AssertionFailedf("unexpected sc kind %q for sc %q (%d)",
		sc.SchemaKind(), sc.GetName(), sc.GetID()))
}
