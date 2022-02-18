// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdecomp

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// ElementVisitor is the type of the visitor callback function used by
// WalkDescriptor.
type ElementVisitor func(status scpb.Status, element scpb.Element)

// WalkDescriptor walks through the elements which are implicitly defined in
// the descriptor, and returns the set of descriptor IDs back-referenced by
// the descriptor. The back-references are not elements themselves and instead
// are owned by the forward-referencing elements.
func WalkDescriptor(desc catalog.Descriptor, ev ElementVisitor) (backRefs catalog.DescriptorIDSet) {
	defer func() {
		backRefs.Remove(descpb.InvalidID)
	}()
	// Common elements.
	ev(scpb.Status_PUBLIC, &scpb.Namespace{
		DatabaseID:   desc.GetParentID(),
		SchemaID:     desc.GetParentSchemaID(),
		DescriptorID: desc.GetID(),
		Name:         desc.GetName(),
	})
	privileges := desc.GetPrivileges()
	ev(scpb.Status_PUBLIC, &scpb.Owner{
		DescriptorID: desc.GetID(),
		Owner:        privileges.Owner().Normalized(),
	})
	for _, user := range privileges.Users {
		ev(scpb.Status_PUBLIC, &scpb.UserPrivileges{
			DescriptorID: desc.GetID(),
			UserName:     user.User().Normalized(),
			Privileges:   user.Privileges,
		})
	}
	// Dispatch on type.
	switch d := desc.(type) {
	case catalog.DatabaseDescriptor:
		return walkDatabase(ev, d)
	case catalog.SchemaDescriptor:
		return walkSchema(ev, d)
	case catalog.TypeDescriptor:
		return walkType(ev, d)
	case catalog.TableDescriptor:
		return walkRelation(ev, d)
	default:
		panic(errors.AssertionFailedf("unexpected descriptor type %T: %+v",
			desc, desc))
	}
}

func walkDatabase(
	ev ElementVisitor, db catalog.DatabaseDescriptor,
) (backRefs catalog.DescriptorIDSet) {
	dbStatus := scpb.Status_PUBLIC
	if db.Dropped() {
		dbStatus = scpb.Status_DROPPED
	}
	ev(dbStatus, &scpb.Database{DatabaseID: db.GetID()})
	// TODO(postamar): proper handling of comment and role setting
	ev(scpb.Status_PUBLIC, &scpb.DatabaseRoleSetting{
		DatabaseID: db.GetID(),
		RoleName:   scpb.PlaceHolderRoleName,
	})
	ev(scpb.Status_PUBLIC, &scpb.DatabaseComment{
		DatabaseID: db.GetID(),
		Comment:    scpb.PlaceHolderComment,
	})
	if db.IsMultiRegion() {
		ev(scpb.Status_PUBLIC, &scpb.DatabaseRegionConfig{
			DatabaseID:       db.GetID(),
			RegionEnumTypeID: db.GetRegionConfig().RegionEnumID,
		})
	}
	_ = db.ForEachNonDroppedSchema(func(id descpb.ID, name string) error {
		backRefs.Add(id)
		return nil
	})
	return backRefs
}

func walkSchema(ev ElementVisitor, sc catalog.SchemaDescriptor) (backRefs catalog.DescriptorIDSet) {
	scStatus := scpb.Status_PUBLIC
	if sc.Dropped() {
		scStatus = scpb.Status_DROPPED
	}
	ev(scStatus, &scpb.Schema{
		SchemaID:    sc.GetID(),
		IsPublic:    sc.SchemaKind() == catalog.SchemaPublic,
		IsVirtual:   sc.SchemaKind() == catalog.SchemaVirtual,
		IsTemporary: sc.SchemaKind() == catalog.SchemaTemporary,
	})
	ev(scpb.Status_PUBLIC, &scpb.SchemaParent{
		SchemaID:         sc.GetID(),
		ParentDatabaseID: sc.GetParentID(),
	})
	// TODO(postamar): proper handling of comment
	ev(scpb.Status_PUBLIC, &scpb.SchemaComment{
		SchemaID: sc.GetID(),
		Comment:  scpb.PlaceHolderComment,
	})
	return catalog.DescriptorIDSet{}
}

func walkType(ev ElementVisitor, typ catalog.TypeDescriptor) (backRefs catalog.DescriptorIDSet) {
	typStatus := scpb.Status_PUBLIC
	if typ.Dropped() {
		typStatus = scpb.Status_DROPPED
	}
	switch typ.GetKind() {
	case descpb.TypeDescriptor_ALIAS:
		typeT, err := newTypeT(typ.TypeDesc().Alias)
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "alias type %q (%d)",
				typ.GetName(), typ.GetID()))
		}
		ev(typStatus, &scpb.AliasType{
			TypeID: typ.GetID(),
			TypeT:  *typeT,
		})
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		ev(typStatus, &scpb.EnumType{
			TypeID:      typ.GetID(),
			ArrayTypeID: typ.GetArrayTypeID(),
		})
	default:
		panic(errors.AssertionFailedf("unsupported type kind %q", typ.GetKind()))
	}
	ev(scpb.Status_PUBLIC, &scpb.ObjectParent{
		ObjectID:       typ.GetID(),
		ParentSchemaID: typ.GetParentSchemaID(),
	})
	for i := 0; i < typ.NumReferencingDescriptors(); i++ {
		backRefs.Add(typ.GetReferencingDescriptorID(i))
	}
	return backRefs
}

func walkRelation(
	ev ElementVisitor, tbl catalog.TableDescriptor,
) (backRefs catalog.DescriptorIDSet) {
	relStatus := scpb.Status_PUBLIC
	if tbl.Dropped() {
		relStatus = scpb.Status_DROPPED
	}
	switch {
	case tbl.IsSequence():
		ev(relStatus, &scpb.Sequence{
			SequenceID:  tbl.GetID(),
			IsTemporary: tbl.IsTemporary(),
		})
		if opts := tbl.GetSequenceOpts(); opts != nil {
			if ownerID := opts.SequenceOwner.OwnerTableID; ownerID != descpb.InvalidID {
				backRefs.Add(ownerID)
			}
		}
	case tbl.IsView():
		ev(relStatus, &scpb.View{
			ViewID:          tbl.GetID(),
			UsesTypeIDs:     catalog.MakeDescriptorIDSet(tbl.GetDependsOnTypes()...).Ordered(),
			UsesRelationIDs: catalog.MakeDescriptorIDSet(tbl.GetDependsOn()...).Ordered(),
			IsTemporary:     tbl.IsTemporary(),
			IsMaterialized:  tbl.MaterializedView(),
		})
	default:
		ev(relStatus, &scpb.Table{
			TableID:     tbl.GetID(),
			IsTemporary: tbl.IsTemporary(),
		})
	}

	ev(scpb.Status_PUBLIC, &scpb.ObjectParent{
		ObjectID:       tbl.GetID(),
		ParentSchemaID: tbl.GetParentSchemaID(),
	})
	if l := tbl.GetLocalityConfig(); l != nil {
		ev(scpb.Status_PUBLIC, &scpb.TableLocality{
			TableID:        tbl.GetID(),
			LocalityConfig: *protoutil.Clone(l).(*catpb.LocalityConfig),
		})
	}
	{
		// TODO(postamar): proper handling of comment
		ev(scpb.Status_PUBLIC, &scpb.TableComment{
			TableID: tbl.GetID(),
			Comment: scpb.PlaceHolderComment,
		})
	}
	if !tbl.IsSequence() {
		_ = tbl.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			ev(scpb.Status_PUBLIC, &scpb.ColumnFamily{
				TableID:  tbl.GetID(),
				FamilyID: family.ID,
				Name:     family.Name,
			})
			return nil
		})
		for _, col := range tbl.AllColumns() {
			if col.IsSystemColumn() {
				continue
			}
			walkColumn(ev, tbl, col)
		}
	}
	if (tbl.IsTable() && !tbl.IsVirtualTable()) || tbl.MaterializedView() {
		for _, idx := range tbl.AllIndexes() {
			walkIndex(ev, tbl, idx)
		}
		if ttl := tbl.GetRowLevelTTL(); ttl != nil {
			ev(scpb.Status_PUBLIC, &scpb.RowLevelTTL{
				TableID:     tbl.GetID(),
				RowLevelTTL: *ttl,
			})
		}
	}
	for _, c := range tbl.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		walkUniqueWithoutIndexConstraint(ev, tbl, c)
	}
	for _, c := range tbl.AllActiveAndInactiveChecks() {
		walkCheckConstraint(ev, tbl, c)
	}
	for _, c := range tbl.AllActiveAndInactiveForeignKeys() {
		walkForeignKeyConstraint(ev, tbl, c)
	}

	_ = tbl.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		backRefs.Add(dep.ID)
		return nil
	})
	_ = tbl.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		backRefs.Add(fk.OriginTableID)
		return nil
	})
	return backRefs
}

func walkColumn(ev ElementVisitor, tbl catalog.TableDescriptor, col catalog.Column) {
	onErrPanic := func(err error) {
		if err == nil {
			return
		}
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "column %q in table %q (%d)",
			col.GetName(), tbl.GetName(), tbl.GetID()))
	}
	{
		column := &scpb.Column{
			TableID:                           tbl.GetID(),
			ColumnID:                          col.GetID(),
			Nullable:                          col.IsNullable(),
			Virtual:                           col.IsVirtual(),
			Hidden:                            col.IsHidden(),
			Inaccessible:                      col.IsInaccessible(),
			GeneratedAsIdentityType:           col.GetGeneratedAsIdentityType(),
			GeneratedAsIdentitySequenceOption: col.GetGeneratedAsIdentitySequenceOption(),
			PgAttributeNum:                    col.ColumnDesc().PGAttributeNum,
			SystemColumnKind:                  col.ColumnDesc().SystemColumnKind,
		}
		_ = tbl.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			if catalog.MakeTableColSet(family.ColumnIDs...).Contains(col.GetID()) {
				column.FamilyID = family.ID
				return iterutil.StopIteration()
			}
			return nil
		})
		if col.HasType() {
			typeT, err := newTypeT(col.GetType())
			onErrPanic(err)
			column.TypeT = *typeT
		}
		if col.IsComputed() {
			expr, err := newExpression(col.GetComputeExpr())
			onErrPanic(err)
			column.ComputeExpr = expr
		}
		ev(maybeMutationStatus(col), column)
	}
	ev(scpb.Status_PUBLIC, &scpb.ColumnName{
		TableID:  tbl.GetID(),
		ColumnID: col.GetID(),
		Name:     col.GetName(),
	})
	if col.HasDefault() {
		expr, err := newExpression(col.GetDefaultExpr())
		onErrPanic(err)
		ev(scpb.Status_PUBLIC, &scpb.ColumnDefaultExpression{
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
			Expression: *expr,
		})
	}
	if col.HasOnUpdate() {
		expr, err := newExpression(col.GetOnUpdateExpr())
		onErrPanic(err)
		ev(scpb.Status_PUBLIC, &scpb.ColumnOnUpdateExpression{
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
			Expression: *expr,
		})
	}
	for i := 0; i < col.NumOwnsSequences(); i++ {
		ev(scpb.Status_PUBLIC, &scpb.SequenceOwner{
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
			SequenceID: col.GetOwnsSequenceID(i),
		})
	}
	// TODO(postamar): proper handling of comments
	ev(scpb.Status_PUBLIC, &scpb.ColumnComment{
		TableID:  tbl.GetID(),
		ColumnID: col.GetID(),
		Comment:  scpb.PlaceHolderComment,
	})
}

func walkIndex(ev ElementVisitor, tbl catalog.TableDescriptor, idx catalog.Index) {
	{
		cpy := idx.IndexDescDeepCopy()
		index := scpb.Index{
			TableID:            tbl.GetID(),
			IndexID:            idx.GetID(),
			Unique:             idx.IsUnique(),
			KeyColumnIDs:       cpy.KeyColumnIDs,
			KeySuffixColumnIDs: cpy.KeySuffixColumnIDs,
			StoringColumnIDs:   cpy.StoreColumnIDs,
			CompositeColumnIDs: cpy.CompositeColumnIDs,
			Inverted:           idx.GetType() == descpb.IndexDescriptor_INVERTED,
		}
		index.KeyColumnDirections = make([]scpb.Index_Direction, len(index.KeyColumnIDs))
		for i := 0; i < idx.NumKeyColumns(); i++ {
			if idx.GetKeyColumnDirection(i) == descpb.IndexDescriptor_DESC {
				index.KeyColumnDirections[i] = scpb.Index_DESC
			}
		}
		if idx.IsSharded() {
			index.Sharding = &cpy.Sharded
		}
		idxStatus := maybeMutationStatus(idx)
		if idx.GetEncodingType() == descpb.PrimaryIndexEncoding {
			ev(idxStatus, &scpb.PrimaryIndex{Index: index})
		} else {
			ev(idxStatus, &scpb.SecondaryIndex{Index: index})
		}
		if p := idx.GetPartitioning(); p != nil && p.NumLists()+p.NumRanges() > 0 {
			ev(scpb.Status_PUBLIC, &scpb.IndexPartitioning{
				TableID:                tbl.GetID(),
				IndexID:                idx.GetID(),
				PartitioningDescriptor: cpy.Partitioning,
			})
		}
	}
	ev(scpb.Status_PUBLIC, &scpb.IndexName{
		TableID: tbl.GetID(),
		IndexID: idx.GetID(),
		Name:    idx.GetName(),
	})
	// TODO(postamar): proper handling of comments
	ev(scpb.Status_PUBLIC, &scpb.IndexComment{
		TableID: tbl.GetID(),
		IndexID: idx.GetID(),
		Comment: scpb.PlaceHolderComment,
	})
}

func walkUniqueWithoutIndexConstraint(
	ev ElementVisitor, tbl catalog.TableDescriptor, c *descpb.UniqueWithoutIndexConstraint,
) {
	// TODO(postamar): proper handling of constraint status
	ev(scpb.Status_PUBLIC, &scpb.UniqueWithoutIndexConstraint{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		ColumnIDs:    catalog.MakeTableColSet(c.ColumnIDs...).Ordered(),
	})
	ev(scpb.Status_PUBLIC, &scpb.ConstraintName{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Name:         c.Name,
	})
	// TODO(postamar): proper handling of comments
	ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Comment:      scpb.PlaceHolderComment,
	})
}

func walkCheckConstraint(
	ev ElementVisitor, tbl catalog.TableDescriptor, c *descpb.TableDescriptor_CheckConstraint,
) {
	expr, err := newExpression(c.Expr)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "check constraint %q in table %q (%d)",
			c.Name, tbl.GetName(), tbl.GetID()))
	}
	// TODO(postamar): proper handling of constraint status
	ev(scpb.Status_PUBLIC, &scpb.CheckConstraint{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		ColumnIDs:    catalog.MakeTableColSet(c.ColumnIDs...).Ordered(),
		Expression:   *expr,
	})
	ev(scpb.Status_PUBLIC, &scpb.ConstraintName{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Name:         c.Name,
	})
	// TODO(postamar): proper handling of comments
	ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Comment:      scpb.PlaceHolderComment,
	})
}

func walkForeignKeyConstraint(
	ev ElementVisitor, tbl catalog.TableDescriptor, c *descpb.ForeignKeyConstraint,
) {
	// TODO(postamar): proper handling of constraint status
	ev(scpb.Status_PUBLIC, &scpb.ForeignKeyConstraint{
		TableID:             tbl.GetID(),
		ConstraintID:        c.ConstraintID,
		ColumnIDs:           catalog.MakeTableColSet(c.OriginColumnIDs...).Ordered(),
		ReferencedTableID:   c.ReferencedTableID,
		ReferencedColumnIDs: catalog.MakeTableColSet(c.ReferencedColumnIDs...).Ordered(),
	})
	ev(scpb.Status_PUBLIC, &scpb.ConstraintName{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Name:         c.Name,
	})
	// TODO(postamar): proper handling of comments
	ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Comment:      scpb.PlaceHolderComment,
	})
}
