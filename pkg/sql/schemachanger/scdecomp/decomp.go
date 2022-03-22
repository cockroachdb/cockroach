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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// ElementVisitor is the type of the visitor callback function used by
// WalkDescriptor.
type ElementVisitor func(status scpb.Status, element scpb.Element)

type walkCtx struct {
	desc                 catalog.Descriptor
	ev                   ElementVisitor
	lookupFn             func(id catid.DescID) catalog.Descriptor
	cachedTypeIDClosures map[catid.DescID]map[catid.DescID]struct{}
	backRefs             catalog.DescriptorIDSet
}

// WalkDescriptor walks through the elements which are implicitly defined in
// the descriptor, and returns the set of descriptor IDs back-referenced by
// the descriptor. The back-references are not elements themselves and instead
// are owned by the forward-referencing elements. Any errors are panicked.
//
// This function assumes that the descriptor already exists, that is to say,
// as it would be after being retrieved from the system.descriptor table, as
// opposed to a newly-created mutable descriptor that only exists in memory.
// This allows us to make certain assumptions, such as all named references to
// other objects having been replaced by IDs in the table column expressions.
//
// If these assumptions are violated, this function panics an error which
// verifies scerrors.HasNotImplemented.
//
// TODO(postamar): remove the dependency on the lookup function
//  This is required to look up the the multi-region enum type ID and the
//  type ID closure of types referenced in expressions. This data should
//  instead be stored in the backing struct of the catalog.Descriptor.
func WalkDescriptor(
	desc catalog.Descriptor, lookupFn func(id catid.DescID) catalog.Descriptor, ev ElementVisitor,
) (backRefs catalog.DescriptorIDSet) {
	w := walkCtx{
		desc:                 desc,
		ev:                   ev,
		lookupFn:             lookupFn,
		cachedTypeIDClosures: make(map[catid.DescID]map[catid.DescID]struct{}),
	}
	w.walkRoot()
	w.backRefs.Remove(catid.InvalidDescID)
	return w.backRefs
}

func (w *walkCtx) walkRoot() {
	// Common elements.
	w.ev(scpb.Status_PUBLIC, &scpb.Namespace{
		DatabaseID:   w.desc.GetParentID(),
		SchemaID:     w.desc.GetParentSchemaID(),
		DescriptorID: w.desc.GetID(),
		Name:         w.desc.GetName(),
	})
	privileges := w.desc.GetPrivileges()
	w.ev(scpb.Status_PUBLIC, &scpb.Owner{
		DescriptorID: w.desc.GetID(),
		Owner:        privileges.Owner().Normalized(),
	})
	for _, user := range privileges.Users {
		w.ev(scpb.Status_PUBLIC, &scpb.UserPrivileges{
			DescriptorID: w.desc.GetID(),
			UserName:     user.User().Normalized(),
			Privileges:   user.Privileges,
		})
	}
	// Dispatch on type.
	switch d := w.desc.(type) {
	case catalog.DatabaseDescriptor:
		w.walkDatabase(d)
	case catalog.SchemaDescriptor:
		w.walkSchema(d)
	case catalog.TypeDescriptor:
		w.walkType(d)
	case catalog.TableDescriptor:
		w.walkRelation(d)
	default:
		panic(errors.AssertionFailedf("unexpected descriptor type %T: %+v",
			w.desc, w.desc))
	}
}

func (w *walkCtx) walkDatabase(db catalog.DatabaseDescriptor) {
	w.ev(descriptorStatus(db), &scpb.Database{DatabaseID: db.GetID()})
	// TODO(postamar): proper handling of comment and role setting
	w.ev(scpb.Status_PUBLIC, &scpb.DatabaseRoleSetting{
		DatabaseID: db.GetID(),
		RoleName:   scpb.PlaceHolderRoleName,
	})
	w.ev(scpb.Status_PUBLIC, &scpb.DatabaseComment{
		DatabaseID: db.GetID(),
		Comment:    scpb.PlaceHolderComment,
	})
	if db.IsMultiRegion() {
		w.ev(scpb.Status_PUBLIC, &scpb.DatabaseRegionConfig{
			DatabaseID:       db.GetID(),
			RegionEnumTypeID: db.GetRegionConfig().RegionEnumID,
		})
	}
	_ = db.ForEachNonDroppedSchema(func(id descpb.ID, name string) error {
		w.backRefs.Add(id)
		return nil
	})
}

func (w *walkCtx) walkSchema(sc catalog.SchemaDescriptor) {
	w.ev(descriptorStatus(sc), &scpb.Schema{
		SchemaID:    sc.GetID(),
		IsPublic:    sc.GetName() == catconstants.PublicSchemaName,
		IsVirtual:   sc.SchemaKind() == catalog.SchemaVirtual,
		IsTemporary: sc.SchemaKind() == catalog.SchemaTemporary,
	})
	w.ev(scpb.Status_PUBLIC, &scpb.SchemaParent{
		SchemaID:         sc.GetID(),
		ParentDatabaseID: sc.GetParentID(),
	})
	// TODO(postamar): proper handling of comment
	w.ev(scpb.Status_PUBLIC, &scpb.SchemaComment{
		SchemaID: sc.GetID(),
		Comment:  scpb.PlaceHolderComment,
	})
}

func (w *walkCtx) walkType(typ catalog.TypeDescriptor) {
	switch typ.GetKind() {
	case descpb.TypeDescriptor_ALIAS:
		typeT, err := newTypeT(typ.TypeDesc().Alias)
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "alias type %q (%d)",
				typ.GetName(), typ.GetID()))
		}
		w.ev(descriptorStatus(typ), &scpb.AliasType{
			TypeID: typ.GetID(),
			TypeT:  *typeT,
		})
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		w.ev(descriptorStatus(typ), &scpb.EnumType{
			TypeID:        typ.GetID(),
			ArrayTypeID:   typ.GetArrayTypeID(),
			IsMultiRegion: typ.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM,
		})
	default:
		panic(errors.AssertionFailedf("unsupported type kind %q", typ.GetKind()))
	}
	w.ev(scpb.Status_PUBLIC, &scpb.ObjectParent{
		ObjectID:       typ.GetID(),
		ParentSchemaID: typ.GetParentSchemaID(),
	})
	for i := 0; i < typ.NumReferencingDescriptors(); i++ {
		w.backRefs.Add(typ.GetReferencingDescriptorID(i))
	}
}

func (w *walkCtx) walkRelation(tbl catalog.TableDescriptor) {
	switch {
	case tbl.IsSequence():
		w.ev(descriptorStatus(tbl), &scpb.Sequence{
			SequenceID:  tbl.GetID(),
			IsTemporary: tbl.IsTemporary(),
		})
		if opts := tbl.GetSequenceOpts(); opts != nil {
			w.backRefs.Add(opts.SequenceOwner.OwnerTableID)
		}
	case tbl.IsView():
		w.ev(descriptorStatus(tbl), &scpb.View{
			ViewID:          tbl.GetID(),
			UsesTypeIDs:     catalog.MakeDescriptorIDSet(tbl.GetDependsOnTypes()...).Ordered(),
			UsesRelationIDs: catalog.MakeDescriptorIDSet(tbl.GetDependsOn()...).Ordered(),
			IsTemporary:     tbl.IsTemporary(),
			IsMaterialized:  tbl.MaterializedView(),
		})
	default:
		w.ev(descriptorStatus(tbl), &scpb.Table{
			TableID:     tbl.GetID(),
			IsTemporary: tbl.IsTemporary(),
		})
	}

	w.ev(scpb.Status_PUBLIC, &scpb.ObjectParent{
		ObjectID:       tbl.GetID(),
		ParentSchemaID: tbl.GetParentSchemaID(),
	})
	if l := tbl.GetLocalityConfig(); l != nil {
		w.walkLocality(tbl, l)
	}
	{
		// TODO(postamar): proper handling of comment
		w.ev(scpb.Status_PUBLIC, &scpb.TableComment{
			TableID: tbl.GetID(),
			Comment: scpb.PlaceHolderComment,
		})
	}
	if !tbl.IsSequence() {
		_ = tbl.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			w.ev(scpb.Status_PUBLIC, &scpb.ColumnFamily{
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
			w.walkColumn(tbl, col)
		}
	}
	if (tbl.IsTable() && !tbl.IsVirtualTable()) || tbl.MaterializedView() {
		for _, idx := range tbl.AllIndexes() {
			w.walkIndex(tbl, idx)
		}
		if ttl := tbl.GetRowLevelTTL(); ttl != nil {
			w.ev(scpb.Status_PUBLIC, &scpb.RowLevelTTL{
				TableID:     tbl.GetID(),
				RowLevelTTL: *ttl,
			})
		}
	}
	for _, c := range tbl.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		w.walkUniqueWithoutIndexConstraint(tbl, c)
	}
	for _, c := range tbl.AllActiveAndInactiveChecks() {
		w.walkCheckConstraint(tbl, c)
	}
	for _, c := range tbl.AllActiveAndInactiveForeignKeys() {
		w.walkForeignKeyConstraint(tbl, c)
	}

	_ = tbl.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		w.backRefs.Add(dep.ID)
		return nil
	})
	_ = tbl.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		w.backRefs.Add(fk.OriginTableID)
		return nil
	})
}

func (w *walkCtx) walkLocality(tbl catalog.TableDescriptor, l *catpb.LocalityConfig) {
	if g := l.GetGlobal(); g != nil {
		w.ev(scpb.Status_PUBLIC, &scpb.TableLocalityGlobal{
			TableID: tbl.GetID(),
		})
		return
	} else if rbr := l.GetRegionalByRow(); rbr != nil {
		var as string
		if rbr.As != nil {
			as = *rbr.As
		}
		w.ev(scpb.Status_PUBLIC, &scpb.TableLocalityRegionalByRow{
			TableID: tbl.GetID(),
			As:      as,
		})
	} else if rbt := l.GetRegionalByTable(); rbt != nil {
		if rgn := rbt.Region; rgn != nil {
			parent := w.lookupFn(tbl.GetParentID())
			db, err := catalog.AsDatabaseDescriptor(parent)
			if err != nil {
				panic(err)
			}
			id, err := db.MultiRegionEnumID()
			if err != nil {
				panic(err)
			}
			w.ev(scpb.Status_PUBLIC, &scpb.TableLocalitySecondaryRegion{
				TableID:          tbl.GetID(),
				RegionName:       *rgn,
				RegionEnumTypeID: id,
			})
		} else {
			w.ev(scpb.Status_PUBLIC, &scpb.TableLocalityPrimaryRegion{TableID: tbl.GetID()})
		}
	}
}

func (w *walkCtx) walkColumn(tbl catalog.TableDescriptor, col catalog.Column) {
	onErrPanic := func(err error) {
		if err == nil {
			return
		}
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "column %q in table %q (%d)",
			col.GetName(), tbl.GetName(), tbl.GetID()))
	}
	column := &scpb.Column{
		TableID:                           tbl.GetID(),
		ColumnID:                          col.GetID(),
		IsHidden:                          col.IsHidden(),
		IsInaccessible:                    col.IsInaccessible(),
		GeneratedAsIdentityType:           col.GetGeneratedAsIdentityType(),
		GeneratedAsIdentitySequenceOption: col.GetGeneratedAsIdentitySequenceOption(),
		PgAttributeNum:                    col.ColumnDesc().PGAttributeNum,
	}
	w.ev(maybeMutationStatus(col), column)
	w.ev(scpb.Status_PUBLIC, &scpb.ColumnName{
		TableID:  tbl.GetID(),
		ColumnID: col.GetID(),
		Name:     col.GetName(),
	})
	{
		columnType := &scpb.ColumnType{
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
			IsNullable: col.IsNullable(),
			IsVirtual:  col.IsVirtual(),
		}
		_ = tbl.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			if catalog.MakeTableColSet(family.ColumnIDs...).Contains(col.GetID()) {
				columnType.FamilyID = family.ID
				return iterutil.StopIteration()
			}
			return nil
		})
		typeT, err := newTypeT(col.GetType())
		onErrPanic(err)
		columnType.TypeT = *typeT

		if col.IsComputed() {
			expr, err := w.newExpression(col.GetComputeExpr())
			onErrPanic(err)
			columnType.ComputeExpr = expr
		}
		w.ev(scpb.Status_PUBLIC, columnType)
	}
	if col.HasDefault() {
		expr, err := w.newExpression(col.GetDefaultExpr())
		onErrPanic(err)
		w.ev(scpb.Status_PUBLIC, &scpb.ColumnDefaultExpression{
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
			Expression: *expr,
		})
	}
	if col.HasOnUpdate() {
		expr, err := w.newExpression(col.GetOnUpdateExpr())
		onErrPanic(err)
		w.ev(scpb.Status_PUBLIC, &scpb.ColumnOnUpdateExpression{
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
			Expression: *expr,
		})
	}
	// TODO(postamar): proper handling of comments
	w.ev(scpb.Status_PUBLIC, &scpb.ColumnComment{
		TableID:  tbl.GetID(),
		ColumnID: col.GetID(),
		Comment:  scpb.PlaceHolderComment,
	})
	owns := catalog.MakeDescriptorIDSet(col.ColumnDesc().OwnsSequenceIds...)
	owns.Remove(catid.InvalidDescID)
	owns.ForEach(func(id descpb.ID) {
		w.ev(scpb.Status_PUBLIC, &scpb.SequenceOwner{
			SequenceID: id,
			TableID:    tbl.GetID(),
			ColumnID:   col.GetID(),
		})
	})
}

func (w *walkCtx) walkIndex(tbl catalog.TableDescriptor, idx catalog.Index) {
	onErrPanic := func(err error) {
		if err == nil {
			return
		}
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "index %q in table %q (%d)",
			idx.GetName(), tbl.GetName(), tbl.GetID()))
	}
	{
		cpy := idx.IndexDescDeepCopy()
		index := scpb.Index{
			TableID:            tbl.GetID(),
			IndexID:            idx.GetID(),
			IsUnique:           idx.IsUnique(),
			KeyColumnIDs:       cpy.KeyColumnIDs,
			KeySuffixColumnIDs: cpy.KeySuffixColumnIDs,
			StoringColumnIDs:   cpy.StoreColumnIDs,
			CompositeColumnIDs: cpy.CompositeColumnIDs,
			IsInverted:         idx.GetType() == descpb.IndexDescriptor_INVERTED,
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
			w.ev(idxStatus, &scpb.PrimaryIndex{Index: index})
		} else {
			sec := &scpb.SecondaryIndex{Index: index}
			if idx.IsPartial() {
				pp, err := w.newExpression(idx.GetPredicate())
				onErrPanic(err)
				w.ev(scpb.Status_PUBLIC, &scpb.SecondaryIndexPartial{
					TableID:    index.TableID,
					IndexID:    index.IndexID,
					Expression: *pp,
				})
			}
			w.ev(idxStatus, sec)
		}
		if p := idx.GetPartitioning(); p != nil && p.NumLists()+p.NumRanges() > 0 {
			w.ev(scpb.Status_PUBLIC, &scpb.IndexPartitioning{
				TableID:                tbl.GetID(),
				IndexID:                idx.GetID(),
				PartitioningDescriptor: cpy.Partitioning,
			})
		}
	}
	w.ev(scpb.Status_PUBLIC, &scpb.IndexName{
		TableID: tbl.GetID(),
		IndexID: idx.GetID(),
		Name:    idx.GetName(),
	})
	// TODO(postamar): proper handling of comments
	w.ev(scpb.Status_PUBLIC, &scpb.IndexComment{
		TableID: tbl.GetID(),
		IndexID: idx.GetID(),
		Comment: scpb.PlaceHolderComment,
	})
}

func (w *walkCtx) walkUniqueWithoutIndexConstraint(
	tbl catalog.TableDescriptor, c *descpb.UniqueWithoutIndexConstraint,
) {
	// TODO(postamar): proper handling of constraint status
	w.ev(scpb.Status_PUBLIC, &scpb.UniqueWithoutIndexConstraint{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		ColumnIDs:    catalog.MakeTableColSet(c.ColumnIDs...).Ordered(),
	})
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintName{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Name:         c.Name,
	})
	// TODO(postamar): proper handling of comments
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Comment:      scpb.PlaceHolderComment,
	})
}

func (w *walkCtx) walkCheckConstraint(
	tbl catalog.TableDescriptor, c *descpb.TableDescriptor_CheckConstraint,
) {
	expr, err := w.newExpression(c.Expr)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "check constraint %q in table %q (%d)",
			c.Name, tbl.GetName(), tbl.GetID()))
	}
	// TODO(postamar): proper handling of constraint status
	w.ev(scpb.Status_PUBLIC, &scpb.CheckConstraint{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		ColumnIDs:    catalog.MakeTableColSet(c.ColumnIDs...).Ordered(),
		Expression:   *expr,
	})
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintName{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Name:         c.Name,
	})
	// TODO(postamar): proper handling of comments
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Comment:      scpb.PlaceHolderComment,
	})
}

func (w *walkCtx) walkForeignKeyConstraint(
	tbl catalog.TableDescriptor, c *descpb.ForeignKeyConstraint,
) {
	// TODO(postamar): proper handling of constraint status
	w.ev(scpb.Status_PUBLIC, &scpb.ForeignKeyConstraint{
		TableID:             tbl.GetID(),
		ConstraintID:        c.ConstraintID,
		ColumnIDs:           catalog.MakeTableColSet(c.OriginColumnIDs...).Ordered(),
		ReferencedTableID:   c.ReferencedTableID,
		ReferencedColumnIDs: catalog.MakeTableColSet(c.ReferencedColumnIDs...).Ordered(),
	})
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintName{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Name:         c.Name,
	})
	// TODO(postamar): proper handling of comments
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
		TableID:      tbl.GetID(),
		ConstraintID: c.ConstraintID,
		Comment:      scpb.PlaceHolderComment,
	})
}
