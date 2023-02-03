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
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type walkCtx struct {
	ctx                  context.Context
	desc                 catalog.Descriptor
	ev                   ElementVisitor
	lookupFn             func(id catid.DescID) catalog.Descriptor
	cachedTypeIDClosures map[catid.DescID]catalog.DescriptorIDSet
	backRefs             catalog.DescriptorIDSet
	commentReader        CommentGetter
	zoneConfigReader     ZoneConfigGetter
	clusterVersion       clusterversion.ClusterVersion
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
//
//	This is required to look up the the multi-region enum type ID and the
//	type ID closure of types referenced in expressions. This data should
//	instead be stored in the backing struct of the catalog.Descriptor.
func WalkDescriptor(
	ctx context.Context,
	desc catalog.Descriptor,
	lookupFn func(id catid.DescID) catalog.Descriptor,
	ev ElementVisitor,
	commentReader CommentGetter,
	zoneConfigReader ZoneConfigGetter,
	clusterVersion clusterversion.ClusterVersion,
) (backRefs catalog.DescriptorIDSet) {
	w := walkCtx{
		ctx:                  ctx,
		desc:                 desc,
		ev:                   ev,
		lookupFn:             lookupFn,
		cachedTypeIDClosures: make(map[catid.DescID]catalog.DescriptorIDSet),
		commentReader:        commentReader,
		zoneConfigReader:     zoneConfigReader,
		clusterVersion:       clusterVersion,
	}
	w.walkRoot()
	w.backRefs.Remove(catid.InvalidDescID)
	return w.backRefs
}

func (w *walkCtx) walkRoot() {
	// Common elements.
	if !w.desc.SkipNamespace() {
		w.ev(scpb.Status_PUBLIC, &scpb.Namespace{
			DatabaseID:   w.desc.GetParentID(),
			SchemaID:     w.desc.GetParentSchemaID(),
			DescriptorID: w.desc.GetID(),
			Name:         w.desc.GetName(),
		})
	}
	privileges := w.desc.GetPrivileges()
	w.ev(scpb.Status_PUBLIC, &scpb.Owner{
		DescriptorID: w.desc.GetID(),
		Owner:        privileges.Owner().Normalized(),
	})
	for _, user := range privileges.Users {
		w.ev(scpb.Status_PUBLIC, &scpb.UserPrivileges{
			DescriptorID:    w.desc.GetID(),
			UserName:        user.User().Normalized(),
			Privileges:      user.Privileges,
			WithGrantOption: user.WithGrantOption,
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
	case catalog.FunctionDescriptor:
		w.walkFunction(d)
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
	if comment, ok := w.commentReader.GetDatabaseComment(db.GetID()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.DatabaseComment{
			DatabaseID: db.GetID(),
			Comment:    comment,
		})
	}
	if db.IsMultiRegion() {
		w.ev(scpb.Status_PUBLIC, &scpb.DatabaseRegionConfig{
			DatabaseID:       db.GetID(),
			RegionEnumTypeID: db.GetRegionConfig().RegionEnumID,
		})
	}
	w.ev(scpb.Status_PUBLIC, &scpb.DatabaseData{DatabaseID: db.GetID()})
	_ = db.ForEachSchema(func(id descpb.ID, name string) error {
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
	if comment, ok := w.commentReader.GetSchemaComment(sc.GetID()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.SchemaComment{
			SchemaID: sc.GetID(),
			Comment:  comment,
		})
	}
}

func (w *walkCtx) walkType(typ catalog.TypeDescriptor) {
	if alias := typ.AsAliasTypeDescriptor(); alias != nil {
		typeT := newTypeT(alias.Aliased())
		w.ev(descriptorStatus(typ), &scpb.AliasType{
			TypeID: typ.GetID(),
			TypeT:  *typeT,
		})
	} else if enum := typ.AsEnumTypeDescriptor(); enum != nil {
		w.ev(descriptorStatus(enum), &scpb.EnumType{
			TypeID:        enum.GetID(),
			ArrayTypeID:   enum.GetArrayTypeID(),
			IsMultiRegion: enum.AsRegionEnumTypeDescriptor() != nil,
		})
		for ord := 0; ord < enum.NumEnumMembers(); ord++ {
			w.ev(descriptorStatus(enum), &scpb.EnumTypeValue{
				TypeID:                 enum.GetID(),
				PhysicalRepresentation: enum.GetMemberPhysicalRepresentation(ord),
				LogicalRepresentation:  enum.GetMemberLogicalRepresentation(ord),
			})
		}
	} else if comp := typ.AsCompositeTypeDescriptor(); comp != nil {
		w.ev(descriptorStatus(typ), &scpb.CompositeType{
			TypeID:      comp.GetID(),
			ArrayTypeID: comp.GetArrayTypeID(),
		})
		for i := 0; i < comp.NumElements(); i++ {
			typeT := newTypeT(comp.GetElementType(i))
			w.ev(descriptorStatus(typ), &scpb.CompositeTypeAttrType{
				CompositeTypeID: typ.GetID(),
				TypeT:           *typeT,
			})
			w.ev(descriptorStatus(typ), &scpb.CompositeTypeAttrName{
				CompositeTypeID: typ.GetID(),
				Name:            comp.GetElementLabel(i),
			})
		}
	} else {
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
			ForwardReferences: func(tbl catalog.TableDescriptor) []*scpb.View_Reference {
				result := make([]*scpb.View_Reference, 0)

				// For each `to` relation, find the back reference to `tbl`.
				for _, toID := range tbl.GetDependsOn() {
					to := w.lookupFn(toID)
					toDesc, err := catalog.AsTableDescriptor(to)
					if err != nil {
						panic(err)
					}

					_ = toDesc.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
						if dep.ID != tbl.GetID() {
							return nil
						}
						ref := &scpb.View_Reference{
							ToID:    toID,
							IndexID: dep.IndexID,
							ColumnIDs: func(colIDs []catid.ColumnID) []catid.ColumnID {
								// de-duplicate, remove-zeros, and order column IDs from `dep.ColumnIDs`.
								result := catalog.MakeTableColSet()
								for _, colID := range colIDs {
									if colID != 0 && !result.Contains(colID) {
										result.Add(colID)
									}
								}
								return result.Ordered()
							}(dep.ColumnIDs),
						}
						result = append(result, ref)
						return nil
					})
				}

				return result
			}(tbl),
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
	if tbl.IsPartitionAllBy() {
		w.ev(descriptorStatus(tbl), &scpb.TablePartitioning{
			TableID: tbl.GetID(),
		})
	}
	if l := tbl.GetLocalityConfig(); l != nil {
		w.walkLocality(tbl, l)
	}
	{
		if comment, ok := w.commentReader.GetTableComment(tbl.GetID()); ok {
			w.ev(scpb.Status_PUBLIC, &scpb.TableComment{
				TableID: tbl.GetID(),
				Comment: comment,
			})
		}
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
	for _, c := range tbl.UniqueConstraintsWithoutIndex() {
		w.walkUniqueWithoutIndexConstraint(tbl, c)
	}
	for _, c := range tbl.CheckConstraints() {
		w.walkCheckConstraint(tbl, c)
	}
	for _, c := range tbl.OutboundForeignKeys() {
		w.walkForeignKeyConstraint(tbl, c)
	}

	_ = tbl.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
		w.backRefs.Add(dep.ID)
		return nil
	})
	for _, fk := range tbl.InboundForeignKeys() {
		w.backRefs.Add(fk.GetOriginTableID())
	}
	// Add a zone config element which is a stop gap to allow us to block
	// operations on tables. To minimize RTT impact limit
	// this to only tables and materialized views.
	if (tbl.IsTable() && !tbl.IsVirtualTable()) || tbl.MaterializedView() {
		zoneCfg, err := w.zoneConfigReader.GetZoneConfig(w.ctx, tbl.GetID())
		if err != nil {
			panic(err)
		}
		if zoneCfg != nil {
			w.ev(scpb.Status_PUBLIC,
				&scpb.TableZoneConfig{
					TableID: tbl.GetID(),
				})
		}
	}
	if tbl.IsPhysicalTable() {
		w.ev(scpb.Status_PUBLIC, &scpb.TableData{TableID: tbl.GetID(), DatabaseID: tbl.GetParentID()})
	}
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
		GeneratedAsIdentitySequenceOption: col.GetGeneratedAsIdentitySequenceOptionStr(),
		PgAttributeNum:                    col.GetPGAttributeNum(),
		IsSystemColumn:                    col.IsSystemColumn(),
	}
	w.ev(maybeMutationStatus(col), column)
	w.ev(scpb.Status_PUBLIC, &scpb.ColumnName{
		TableID:  tbl.GetID(),
		ColumnID: col.GetID(),
		Name:     col.GetName(),
	})
	{
		columnType := &scpb.ColumnType{
			TableID:                 tbl.GetID(),
			ColumnID:                col.GetID(),
			IsNullable:              col.IsNullable(),
			IsVirtual:               col.IsVirtual(),
			ElementCreationMetadata: NewElementCreationMetadata(w.clusterVersion),
		}
		_ = tbl.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			if catalog.MakeTableColSet(family.ColumnIDs...).Contains(col.GetID()) {
				columnType.FamilyID = family.ID
				return iterutil.StopIteration()
			}
			return nil
		})
		typeT := newTypeT(col.GetType())
		columnType.TypeT = *typeT

		if col.IsComputed() {
			expr, err := w.newExpression(col.GetComputeExpr())
			onErrPanic(err)
			columnType.ComputeExpr = expr
		}
		w.ev(scpb.Status_PUBLIC, columnType)
	}
	if !col.IsNullable() {
		w.ev(scpb.Status_PUBLIC, &scpb.ColumnNotNull{
			TableID:  tbl.GetID(),
			ColumnID: col.GetID(),
		})
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
	if comment, ok := w.commentReader.GetColumnComment(tbl.GetID(), col.GetPGAttributeNum()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.ColumnComment{
			TableID:        tbl.GetID(),
			ColumnID:       col.GetID(),
			Comment:        comment,
			PgAttributeNum: col.GetPGAttributeNum(),
		})
	}
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
			TableID:             tbl.GetID(),
			IndexID:             idx.GetID(),
			IsUnique:            idx.IsUnique(),
			IsInverted:          idx.GetType() == descpb.IndexDescriptor_INVERTED,
			IsCreatedExplicitly: idx.IsCreatedExplicitly(),
			ConstraintID:        idx.GetConstraintID(),
			IsNotVisible:        idx.IsNotVisible(),
		}
		if geoConfig := idx.GetGeoConfig(); !geoConfig.IsEmpty() {
			index.GeoConfig = protoutil.Clone(&geoConfig).(*geoindex.Config)
		}
		for i, c := range cpy.KeyColumnIDs {
			invertedKind := catpb.InvertedIndexColumnKind_DEFAULT
			if index.IsInverted && c == idx.InvertedColumnID() {
				invertedKind = idx.InvertedColumnKind()
			}
			w.ev(scpb.Status_PUBLIC, &scpb.IndexColumn{
				TableID:       tbl.GetID(),
				IndexID:       idx.GetID(),
				ColumnID:      c,
				OrdinalInKind: uint32(i),
				Kind:          scpb.IndexColumn_KEY,
				Direction:     cpy.KeyColumnDirections[i],
				Implicit:      i < idx.ImplicitPartitioningColumnCount(),
				InvertedKind:  invertedKind,
			})
		}
		for i, c := range cpy.KeySuffixColumnIDs {
			w.ev(scpb.Status_PUBLIC, &scpb.IndexColumn{
				TableID:       tbl.GetID(),
				IndexID:       idx.GetID(),
				ColumnID:      c,
				OrdinalInKind: uint32(i),
				Kind:          scpb.IndexColumn_KEY_SUFFIX,
			})
		}
		for i, c := range cpy.StoreColumnIDs {
			w.ev(scpb.Status_PUBLIC, &scpb.IndexColumn{
				TableID:       tbl.GetID(),
				IndexID:       idx.GetID(),
				ColumnID:      c,
				OrdinalInKind: uint32(i),
				Kind:          scpb.IndexColumn_STORED,
			})
		}
		if idx.IsSharded() {
			index.Sharding = &cpy.Sharded
		}
		idxStatus := maybeMutationStatus(idx)
		if idx.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
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
	if comment, ok := w.commentReader.GetIndexComment(tbl.GetID(), idx.GetID()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.IndexComment{
			TableID: tbl.GetID(),
			IndexID: idx.GetID(),
			Comment: comment,
		})
	}
	if constraintID := idx.GetConstraintID(); constraintID != 0 {
		if comment, ok := w.commentReader.GetConstraintComment(tbl.GetID(), constraintID); ok {
			w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
				TableID:      tbl.GetID(),
				ConstraintID: constraintID,
				Comment:      comment,
			})
		}
	}
	w.ev(scpb.Status_PUBLIC, &scpb.IndexData{
		TableID: tbl.GetID(),
		IndexID: idx.GetID(),
	})
}

func (w *walkCtx) walkUniqueWithoutIndexConstraint(
	tbl catalog.TableDescriptor, c catalog.UniqueWithoutIndexConstraint,
) {
	var expr *scpb.Expression
	var err error
	if c.IsPartial() {
		expr, err = w.newExpression(c.GetPredicate())
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "unique without index constraint %q in table %q (%d)",
				c.GetName(), tbl.GetName(), tbl.GetID()))
		}
	}
	if c.IsConstraintUnvalidated() && w.clusterVersion.IsActive(clusterversion.V23_1) {
		uwi := &scpb.UniqueWithoutIndexConstraintUnvalidated{
			TableID:      tbl.GetID(),
			ConstraintID: c.GetConstraintID(),
			ColumnIDs:    c.CollectKeyColumnIDs().Ordered(),
			Predicate:    expr,
		}
		w.ev(scpb.Status_PUBLIC, uwi)
	} else {
		uwi := &scpb.UniqueWithoutIndexConstraint{
			TableID:      tbl.GetID(),
			ConstraintID: c.GetConstraintID(),
			ColumnIDs:    c.CollectKeyColumnIDs().Ordered(),
			Predicate:    expr,
		}
		w.ev(scpb.Status_PUBLIC, uwi)
	}
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintWithoutIndexName{
		TableID:      tbl.GetID(),
		ConstraintID: c.GetConstraintID(),
		Name:         c.GetName(),
	})
	if comment, ok := w.commentReader.GetConstraintComment(tbl.GetID(), c.GetConstraintID()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
			TableID:      tbl.GetID(),
			ConstraintID: c.GetConstraintID(),
			Comment:      comment,
		})
	}
}

func (w *walkCtx) walkCheckConstraint(tbl catalog.TableDescriptor, c catalog.CheckConstraint) {
	expr, err := w.newExpression(c.GetExpr())
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "check constraint %q in table %q (%d)",
			c.GetName(), tbl.GetName(), tbl.GetID()))
	}
	if c.IsConstraintUnvalidated() && w.clusterVersion.IsActive(clusterversion.V23_1) {
		w.ev(scpb.Status_PUBLIC, &scpb.CheckConstraintUnvalidated{
			TableID:      tbl.GetID(),
			ConstraintID: c.GetConstraintID(),
			ColumnIDs:    c.CollectReferencedColumnIDs().Ordered(),
			Expression:   *expr,
		})
	} else {
		w.ev(scpb.Status_PUBLIC, &scpb.CheckConstraint{
			TableID:               tbl.GetID(),
			ConstraintID:          c.GetConstraintID(),
			ColumnIDs:             c.CollectReferencedColumnIDs().Ordered(),
			Expression:            *expr,
			FromHashShardedColumn: c.IsHashShardingConstraint(),
		})
	}
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintWithoutIndexName{
		TableID:      tbl.GetID(),
		ConstraintID: c.GetConstraintID(),
		Name:         c.GetName(),
	})
	if comment, ok := w.commentReader.GetConstraintComment(tbl.GetID(), c.GetConstraintID()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
			TableID:      tbl.GetID(),
			ConstraintID: c.GetConstraintID(),
			Comment:      comment,
		})
	}
}

func (w *walkCtx) walkForeignKeyConstraint(
	tbl catalog.TableDescriptor, c catalog.ForeignKeyConstraint,
) {
	if c.IsConstraintUnvalidated() && w.clusterVersion.IsActive(clusterversion.V23_1) {
		w.ev(scpb.Status_PUBLIC, &scpb.ForeignKeyConstraintUnvalidated{
			TableID:                 tbl.GetID(),
			ConstraintID:            c.GetConstraintID(),
			ColumnIDs:               c.ForeignKeyDesc().OriginColumnIDs,
			ReferencedTableID:       c.GetReferencedTableID(),
			ReferencedColumnIDs:     c.ForeignKeyDesc().ReferencedColumnIDs,
			OnUpdateAction:          c.OnUpdate(),
			OnDeleteAction:          c.OnDelete(),
			CompositeKeyMatchMethod: c.Match(),
		})
	} else {
		w.ev(scpb.Status_PUBLIC, &scpb.ForeignKeyConstraint{
			TableID:                 tbl.GetID(),
			ConstraintID:            c.GetConstraintID(),
			ColumnIDs:               c.ForeignKeyDesc().OriginColumnIDs,
			ReferencedTableID:       c.GetReferencedTableID(),
			ReferencedColumnIDs:     c.ForeignKeyDesc().ReferencedColumnIDs,
			OnUpdateAction:          c.OnUpdate(),
			OnDeleteAction:          c.OnDelete(),
			CompositeKeyMatchMethod: c.Match(),
		})
	}
	w.ev(scpb.Status_PUBLIC, &scpb.ConstraintWithoutIndexName{
		TableID:      tbl.GetID(),
		ConstraintID: c.GetConstraintID(),
		Name:         c.GetName(),
	})
	if comment, ok := w.commentReader.GetConstraintComment(tbl.GetID(), c.GetConstraintID()); ok {
		w.ev(scpb.Status_PUBLIC, &scpb.ConstraintComment{
			TableID:      tbl.GetID(),
			ConstraintID: c.GetConstraintID(),
			Comment:      comment,
		})
	}
}

func (w *walkCtx) walkFunction(fnDesc catalog.FunctionDescriptor) {
	typeT := newTypeT(fnDesc.GetReturnType().Type)
	fn := &scpb.Function{
		FunctionID: fnDesc.GetID(),
		ReturnSet:  fnDesc.GetReturnType().ReturnSet,
		ReturnType: *typeT,
		Params:     make([]scpb.Function_Parameter, len(fnDesc.GetParams())),
	}
	for i, param := range fnDesc.GetParams() {
		typeT := newTypeT(param.Type)
		fn.Params[i] = scpb.Function_Parameter{
			Name:  param.Name,
			Class: catpb.FunctionParamClass{Class: param.Class},
			Type:  *typeT,
		}
		if param.DefaultExpr != nil {
			expr, err := w.newExpression(*param.DefaultExpr)
			if err != nil {
				panic(err)
			}
			w.ev(scpb.Status_PUBLIC, &scpb.FunctionParamDefaultExpression{
				FunctionID: fnDesc.GetID(),
				Ordinal:    uint32(i),
				Expression: *expr,
			})
		}
	}

	w.ev(descriptorStatus(fnDesc), fn)
	w.ev(scpb.Status_PUBLIC, &scpb.ObjectParent{
		ObjectID:       fnDesc.GetID(),
		ParentSchemaID: fnDesc.GetParentSchemaID(),
	})
	w.ev(scpb.Status_PUBLIC, &scpb.FunctionName{
		FunctionID: fnDesc.GetID(),
		Name:       fnDesc.GetName(),
	})
	w.ev(scpb.Status_PUBLIC, &scpb.FunctionVolatility{
		FunctionID: fnDesc.GetID(),
		Volatility: catpb.FunctionVolatility{Volatility: fnDesc.GetVolatility()},
	})
	w.ev(scpb.Status_PUBLIC, &scpb.FunctionLeakProof{
		FunctionID: fnDesc.GetID(),
		LeakProof:  fnDesc.GetLeakProof(),
	})
	w.ev(scpb.Status_PUBLIC, &scpb.FunctionNullInputBehavior{
		FunctionID:        fnDesc.GetID(),
		NullInputBehavior: catpb.FunctionNullInputBehavior{NullInputBehavior: fnDesc.GetNullInputBehavior()},
	})

	fnBody := &scpb.FunctionBody{
		FunctionID:  fnDesc.GetID(),
		Body:        fnDesc.GetFunctionBody(),
		Lang:        catpb.FunctionLanguage{Lang: fnDesc.GetLanguage()},
		UsesTypeIDs: fnDesc.GetDependsOnTypes(),
		// TODO(chengxiong): add UsesFunctionIDs when UDF usage is allowed.
	}
	dedupeColIDs := func(colIDs []catid.ColumnID) []catid.ColumnID {
		ret := catalog.MakeTableColSet()
		for _, id := range colIDs {
			ret.Add(id)
		}
		return ret.Ordered()
	}
	for _, toID := range fnDesc.GetDependsOn() {
		to := w.lookupFn(toID)
		toDesc, err := catalog.AsTableDescriptor(to)
		if err != nil {
			panic(err)
		}
		if toDesc.IsSequence() {
			fnBody.UsesSequenceIDs = append(fnBody.UsesSequenceIDs, toDesc.GetID())
		} else if toDesc.IsView() {
			if err := toDesc.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
				if dep.ID != fnDesc.GetID() {
					return nil
				}
				fnBody.UsesViews = append(fnBody.UsesViews, scpb.FunctionBody_ViewReference{
					ViewID:    toDesc.GetID(),
					ColumnIDs: dedupeColIDs(dep.ColumnIDs),
				})
				return nil
			}); err != nil {
				panic(err)
			}
		} else {
			if err := toDesc.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
				if dep.ID != fnDesc.GetID() {
					return nil
				}
				fnBody.UsesTables = append(fnBody.UsesTables, scpb.FunctionBody_TableReference{
					TableID:   toDesc.GetID(),
					IndexID:   dep.IndexID,
					ColumnIDs: dedupeColIDs(dep.ColumnIDs),
				})
				return nil
			}); err != nil {
				panic(err)
			}
		}
	}
	w.ev(scpb.Status_PUBLIC, fnBody)
}
