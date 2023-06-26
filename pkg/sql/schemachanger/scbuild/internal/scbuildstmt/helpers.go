// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func qualifiedName(b BuildCtx, id catid.DescID) string {
	_, _, ns := scpb.FindNamespace(b.QueryByID(id))
	_, _, sc := scpb.FindNamespace(b.QueryByID(ns.SchemaID))
	_, _, db := scpb.FindNamespace(b.QueryByID(ns.DatabaseID))
	if db == nil {
		return ns.Name
	}
	if sc == nil {
		return db.Name + "." + ns.Name
	}
	return db.Name + "." + sc.Name + "." + ns.Name
}

func simpleName(b BuildCtx, id catid.DescID) string {
	_, _, ns := scpb.FindNamespace(b.QueryByID(id))
	return ns.Name
}

// dropRestrictDescriptor contains the common logic for dropping something with
// RESTRICT.
func dropRestrictDescriptor(b BuildCtx, id catid.DescID) (hasChanged bool) {
	undropped := undroppedElements(b, id)
	if undropped.IsEmpty() {
		return false
	}
	undropped.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		b.CheckPrivilege(e, privilege.DROP)
		dropElementWhenDroppingDescriptor(b, e)
	})
	return true
}

// undroppedElements returns the set of elements for a descriptor which need
// to be part of the target state of a DROP statement.
func undroppedElements(b BuildCtx, id catid.DescID) ElementResultSet {
	return b.QueryByID(id).Filter(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) bool {
		switch target {
		case scpb.InvalidTarget:
			// Validate that the descriptor-element is droppable, or already dropped.
			// This is the case when its current status is either PUBLIC or ABSENT,
			// which in the descriptor model correspond to it being in the PUBLIC state
			// or not being present at all.
			//
			// Objects undergoing an import or a backup restore will on the other hand
			// be have their descriptor states set to OFFLINE. When these descriptors
			// are decomposed to elements, these are then given scpb.InvalidTarget
			// target states by the decomposition logic.
			switch e.(type) {
			case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.Sequence, *scpb.View, *scpb.EnumType, *scpb.AliasType:
				panic(errors.Wrapf(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"object state is %s instead of PUBLIC, cannot be targeted by DROP", current),
					"%s", errMsgPrefix(b, id)))
			}
			// Ignore any other elements with undefined targets.
			return false
		case scpb.ToAbsent, scpb.Transient:
			// If the target is already ABSENT or TRANSIENT then the element is going
			// away anyway and so it doesn't need to have a target set for this DROP.
			return false
		}
		// Otherwise, return true to signal the removal of the element.
		return true
	})
}

// errMsgPrefix returns a human-readable prefix to scope error messages
// by the parent object's name and type. If the name can't be inferred we fall
// back on the descriptor ID.
func errMsgPrefix(b BuildCtx, id catid.DescID) string {
	typ := "descriptor"
	var name string
	b.QueryByID(id).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Database:
			typ = "database"
		case *scpb.Schema:
			typ = "schema"
		case *scpb.Table:
			typ = "table"
		case *scpb.Sequence:
			typ = "sequence"
		case *scpb.View:
			typ = "view"
		case *scpb.EnumType, *scpb.AliasType:
			typ = "type"
		case *scpb.Namespace:
			// Set the name either from the first encountered Namespace element, or
			// if there are several (in case of a rename) from the one with the old
			// name.
			if name == "" || target == scpb.ToAbsent {
				name = t.Name
			}
		}
	})
	if name == "" {
		return fmt.Sprintf("%s #%d", typ, id)
	}
	return fmt.Sprintf("%s %q", typ, name)
}

// dropElementWhenDroppingDescriptor is a helper to drop an element when
// dropping a descriptor which sets the bit to indicate that the descriptor
// is being dropped.
//
// TODO(postamar): remove this dirty hack ASAP, see column/index dep rules.
func dropElementWhenDroppingDescriptor(b BuildCtx, e scpb.Element) {
	switch t := e.(type) {
	case *scpb.ColumnType:
		t.IsRelationBeingDropped = true
	case *scpb.SecondaryIndexPartial:
		t.IsRelationBeingDropped = true
	}
	b.Drop(e)
}

// dropCascadeDescriptor contains the common logic for dropping something with
// CASCADE.
func dropCascadeDescriptor(b BuildCtx, id catid.DescID) {
	undropped := undroppedElements(b, id)
	// Exit early if all elements already have ABSENT targets.
	if undropped.IsEmpty() {
		return
	}
	// Check privileges and decide which actions to take or not.
	var isVirtualSchema bool
	undropped.ForEachElementStatus(func(current scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Database:
			break
		case *scpb.Schema:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary schema"))
			}
			isVirtualSchema = t.IsVirtual
			// Return early to skip checking privileges on schemas.
			return
		case *scpb.Table:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary table"))
			}
		case *scpb.Sequence:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary sequence"))
			}
		case *scpb.View:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary view"))
			}
		case *scpb.EnumType, *scpb.AliasType:
			break
		default:
			return
		}
		b.CheckPrivilege(e, privilege.DROP)
	})
	// Mark element targets as ABSENT.
	next := b.WithNewSourceElementID()
	undropped.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if isVirtualSchema {
			// Don't actually drop any elements of virtual schemas.
			return
		}
		dropElementWhenDroppingDescriptor(b, e)
		switch t := e.(type) {
		case *scpb.EnumType:
			dropCascadeDescriptor(next, t.ArrayTypeID)
		case *scpb.SequenceOwner:
			dropCascadeDescriptor(next, t.SequenceID)
		}
	})
	// Recurse on back-referenced elements.
	ub := undroppedBackrefs(b, id)
	ub.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.SchemaParent:
			dropCascadeDescriptor(next, t.SchemaID)
		case *scpb.ObjectParent:
			dropCascadeDescriptor(next, t.ObjectID)
		case *scpb.View:
			dropCascadeDescriptor(next, t.ViewID)
		case *scpb.Sequence:
			dropCascadeDescriptor(next, t.SequenceID)
		case *scpb.AliasType:
			dropCascadeDescriptor(next, t.TypeID)
		case *scpb.EnumType:
			dropCascadeDescriptor(next, t.TypeID)
		case *scpb.Column, *scpb.ColumnType, *scpb.SecondaryIndexPartial:
			// These only have type references.
			break
		case
			*scpb.ColumnDefaultExpression,
			*scpb.ColumnOnUpdateExpression,
			*scpb.CheckConstraint,
			*scpb.ForeignKeyConstraint,
			*scpb.SequenceOwner,
			*scpb.DatabaseRegionConfig:
			dropElementWhenDroppingDescriptor(b, e)
		}
	})
}

func undroppedBackrefs(b BuildCtx, id catid.DescID) ElementResultSet {
	return b.BackReferences(id).Filter(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) bool {
		return target != scpb.ToAbsent && screl.ContainsDescID(e, id)
	})
}

func descIDs(input ElementResultSet) (ids catalog.DescriptorIDSet) {
	input.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		ids.Add(screl.GetDescID(e))
	})
	return ids
}

func columnElements(b BuildCtx, relationID catid.DescID, columnID catid.ColumnID) ElementResultSet {
	return b.QueryByID(relationID).Filter(func(
		current scpb.Status, target scpb.TargetStatus, e scpb.Element,
	) bool {
		idI, _ := screl.Schema.GetAttribute(screl.ColumnID, e)
		return idI != nil && idI.(catid.ColumnID) == columnID
	})
}

func constraintElements(
	b BuildCtx, relationID catid.DescID, constraintID catid.ConstraintID,
) ElementResultSet {
	return b.QueryByID(relationID).Filter(func(
		current scpb.Status, target scpb.TargetStatus, e scpb.Element,
	) bool {
		idI, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
		return idI != nil && idI.(catid.ConstraintID) == constraintID
	})
}

// indexColumnIDs return an index's key column IDs, key suffix column IDs,
// and storing column IDs, in sorted order.
func getSortedColumnIDsInIndex(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (
	keyColumnIDs []catid.ColumnID,
	keySuffixColumnIDs []catid.ColumnID,
	storingColumnIDs []catid.ColumnID,
) {
	// Retrieve all columns of this index
	allColumns := make([]*scpb.IndexColumn, 0)
	scpb.ForEachIndexColumn(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, ice *scpb.IndexColumn,
	) {
		if ice.TableID != tableID || ice.IndexID != indexID {
			return
		}
		allColumns = append(allColumns, ice)
	})

	// Sort all columns by their (Kind, OrdinalInKind).
	sort.Slice(allColumns, func(i, j int) bool {
		return (allColumns[i].Kind < allColumns[j].Kind) ||
			(allColumns[i].Kind == allColumns[j].Kind && allColumns[i].OrdinalInKind < allColumns[j].OrdinalInKind)
	})

	// Populate results.
	keyColumnIDs = make([]catid.ColumnID, 0)
	keySuffixColumnIDs = make([]catid.ColumnID, 0)
	storingColumnIDs = make([]catid.ColumnID, 0)
	for _, ice := range allColumns {
		switch ice.Kind {
		case scpb.IndexColumn_KEY:
			keyColumnIDs = append(keyColumnIDs, ice.ColumnID)
		case scpb.IndexColumn_KEY_SUFFIX:
			keySuffixColumnIDs = append(keySuffixColumnIDs, ice.ColumnID)
		case scpb.IndexColumn_STORED:
			storingColumnIDs = append(storingColumnIDs, ice.ColumnID)
		default:
			panic(fmt.Sprintf("Unknown index column element kind %v", ice.Kind))
		}
	}
	return keyColumnIDs, keySuffixColumnIDs, storingColumnIDs
}

func toPublicNotCurrentlyPublicFilter(
	status scpb.Status, target scpb.TargetStatus, _ scpb.Element,
) bool {
	return status != scpb.Status_PUBLIC && target == scpb.ToPublic
}

func isColumnFilter(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
	_, isColumn := e.(*scpb.Column)
	return isColumn
}

func publicTargetFilter(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
	return target == scpb.ToPublic
}

func absentTargetFilter(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
	return target == scpb.ToAbsent
}

func notAbsentTargetFilter(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
	return target != scpb.ToAbsent
}

func statusAbsentOrBackfillOnlyFilter(
	status scpb.Status, _ scpb.TargetStatus, _ scpb.Element,
) bool {
	return status == scpb.Status_ABSENT || status == scpb.Status_BACKFILL_ONLY
}

func statusPublicFilter(status scpb.Status, _ scpb.TargetStatus, _ scpb.Element) bool {
	return status == scpb.Status_PUBLIC
}

func hasIndexIDAttrFilter(
	indexID catid.IndexID,
) func(_ scpb.Status, _ scpb.TargetStatus, _ scpb.Element) bool {
	return func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) (included bool) {
		idI, _ := screl.Schema.GetAttribute(screl.IndexID, e)
		return idI != nil && idI.(catid.IndexID) == indexID
	}
}

func hasColumnIDAttrFilter(
	columnID catid.ColumnID,
) func(_ scpb.Status, _ scpb.TargetStatus, _ scpb.Element) bool {
	return func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) (included bool) {
		idI, _ := screl.Schema.GetAttribute(screl.ColumnID, e)
		return idI != nil && idI.(catid.ColumnID) == columnID
	}
}

func referencesColumnIDFilter(
	columnID catid.ColumnID,
) func(_ scpb.Status, _ scpb.TargetStatus, _ scpb.Element) bool {
	return func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) (included bool) {
		_ = screl.WalkColumnIDs(e, func(id *catid.ColumnID) error {
			if id != nil && *id == columnID {
				included = true
			}
			return nil
		})
		return included
	}
}

// getPrimaryIndexes returns the primary indexes of the current table.
// Note that it assumes that there are at most two primary indexes and at
// least one. The existing primary index is the primary index which is
// currently public. The freshlyAdded primary index is one which is targeting
// public.
//
// TODO(ajwerner): This will not be true at some point in the near future when
// we need an intermediate primary index to support adding and dropping columns
// in the same transaction.
func getPrimaryIndexes(
	b BuildCtx, tableID catid.DescID,
) (existing, freshlyAdded *scpb.PrimaryIndex) {
	allTargets := b.QueryByID(tableID)
	_, _, freshlyAdded = scpb.FindPrimaryIndex(allTargets.
		Filter(publicTargetFilter).
		Filter(statusAbsentOrBackfillOnlyFilter))
	_, _, existing = scpb.FindPrimaryIndex(allTargets.Filter(statusPublicFilter))
	if existing == nil {
		// TODO(postamar): can this even be possible?
		panic(pgerror.Newf(pgcode.NoPrimaryKey, "missing active primary key"))
	}
	return existing, freshlyAdded
}

// indexColumnDirection converts tree.Direction to catpb.IndexColumn_Direction.
func indexColumnDirection(d tree.Direction) catpb.IndexColumn_Direction {
	switch d {
	case tree.DefaultDirection, tree.Ascending:
		return catpb.IndexColumn_ASC
	case tree.Descending:
		return catpb.IndexColumn_DESC
	default:
		panic(errors.AssertionFailedf("unknown direction %s", d))
	}
}

// indexSpec holds an index element and its children.
type indexSpec struct {
	primary   *scpb.PrimaryIndex
	secondary *scpb.SecondaryIndex
	temporary *scpb.TemporaryIndex

	name          *scpb.IndexName
	partial       *scpb.SecondaryIndexPartial
	partitioning  *scpb.IndexPartitioning
	columns       []*scpb.IndexColumn
	idxComment    *scpb.IndexComment
	constrComment *scpb.ConstraintComment
}

// apply makes it possible to conveniently define build targets for all
// the elements in the indexSpec.
func (s indexSpec) apply(fn func(e scpb.Element)) {
	if s.primary != nil {
		fn(s.primary)
	}
	if s.secondary != nil {
		fn(s.secondary)
	}
	if s.temporary != nil {
		fn(s.temporary)
	}
	if s.name != nil {
		fn(s.name)
	}
	if s.partial != nil {
		fn(s.partial)
	}
	if s.partitioning != nil {
		fn(s.partitioning)
	}
	for _, ic := range s.columns {
		fn(ic)
	}
	if s.idxComment != nil {
		fn(s.idxComment)
	}
	if s.constrComment != nil {
		fn(s.constrComment)
	}
}

// clone conveniently deep-copies all the elements in the indexSpec.
func (s indexSpec) clone() (c indexSpec) {
	if s.primary != nil {
		c.primary = protoutil.Clone(s.primary).(*scpb.PrimaryIndex)
	}
	if s.secondary != nil {
		c.secondary = protoutil.Clone(s.secondary).(*scpb.SecondaryIndex)
	}
	if s.temporary != nil {
		c.temporary = protoutil.Clone(s.temporary).(*scpb.TemporaryIndex)
	}
	if s.name != nil {
		c.name = protoutil.Clone(s.name).(*scpb.IndexName)
	}
	if s.partial != nil {
		c.partial = protoutil.Clone(s.partial).(*scpb.SecondaryIndexPartial)
	}
	if s.partitioning != nil {
		c.partitioning = protoutil.Clone(s.partitioning).(*scpb.IndexPartitioning)
	}
	for _, ic := range s.columns {
		c.columns = append(c.columns, protoutil.Clone(ic).(*scpb.IndexColumn))
	}
	if s.idxComment != nil {
		c.idxComment = protoutil.Clone(s.idxComment).(*scpb.IndexComment)
	}
	if s.constrComment != nil {
		c.constrComment = protoutil.Clone(s.constrComment).(*scpb.ConstraintComment)
	}
	return c
}

// makeIndexSpec constructs an indexSpec based on an existing index element.
func makeIndexSpec(b BuildCtx, tableID catid.DescID, indexID catid.IndexID) (s indexSpec) {
	tableElts := b.QueryByID(tableID).Filter(notAbsentTargetFilter)
	idxElts := tableElts.Filter(hasIndexIDAttrFilter(indexID))
	var constraintID catid.ConstraintID
	var n int
	_, _, s.primary = scpb.FindPrimaryIndex(idxElts)
	if s.primary != nil {
		constraintID = s.primary.ConstraintID
		n++
	}
	_, _, s.secondary = scpb.FindSecondaryIndex(idxElts)
	if s.secondary != nil {
		constraintID = s.secondary.ConstraintID
		n++
	}
	_, _, s.temporary = scpb.FindTemporaryIndex(idxElts)
	if s.temporary != nil {
		constraintID = s.temporary.ConstraintID
		n++
	}
	if n != 1 {
		panic(errors.AssertionFailedf("invalid index spec for TableID=%d and IndexID=%d: "+
			"primary=%v, secondary=%v, temporary=%v",
			tableID, indexID, s.primary != nil, s.secondary != nil, s.temporary != nil))
	}
	_, _, s.name = scpb.FindIndexName(idxElts)
	_, _, s.partial = scpb.FindSecondaryIndexPartial(idxElts)
	_, _, s.partitioning = scpb.FindIndexPartitioning(idxElts)
	scpb.ForEachIndexColumn(idxElts, func(_ scpb.Status, _ scpb.TargetStatus, ic *scpb.IndexColumn) {
		s.columns = append(s.columns, ic)
	})
	_, _, s.idxComment = scpb.FindIndexComment(idxElts)
	scpb.ForEachConstraintComment(tableElts, func(_ scpb.Status, _ scpb.TargetStatus, cc *scpb.ConstraintComment) {
		if cc.ConstraintID == constraintID {
			s.constrComment = cc
		}
	})
	return s
}

// indexColumnSpec specifies how to construct a scpb.IndexColumn element.
type indexColumnSpec struct {
	columnID  catid.ColumnID
	kind      scpb.IndexColumn_Kind
	direction catpb.IndexColumn_Direction
}

func makeIndexColumnSpec(ic *scpb.IndexColumn) indexColumnSpec {
	return indexColumnSpec{
		columnID:  ic.ColumnID,
		kind:      ic.Kind,
		direction: ic.Direction,
	}
}

// makeSwapIndexSpec constructs a pair of indexSpec for the new index and the
// accompanying temporary index to swap out an existing index with.
func makeSwapIndexSpec(
	b BuildCtx, out indexSpec, sourceIndexID catid.IndexID, inColumns []indexColumnSpec,
) (in, temp indexSpec) {
	isSecondary := out.secondary != nil
	// Determine table ID and validate input.
	var tableID catid.DescID
	{
		var n int
		var outID catid.IndexID
		if isSecondary {
			tableID = out.secondary.TableID
			outID = out.secondary.IndexID
			n++
		}
		if out.primary != nil {
			tableID = out.primary.TableID
			outID = out.primary.IndexID
			n++
		}
		if out.temporary != nil {
			tableID = out.primary.TableID
			outID = out.primary.IndexID
		}
		if n != 1 {
			panic(errors.AssertionFailedf("invalid swap source index spec for TableID=%d and IndexID=%d: "+
				"primary=%v, secondary=%v, temporary=%v",
				tableID, outID, out.primary != nil, isSecondary, out.temporary != nil))
		}
	}
	// Determine old and new IDs.
	var inID, tempID catid.IndexID
	var inConstraintID, tempConstraintID catid.ConstraintID
	{
		_, _, tbl := scpb.FindTable(b.QueryByID(tableID).Filter(notAbsentTargetFilter))
		inID = b.NextTableIndexID(tbl)
		inConstraintID = b.NextTableConstraintID(tbl.TableID)
		tempID = inID + 1
		tempConstraintID = inConstraintID + 1
	}
	// Setup new primary or secondary index.
	{
		in = out.clone()
		var idx *scpb.Index
		if isSecondary {
			idx = &in.secondary.Index
		} else {
			idx = &in.primary.Index
		}
		idx.IndexID = inID
		idx.SourceIndexID = sourceIndexID
		idx.TemporaryIndexID = tempID
		idx.ConstraintID = inConstraintID
		if in.name != nil {
			in.name.IndexID = inID
		}
		if in.partial != nil {
			in.partial.IndexID = inID
		}
		if in.partitioning != nil {
			in.partitioning.IndexID = inID
		}
		m := make(map[scpb.IndexColumn_Kind]uint32)
		in.columns = in.columns[:0]
		for _, cs := range inColumns {
			ordinalInKind := m[cs.kind]
			m[cs.kind] = ordinalInKind + 1
			in.columns = append(in.columns, &scpb.IndexColumn{
				TableID:       idx.TableID,
				IndexID:       inID,
				ColumnID:      cs.columnID,
				OrdinalInKind: ordinalInKind,
				Kind:          cs.kind,
				Direction:     cs.direction,
			})
		}
		if in.idxComment != nil {
			in.idxComment.IndexID = inID
		}
		if in.constrComment != nil {
			in.constrComment.ConstraintID = inConstraintID
		}
	}
	// Setup temporary index.
	{
		s := in.clone()
		if isSecondary {
			temp.temporary = &scpb.TemporaryIndex{Index: s.secondary.Index}
		} else {
			temp.temporary = &scpb.TemporaryIndex{Index: s.primary.Index}
		}
		temp.temporary.IndexID = tempID
		temp.temporary.TemporaryIndexID = 0
		temp.temporary.ConstraintID = tempConstraintID
		temp.temporary.IsUsingSecondaryEncoding = isSecondary
		if s.partial != nil {
			temp.partial = s.partial
			temp.partial.IndexID = tempID
		}
		if s.partitioning != nil {
			temp.partitioning = s.partitioning
			temp.partitioning.IndexID = tempID
		}
		for _, ic := range s.columns {
			ic.IndexID = tempID
		}
		temp.columns = s.columns
	}
	return in, temp
}

// fallBackIfZoneConfigExists determines if the table has regional by row
// properties and throws an unimplemented error.
func fallBackIfZoneConfigExists(b BuildCtx, n tree.NodeFormatter, id catid.DescID) {
	{
		tableElts := b.QueryByID(id)
		if _, _, elem := scpb.FindTableZoneConfig(tableElts); elem != nil {
			panic(scerrors.NotImplementedErrorf(n,
				"regional by row partitioning is not supported"))
		}
	}
}

// fallBackIfVirtualColumnWithNotNullConstraint throws an unimplemented error
// if the to-be-added column `d` is a virtual column with not null constraint.
// This is a quick, temporary fix for the following troubled stmt in the
// declarative schema changer:
// `ALTER TABLE t ADD COLUMN j INT AS (NULL::INT) VIRTUAL NOT NULL;` succeeded
// but expectedly failed in the legacy schema changer.
func fallBackIfVirtualColumnWithNotNullConstraint(t *tree.AlterTableAddColumn) {
	d := t.ColumnDef
	if d.IsVirtual() && d.Nullable.Nullability == tree.NotNull {
		panic(scerrors.NotImplementedErrorf(t,
			"virtual column with NOT NULL constraint is not supported"))
	}
}

// panicIfSystemColumn blocks alter operations on system columns.
func panicIfSystemColumn(column *scpb.Column, columnName string) {
	if column.IsSystemColumn {
		// Block alter operations on system columns.
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot alter system column %q", columnName))
	}
}
