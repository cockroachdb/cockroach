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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	if ns == nil {
		// Function descriptors don't have namespace. So we need to handle this
		// special case here.
		return qualifiedFunctionName(b, id)
	}
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

func qualifiedFunctionName(b BuildCtx, id catid.DescID) string {
	elts := b.QueryByID(id)
	_, _, fnName := scpb.FindFunctionName(elts)
	_, _, objParent := scpb.FindObjectParent(elts)
	_, _, scName := scpb.FindNamespace(b.QueryByID(objParent.ParentSchemaID))
	_, _, scParent := scpb.FindSchemaParent(b.QueryByID(objParent.ParentSchemaID))
	_, _, dbName := scpb.FindNamespace(b.QueryByID(scParent.ParentDatabaseID))
	return dbName.Name + "." + scName.Name + "." + fnName.Name
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
		b.Drop(e)
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
			case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.Sequence, *scpb.View, *scpb.EnumType, *scpb.AliasType,
				*scpb.CompositeType:
				panic(errors.Wrapf(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"object state is %s instead of PUBLIC, cannot be targeted by DROP", current),
					"%s", errMsgPrefix(b, id)))
			}
			// Ignore any other elements with undefined targets.
			return false
		case scpb.ToAbsent:
			// If the target is already ABSENT then the element is going away anyway
			// so it doesn't need to have a target set for this DROP.
			return false
		}
		// Otherwise, return true to signal the removal of the element.
		// TRANSIENT targets also need to be considered here, as we do not want
		// them to come into existence at all, even if they are to go away
		// eventually.
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
		case *scpb.EnumType, *scpb.AliasType, *scpb.CompositeType:
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

// dropCascadeDescriptor contains the common logic for dropping something with
// CASCADE.
func dropCascadeDescriptor(b BuildCtx, n tree.NodeFormatter, id catid.DescID) {
	var s dropCascadeState
	// First, we recursively visit the descriptors to which the drop cascades.
	s.visitRecursive(b, id)
	// Do all known whole-descriptor drops first.
	// This way, we straightforwardly set a large number of to-ABSENT targets.
	for _, qe := range s.q {
		qe.dropWholeDescriptor()
	}
	// Afterwards, we can deal with any remaining column, index, constraint or
	// other element removals when their parent descriptor-element isn't dropped.
	for _, qe := range s.q {
		qe.dropUndroppedBackReferencedElements(n)
	}
}

type dropCascadeState struct {
	ids catalog.DescriptorIDSet
	q   []dropCascadeQueueElement
}

func (s *dropCascadeState) visitRecursive(b BuildCtx, id catid.DescID) {
	if s.ids.Contains(id) {
		return
	}
	s.ids.Add(id)
	undropped := undroppedElements(b, id)
	if undropped.IsEmpty() {
		// Exit early if all elements already have ABSENT targets.
		return
	}
	s.q = append(s.q, dropCascadeQueueElement{b: b, id: id})
	qe := &s.q[len(s.q)-1]
	// Check privileges and decide which actions to take or not.
	undropped.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Database:
			break
		case *scpb.Schema:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary schema"))
			}
			qe.isVirtualSchema = t.IsVirtual
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
		case *scpb.EnumType, *scpb.AliasType, *scpb.CompositeType:
			break
		default:
			return
		}
		b.CheckPrivilege(e, privilege.DROP)
	})
	next := b.WithNewSourceElementID()
	undropped.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.EnumType:
			s.visitRecursive(next, t.ArrayTypeID)
		case *scpb.CompositeType:
			s.visitRecursive(next, t.ArrayTypeID)
		case *scpb.SequenceOwner:
			s.visitRecursive(next, t.SequenceID)
		}
	})
	// Recurse on back-referenced descriptor elements.
	ub := undroppedBackrefs(b, id)
	ub.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.SchemaParent:
			s.visitRecursive(next, t.SchemaID)
		case *scpb.ObjectParent:
			s.visitRecursive(next, t.ObjectID)
		case *scpb.View:
			s.visitRecursive(next, t.ViewID)
		case *scpb.Sequence:
			s.visitRecursive(next, t.SequenceID)
		case *scpb.AliasType:
			s.visitRecursive(next, t.TypeID)
		case *scpb.EnumType:
			s.visitRecursive(next, t.TypeID)
		case *scpb.CompositeType:
			s.visitRecursive(next, t.TypeID)
		case *scpb.FunctionBody:
			s.visitRecursive(next, t.FunctionID)
		}
	})
}

type dropCascadeQueueElement struct {
	b               BuildCtx
	id              catid.DescID
	isVirtualSchema bool
}

func (qe dropCascadeQueueElement) dropWholeDescriptor() {
	if qe.isVirtualSchema {
		return
	}
	undroppedElements(qe.b, qe.id).ForEachElementStatus(
		func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			qe.b.Drop(e)
		},
	)
}

func (qe dropCascadeQueueElement) dropUndroppedBackReferencedElements(n tree.NodeFormatter) {
	ub := undroppedBackrefs(qe.b, qe.id)
	ub.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Column:
			dropColumnByID(qe.b, n, t.TableID, t.ColumnID)
		case *scpb.ColumnType:
			dropColumnByID(qe.b, n, t.TableID, t.ColumnID)
		case *scpb.SecondaryIndexPartial:
			scpb.ForEachSecondaryIndex(
				qe.b.QueryByID(t.TableID).Filter(publicTargetFilter).Filter(hasIndexIDAttrFilter(t.IndexID)),
				func(_ scpb.Status, _ scpb.TargetStatus, sie *scpb.SecondaryIndex) {
					dropSecondaryIndex(qe.b, n, tree.DropCascade, sie)
				},
			)
		case *scpb.CheckConstraint:
			dropConstraintByID(qe.b, t.TableID, t.ConstraintID)
		case *scpb.ForeignKeyConstraint:
			dropConstraintByID(qe.b, t.TableID, t.ConstraintID)
		case *scpb.UniqueWithoutIndexConstraint:
			dropConstraintByID(qe.b, t.TableID, t.ConstraintID)
		case
			*scpb.ColumnDefaultExpression,
			*scpb.ColumnOnUpdateExpression,
			*scpb.SequenceOwner,
			*scpb.DatabaseRegionConfig:
			qe.b.Drop(e)
		}
	})
}

func dropColumnByID(
	b BuildCtx, n tree.NodeFormatter, relationID catid.DescID, columnID catid.ColumnID,
) {
	relationElts := b.QueryByID(relationID).Filter(publicTargetFilter)
	colElts := relationElts.Filter(hasColumnIDAttrFilter(columnID))
	_, _, col := scpb.FindColumn(colElts)
	_, _, tbl := scpb.FindTable(relationElts)
	if tbl == nil {
		return
	}
	dropColumn(b, n, tbl, col, colElts, tree.DropCascade)
}

func dropConstraintByID(b BuildCtx, tableID catid.DescID, constraintID catid.ConstraintID) {
	tableElts := b.QueryByID(tableID).Filter(publicTargetFilter)
	constraintElts := tableElts.Filter(hasConstraintIDAttrFilter(constraintID))
	constraintElts.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		b.Drop(e)
	})
}

func undroppedBackrefs(b BuildCtx, id catid.DescID) ElementResultSet {
	return b.BackReferences(id).Filter(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) bool {
		return target == scpb.ToPublic && screl.ContainsDescID(e, id)
	})
}

func descIDs(input ElementResultSet) (ids catalog.DescriptorIDSet) {
	input.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		ids.Add(screl.GetDescID(e))
	})
	return ids
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

// getNonDropColumnIDs returns all non-drop columns in a table.
func getNonDropColumns(b BuildCtx, tableID catid.DescID) (ret []*scpb.Column) {
	scpb.ForEachColumn(b.QueryByID(tableID).Filter(publicTargetFilter), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.Column,
	) {
		ret = append(ret, e)
	})
	return ret
}

// getColumnIDFromColumnName looks up a column's ID by its name.
// If no column with this name exists, 0 will be returned.
func getColumnIDFromColumnName(
	b BuilderState, tableID catid.DescID, columnName tree.Name,
) catid.ColumnID {
	colElems := b.ResolveColumn(tableID, columnName, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.CREATE,
	})

	if colElems == nil {
		// no column with this name was found
		return 0
	}

	_, _, colElem := scpb.FindColumn(colElems)
	if colElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a Column element for column %v", columnName))
	}
	return colElem.ColumnID
}

// mustGetColumnIDFromColumnName looks up a column's ID by its name.
// If no column with this name exists, panic.
func mustGetColumnIDFromColumnName(
	b BuildCtx, tableID catid.DescID, columnName tree.Name,
) catid.ColumnID {
	colID := getColumnIDFromColumnName(b, tableID, columnName)
	if colID == 0 {
		panic(errors.AssertionFailedf("cannot find column with name %v", columnName))
	}
	return colID
}

func mustGetTableIDFromTableName(b BuildCtx, tableName tree.TableName) catid.DescID {
	tableElems := b.ResolveTable(tableName.ToUnresolvedObjectName(), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	_, _, tableElem := scpb.FindTable(tableElems)
	if tableElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a Table element for table %v", tableName))
	}
	return tableElem.TableID
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

func hasConstraintIDAttrFilter(
	constraintID catid.ConstraintID,
) func(_ scpb.Status, _ scpb.TargetStatus, _ scpb.Element) bool {
	return func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) (included bool) {
		idI, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
		return idI != nil && idI.(catid.ConstraintID) == constraintID
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

// indexColumnDirection converts tree.Direction to catenumpb.IndexColumn_Direction.
func indexColumnDirection(d tree.Direction) catenumpb.IndexColumn_Direction {
	switch d {
	case tree.DefaultDirection, tree.Ascending:
		return catenumpb.IndexColumn_ASC
	case tree.Descending:
		return catenumpb.IndexColumn_DESC
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
	data          *scpb.IndexData
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
	if s.data != nil {
		fn(s.data)
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
	if s.data != nil {
		c.data = protoutil.Clone(s.data).(*scpb.IndexData)
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
	_, _, s.data = scpb.FindIndexData(idxElts)
	return s
}

// makeTempIndexSpec clones the primary/secondary index spec into one for a
// temporary index, based on the populated information.
func makeTempIndexSpec(src indexSpec) indexSpec {
	if src.secondary == nil && src.primary == nil {
		panic(errors.AssertionFailedf("make temp index converts a primary/secondary index into a temporary one"))
	}
	newTempSpec := src.clone()
	var srcIdx scpb.Index
	isSecondary := false
	if src.primary != nil {
		srcIdx = newTempSpec.primary.Index
	}
	if src.secondary != nil {
		srcIdx = newTempSpec.secondary.Index
		isSecondary = true
	}
	tempID := srcIdx.TemporaryIndexID
	newTempSpec.temporary = &scpb.TemporaryIndex{
		Index:                    srcIdx,
		IsUsingSecondaryEncoding: isSecondary,
	}
	newTempSpec.temporary.TemporaryIndexID = 0
	newTempSpec.temporary.IndexID = tempID
	newTempSpec.temporary.ConstraintID = srcIdx.ConstraintID + 1
	newTempSpec.secondary = nil
	newTempSpec.primary = nil

	// Replace all the index IDs in the clone.
	if newTempSpec.data != nil {
		newTempSpec.data.IndexID = tempID
	}
	if newTempSpec.partitioning != nil {
		newTempSpec.partitioning.IndexID = tempID
	}
	if newTempSpec.partial != nil {
		newTempSpec.partial.IndexID = tempID
	}
	for _, ic := range newTempSpec.columns {
		ic.IndexID = tempID
	}
	// Clear fields that temporary indexes should not have.
	newTempSpec.name = nil
	newTempSpec.constrComment = nil
	newTempSpec.idxComment = nil
	newTempSpec.name = nil
	newTempSpec.secondary = nil

	return newTempSpec
}

// indexColumnSpec specifies how to construct a scpb.IndexColumn element.
type indexColumnSpec struct {
	columnID  catid.ColumnID
	kind      scpb.IndexColumn_Kind
	direction catenumpb.IndexColumn_Direction
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
	var inConstraintID catid.ConstraintID
	{
		_, _, tbl := scpb.FindTable(b.QueryByID(tableID).Filter(notAbsentTargetFilter))
		inID = b.NextTableIndexID(tbl)
		inConstraintID = b.NextTableConstraintID(tbl.TableID)
		tempID = inID + 1
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
		if in.data != nil {
			in.data.IndexID = inID
		}
	}
	// Setup temporary index.
	{
		temp = makeTempIndexSpec(in)
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

// ExtractColumnIDsInExpr extracts column IDs used in expr. It's similar to
// schemaexpr.ExtractColumnIDs but this function can also extract columns
// added in the same transaction (e.g. for `ADD COLUMN j INT CHECK (j > 0);`,
// schemaexpr.ExtractColumnIDs will err with "column j does not exist", but
// this function can successfully retrieve the ID of column j from the builder state).
func ExtractColumnIDsInExpr(
	b BuilderState, tableID catid.DescID, expr tree.Expr,
) (catalog.TableColSet, error) {
	var colIDs catalog.TableColSet

	_, err := tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		colID := getColumnIDFromColumnName(b, tableID, c.ColumnName)
		colIDs.Add(colID)
		return false, expr, nil
	})

	return colIDs, err
}

func isColNotNull(b BuildCtx, tableID catid.DescID, columnID catid.ColumnID) (ret bool) {
	// A column is NOT NULL iff there is a ColumnNotNull element on this columnID
	scpb.ForEachColumnNotNull(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnNotNull,
	) {
		if e.ColumnID == columnID {
			ret = true
		}
	})
	return ret
}

func maybeFailOnCrossDBTypeReference(b BuildCtx, typeID descpb.ID, parentDBID descpb.ID) {
	_, _, typeNamespace := scpb.FindNamespace(b.QueryByID(typeID))
	if typeNamespace.DatabaseID != parentDBID {
		typeName := tree.MakeTypeNameWithPrefix(b.NamePrefix(typeNamespace), typeNamespace.Name)
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cross database type references are not supported: %s",
			typeName.String()))
	}
}
