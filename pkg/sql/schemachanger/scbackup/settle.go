// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbackup

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// RESTORE ingests data only for a table's primary index and public secondary
// indexes, but restores the declarative schema changer state as of the backup.
// When that state claims a non-public index holds data, resuming the job would
// promote or revive an empty index. Such a job is settled instead; see settle.

// restoredIndexIDs returns the IDs of the indexes whose data RESTORE ingests
// for desc, or nil if desc is not a table.
func restoredIndexIDs(desc catalog.Descriptor) map[catid.IndexID]bool {
	tbl, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return nil
	}
	ids := make(map[catid.IndexID]bool)
	for _, idx := range tbl.ActiveIndexes() {
		ids[idx.GetID()] = true
	}
	return ids
}

// dependsOnUnrestoredIndexData reports whether ds relies on data in an index
// that is not in restored: an index being added that is already backfilled,
// or a demoted index that a rollback would revive.
func dependsOnUnrestoredIndexData(ds *scpb.DescriptorState, restored map[catid.IndexID]bool) bool {
	for i := range ds.Targets {
		t := &ds.Targets[i]
		if !t.IsLinkedToSchemaChange() {
			continue
		}
		cur := ds.CurrentStatuses[i]
		var needsData bool
		switch t.TargetStatus {
		case scpb.Status_PUBLIC, scpb.Status_TRANSIENT_ABSENT:
			needsData = isBackfilledStatus(cur)
		case scpb.Status_ABSENT:
			needsData = cur != scpb.Status_ABSENT && ds.Revertible
		}
		if !needsData {
			continue
		}
		var id catid.IndexID
		switch e := t.Element().(type) {
		case *scpb.PrimaryIndex:
			id = e.IndexID
		case *scpb.SecondaryIndex:
			id = e.IndexID
		default:
			continue
		}
		if !restored[id] {
			return true
		}
	}
	return false
}

// isBackfilledStatus reports whether s is a status an index being added
// reaches only after its backfill.
func isBackfilledStatus(s scpb.Status) bool {
	switch s {
	case scpb.Status_BACKFILLED, scpb.Status_DELETE_ONLY, scpb.Status_MERGE_ONLY,
		scpb.Status_MERGED, scpb.Status_WRITE_ONLY, scpb.Status_VALIDATED:
		return true
	}
	return false
}

// settle rewrites ds so the job drives every settleable target (see
// isSettleable) to PUBLIC if it was PUBLIC at backup time, and to ABSENT
// otherwise. The exceptions keep the result consistent:
//   - a transient element, which the schema change adds and removes before it
//     finishes, is dropped even if PUBLIC, unless it is part of a primary
//     index, such as the intermediate primary index of ALTER PRIMARY KEY;
//   - the parts of a dropped index, column, or descriptor are dropped with it;
//   - a shadowed recreated index is dropped;
//   - a secondary index on or storing a column that is not kept is dropped;
//   - a key column of a kept primary index is kept, with its NOT NULL;
//   - an enum value being dropped is kept while its type is kept.
//
// The rules match parents to children by ColumnID and IndexID within one
// DescriptorState. A new element type whose child can be PUBLIC while its
// parent is not needs its own rule.
//
// The order of the rules matters: keepKeyColumnsOfKeptPrimaryIndexes runs
// before dropPartsOfDroppedColumns so the parts of a rescued column survive,
// and dropPartsOfDroppedDescriptor runs after both so a dropped descriptor
// drops everything, including rescued columns.
//
// The settled state is marked as a rollback in progress, so the job runs its
// remaining stages in the reverting state, starting in the non-revertible
// phase, and ends as failed. CurrentState.Rollback is a no-op on such a state,
// so neither a cancel nor a job error can reverse the settled targets.
func settle(ds *scpb.DescriptorState) {
	primaryIndexes := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if e, ok := ds.Targets[i].Element().(*scpb.PrimaryIndex); ok {
			primaryIndexes[e.IndexID] = true
		}
	}
	wasDropping := make([]bool, len(ds.Targets))
	for i := range ds.Targets {
		t := &ds.Targets[i]
		if !isSettleable(t) {
			continue
		}
		wasDropping[i] = t.TargetStatus == scpb.Status_ABSENT
		keep := ds.CurrentStatuses[i] == scpb.Status_PUBLIC
		if keep && t.TargetStatus == scpb.Status_TRANSIENT_ABSENT {
			id, ok := owningIndexID(t.Element())
			keep = ok && primaryIndexes[id]
		}
		if keep {
			t.TargetStatus = scpb.Status_PUBLIC
		} else {
			t.TargetStatus = scpb.Status_ABSENT
		}
	}
	dropPartsOfDroppedIndexes(ds)
	dropShadowedRecreates(ds)
	dropSecondaryIndexesOnDroppedColumns(ds)
	keepKeyColumnsOfKeptPrimaryIndexes(ds)
	dropPartsOfDroppedColumns(ds)
	dropPartsOfDroppedDescriptor(ds)
	keepDroppingValuesOfKeptEnums(ds, wasDropping)
	ds.Revertible = false
	ds.InRollback = true
}

// isSettleable reports whether settle may rewrite t's target status.
func isSettleable(t *scpb.Target) bool {
	return t.IsLinkedToSchemaChange() && t.TargetStatus != scpb.Status_TRANSIENT_PUBLIC
}

// keyColumnsOfKeptPrimaryIndexes returns the KEY and KEY_SUFFIX columns of the
// primary indexes of ds whose target is PUBLIC.
func keyColumnsOfKeptPrimaryIndexes(ds *scpb.DescriptorState) map[catid.ColumnID]bool {
	kept := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if e, ok := ds.Targets[i].Element().(*scpb.PrimaryIndex); ok &&
			ds.Targets[i].TargetStatus == scpb.Status_PUBLIC {
			kept[e.IndexID] = true
		}
	}
	cols := make(map[catid.ColumnID]bool)
	for i := range ds.Targets {
		if ic, ok := ds.Targets[i].Element().(*scpb.IndexColumn); ok &&
			ds.Targets[i].TargetStatus == scpb.Status_PUBLIC &&
			ic.Kind != scpb.IndexColumn_STORED && kept[ic.IndexID] {
			cols[ic.ColumnID] = true
		}
	}
	return cols
}

// dropSecondaryIndexesOnDroppedColumns drops, with all of its parts, a kept
// secondary index that has a column, of any kind, whose target is ABSENT and
// that no kept primary index needs, such as an index keyed on or storing the
// new column of an ALTER COLUMN TYPE that was not yet public at backup time.
// Dropping a column removes it from the primary index only, so a stored column
// counts too.
func dropSecondaryIndexesOnDroppedColumns(ds *scpb.DescriptorState) {
	pkCols := keyColumnsOfKeptPrimaryIndexes(ds)
	droppedCols := make(map[catid.ColumnID]bool)
	for i := range ds.Targets {
		if c, ok := ds.Targets[i].Element().(*scpb.Column); ok &&
			ds.Targets[i].TargetStatus == scpb.Status_ABSENT && !pkCols[c.ColumnID] {
			droppedCols[c.ColumnID] = true
		}
	}
	keptSecondary := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if e, ok := ds.Targets[i].Element().(*scpb.SecondaryIndex); ok &&
			ds.Targets[i].TargetStatus == scpb.Status_PUBLIC {
			keptSecondary[e.IndexID] = true
		}
	}
	drop := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if ic, ok := ds.Targets[i].Element().(*scpb.IndexColumn); ok &&
			keptSecondary[ic.IndexID] && droppedCols[ic.ColumnID] {
			drop[ic.IndexID] = true
		}
	}
	dropIndexesWithParts(ds, drop)
}

// keepKeyColumnsOfKeptPrimaryIndexes keeps each key column of a kept primary
// index, with its NOT NULL, even if either was not PUBLIC at backup time. For
// example, it keeps the shard column of a hash-sharded primary key whose
// intermediate primary index is already PUBLIC while the column is not, and the
// NOT NULL of the rowid column of a kept primary index that ALTER PRIMARY KEY
// was dropping.
func keepKeyColumnsOfKeptPrimaryIndexes(ds *scpb.DescriptorState) {
	needed := keyColumnsOfKeptPrimaryIndexes(ds)
	for i := range ds.Targets {
		t := &ds.Targets[i]
		if !isSettleable(t) {
			continue
		}
		switch e := t.Element().(type) {
		case *scpb.Column:
			if needed[e.ColumnID] {
				t.TargetStatus = scpb.Status_PUBLIC
			}
		case *scpb.ColumnNotNull:
			if needed[e.ColumnID] {
				t.TargetStatus = scpb.Status_PUBLIC
			}
		}
	}
}

// keepDroppingValuesOfKeptEnums keeps an enum value that was being dropped
// while its enum type is kept: such a value is read-only but still part of the
// type at backup time. wasDropping reports, per target, whether its target
// status was ABSENT before settling.
func keepDroppingValuesOfKeptEnums(ds *scpb.DescriptorState, wasDropping []bool) {
	keptEnum := false
	for i := range ds.Targets {
		if _, ok := ds.Targets[i].Element().(*scpb.EnumType); ok &&
			ds.Targets[i].TargetStatus == scpb.Status_PUBLIC {
			keptEnum = true
		}
	}
	if !keptEnum {
		return
	}
	for i := range ds.Targets {
		t := &ds.Targets[i]
		if _, ok := t.Element().(*scpb.EnumTypeValue); ok && isSettleable(t) && wasDropping[i] &&
			ds.CurrentStatuses[i] != scpb.Status_ABSENT {
			t.TargetStatus = scpb.Status_PUBLIC
		}
	}
}

// dropPartsOfDroppedColumns drops the elements that belong to a column whose
// target is ABSENT, such as its name, type, and default expression. They can
// be PUBLIC at backup time while the column itself is not, and keeping them
// would leave, for example, a sequence back-reference to a missing column. An
// IndexColumn follows its index rather than its column.
func dropPartsOfDroppedColumns(ds *scpb.DescriptorState) {
	dropped := make(map[catid.ColumnID]bool)
	for i := range ds.Targets {
		if c, ok := ds.Targets[i].Element().(*scpb.Column); ok &&
			ds.Targets[i].TargetStatus == scpb.Status_ABSENT {
			dropped[c.ColumnID] = true
		}
	}
	if len(dropped) == 0 {
		return
	}
	for i := range ds.Targets {
		t := &ds.Targets[i]
		if !isSettleable(t) {
			continue
		}
		switch t.Element().(type) {
		case *scpb.Column, *scpb.IndexColumn:
			continue
		}
		v, err := screl.Schema.GetAttribute(screl.ColumnID, t.Element())
		if err != nil || v == nil {
			continue
		}
		if id, ok := v.(catid.ColumnID); ok && dropped[id] {
			t.TargetStatus = scpb.Status_ABSENT
		}
	}
}

// dropPartsOfDroppedDescriptor drops every element of ds when its descriptor
// element is ABSENT, such as the namespace entry and schema parent of a schema
// created by the job that was not yet PUBLIC at backup time.
func dropPartsOfDroppedDescriptor(ds *scpb.DescriptorState) {
	dropped := false
	for i := range ds.Targets {
		if screl.IsDescriptor(ds.Targets[i].Element()) &&
			ds.Targets[i].TargetStatus == scpb.Status_ABSENT {
			dropped = true
			break
		}
	}
	if !dropped {
		return
	}
	for i := range ds.Targets {
		if t := &ds.Targets[i]; isSettleable(t) {
			t.TargetStatus = scpb.Status_ABSENT
		}
	}
}

// keptIndexIDs returns the primary and secondary indexes of ds whose target is
// PUBLIC.
func keptIndexIDs(ds *scpb.DescriptorState) map[catid.IndexID]bool {
	kept := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if ds.Targets[i].TargetStatus != scpb.Status_PUBLIC {
			continue
		}
		switch e := ds.Targets[i].Element().(type) {
		case *scpb.PrimaryIndex:
			kept[e.IndexID] = true
		case *scpb.SecondaryIndex:
			kept[e.IndexID] = true
		}
	}
	return kept
}

// dropShadowedRecreates drops each secondary index recreated for a primary key
// change, with all of its elements, when the index it recreates is also kept.
// Keeping both leaves a hidden recreate that the schema changer rejects.
func dropShadowedRecreates(ds *scpb.DescriptorState) {
	kept := keptIndexIDs(ds)
	drop := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if e, ok := ds.Targets[i].Element().(*scpb.SecondaryIndex); ok &&
			e.RecreateSourceIndexID != 0 && kept[e.IndexID] && kept[e.RecreateSourceIndexID] {
			drop[e.IndexID] = true
		}
	}
	dropIndexesWithParts(ds, drop)
}

// dropPartsOfDroppedIndexes drops the elements that belong to an index whose
// target is ABSENT, such as its columns, name, and data. They can be PUBLIC at
// backup time while the index itself is not, and the index's data is garbage
// collected only when its IndexData is dropped.
func dropPartsOfDroppedIndexes(ds *scpb.DescriptorState) {
	drop := make(map[catid.IndexID]bool)
	for i := range ds.Targets {
		if ds.Targets[i].TargetStatus != scpb.Status_ABSENT {
			continue
		}
		switch e := ds.Targets[i].Element().(type) {
		case *scpb.PrimaryIndex:
			drop[e.IndexID] = true
		case *scpb.SecondaryIndex:
			drop[e.IndexID] = true
		case *scpb.TemporaryIndex:
			drop[e.IndexID] = true
		}
	}
	dropIndexesWithParts(ds, drop)
}

// dropIndexesWithParts sets the target of every element of the indexes in drop
// to ABSENT.
func dropIndexesWithParts(ds *scpb.DescriptorState, drop map[catid.IndexID]bool) {
	if len(drop) == 0 {
		return
	}
	for i := range ds.Targets {
		t := &ds.Targets[i]
		if !isSettleable(t) {
			continue
		}
		if id, ok := owningIndexID(t.Element()); ok && drop[id] {
			t.TargetStatus = scpb.Status_ABSENT
		}
	}
}

// owningIndexID returns the index that e is, or is a part of, such as its name,
// columns, or data.
func owningIndexID(e scpb.Element) (catid.IndexID, bool) {
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint,
		*scpb.UniqueWithoutIndexConstraint, *scpb.ColumnNotNull:
		// Their IndexID attribute is the index they are validated against, not
		// the index they belong to.
		return 0, false
	}
	v, err := screl.Schema.GetAttribute(screl.IndexID, e)
	if err != nil {
		return 0, false
	}
	id, ok := v.(catid.IndexID)
	return id, ok && id != 0
}

// SettledJobDescriptionPrefix prefixes the description of a restored schema
// change job that was settled to the state at backup time.
const SettledJobDescriptionPrefix = "settled to state at backup time: "

// IsSettledJobDescription reports whether description is that of a restored
// schema change job that was settled to the state at backup time.
func IsSettledJobDescription(description string) bool {
	return strings.HasPrefix(description, SettledJobDescriptionPrefix)
}

// settleRestoredSchemaChanges controls whether RESTORE settles a declarative
// schema change that depends on index data the restore does not bring back.
var settleRestoredSchemaChanges = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.schemachanger.settle_restored_schema_changes.enabled",
	"when enabled, RESTORE stops an in-flight schema change at the state visible "+
		"at backup time if it depends on index data the backup does not restore",
	true,
	settings.WithVisibility(settings.Reserved),
)
