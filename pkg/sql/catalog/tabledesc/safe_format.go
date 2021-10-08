// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/redact"
)

// SafeMessage makes immutable a SafeMessager.
func (desc *immutable) SafeMessage() string {
	return formatSafeTableDesc("tabledesc.immutable", desc)
}

// SafeMessage makes Mutable a SafeMessager.
func (desc *Mutable) SafeMessage() string {
	return formatSafeTableDesc("tabledesc.Mutable", desc)
}

func formatSafeTableDesc(typeName string, desc catalog.TableDescriptor) string {
	var buf redact.StringBuilder
	buf.Printf(typeName + ": {")
	formatSafeTableProperties(&buf, desc)
	buf.Printf("}")
	return buf.String()
}
func formatSafeTableProperties(w *redact.StringBuilder, desc catalog.TableDescriptor) {
	catalog.FormatSafeDescriptorProperties(w, desc)
	if desc.IsTemporary() {
		w.Printf(", Temporary: true")
	}
	if desc.IsView() {
		w.Printf(", View: true")
	}
	if desc.IsSequence() {
		w.Printf(", Sequence: true")
	}
	if desc.IsVirtualTable() {
		w.Printf(", Virtual: true")
	}
	formatSafeTableColumns(w, desc)
	formatSafeTableColumnFamilies(w, desc)
	formatSafeTableMutationJobs(w, desc)
	formatSafeMutations(w, desc)
	formatSafeTableIndexes(w, desc)
	formatSafeTableConstraints(w, desc)
	// TODO(ajwerner): Expose OID hashing so that privileges can be displayed
	// reasonably
}

func formatSafeTableColumns(w *redact.StringBuilder, desc catalog.TableDescriptor) {
	var printed bool
	formatColumn := func(c *descpb.ColumnDescriptor, m *descpb.DescriptorMutation) {
		if printed {
			w.Printf(", ")
		}
		formatSafeColumn(w, c, m)
		printed = true
	}
	td := desc.TableDesc()
	w.Printf(", NextColumnID: %d", td.NextColumnID)
	w.Printf(", Columns: [")
	for i := range td.Columns {
		formatColumn(&td.Columns[i], nil)
	}
	w.Printf("]")
}

func formatSafeColumn(
	w *redact.StringBuilder, c *descpb.ColumnDescriptor, m *descpb.DescriptorMutation,
) {
	w.Printf("{ID: %d, TypeID: %d", c.ID, c.Type.InternalType.Oid)
	w.Printf(", Null: %t", c.Nullable)
	if c.Hidden {
		w.Printf(", Hidden: true")
	}
	if c.HasDefault() {
		w.Printf(", HasDefault: true")
	}
	if c.IsComputed() {
		w.Printf(", IsComputed: true")
	}
	if c.AlterColumnTypeInProgress {
		w.Printf(", AlterColumnTypeInProgress: t")
	}
	if len(c.OwnsSequenceIds) > 0 {
		w.Printf(", OwnsSequenceIDs: ")
		formatSafeIDs(w, c.OwnsSequenceIds)
	}
	if len(c.UsesSequenceIds) > 0 {
		w.Printf(", UsesSequenceIDs: ")
		formatSafeIDs(w, c.UsesSequenceIds)
	}
	if m != nil {
		w.Printf(", State: %s, MutationID: %d", m.Direction, m.MutationID)
	}
	w.Printf("}")
}

func formatSafeTableIndexes(w *redact.StringBuilder, desc catalog.TableDescriptor) {
	w.Printf(", PrimaryIndex: %d", desc.GetPrimaryIndexID())
	w.Printf(", NextIndexID: %d", desc.GetNextIndexID())
	w.Printf(", Indexes: [")
	_ = catalog.ForEachActiveIndex(desc, func(idx catalog.Index) error {
		if !idx.Primary() {
			w.Printf(", ")
		}
		formatSafeIndex(w, idx.IndexDesc(), nil)
		return nil
	})
	w.Printf("]")
}

func formatSafeIndex(
	w *redact.StringBuilder, idx *descpb.IndexDescriptor, mut *descpb.DescriptorMutation,
) {
	w.Printf("{")
	w.Printf("ID: %d", idx.ID)
	w.Printf(", Unique: %t", idx.Unique)
	if idx.Predicate != "" {
		w.Printf(", Partial: true")
	}
	if !idx.Interleave.Equal(&descpb.InterleaveDescriptor{}) {
		w.Printf(", InterleaveParents: [")
		for i := range idx.Interleave.Ancestors {
			a := &idx.Interleave.Ancestors[i]
			if i > 0 {
				w.Printf(", ")
			}
			w.Printf("{TableID: %d, IndexID: %d}", a.TableID, a.IndexID)
		}
		w.Printf("]")
	}
	if len(idx.InterleavedBy) > 0 {
		w.Printf(", InterleaveChildren: [")
		for i := range idx.InterleavedBy {
			a := &idx.InterleavedBy[i]
			if i > 0 {
				w.Printf(", ")
			}
			w.Printf("{TableID: %d, IndexID: %d}", a.Table, a.Index)
		}
		w.Printf("]")
	}
	w.Printf(", KeyColumns: [")
	for i := range idx.KeyColumnIDs {
		if i > 0 {
			w.Printf(", ")
		}
		w.Printf("{ID: %d, Dir: %s}", idx.KeyColumnIDs[i], idx.KeyColumnDirections[i])
	}
	w.Printf("]")
	if len(idx.KeySuffixColumnIDs) > 0 {
		w.Printf(", KeySuffixColumns: ")
		formatSafeColumnIDs(w, idx.KeySuffixColumnIDs)
	}
	if len(idx.StoreColumnIDs) > 0 {
		w.Printf(", StoreColumns: ")
		formatSafeColumnIDs(w, idx.StoreColumnIDs)
	}
	if mut != nil {
		w.Printf(", State: %s, MutationID: %d", mut.Direction, mut.MutationID)
	}
	w.Printf("}")
}

func formatSafeTableConstraints(w *redact.StringBuilder, desc catalog.TableDescriptor) {
	td := desc.TableDesc()
	formatSafeTableChecks(w, td.Checks)
	formatSafeTableUniqueWithoutIndexConstraints(w, td.UniqueWithoutIndexConstraints)
	formatSafeTableFKs(w, "InboundFKs", td.InboundFKs)
	formatSafeTableFKs(w, "OutboundFKs", td.OutboundFKs)
}

func formatSafeTableFKs(
	w *redact.StringBuilder, fksType string, fks []descpb.ForeignKeyConstraint,
) {
	for i := range fks {
		w.Printf(", ")
		if i == 0 {
			w.Printf(fksType)
			w.Printf(": [")
		}
		fk := &fks[i]
		formatSafeFK(w, fk, nil)
	}
	if len(fks) > 0 {
		w.Printf("]")
	}
}

func formatSafeFK(
	w *redact.StringBuilder, fk *descpb.ForeignKeyConstraint, m *descpb.DescriptorMutation,
) {
	w.Printf("{OriginTableID: %d", fk.OriginTableID)
	w.Printf(", OriginColumns: ")
	formatSafeColumnIDs(w, fk.OriginColumnIDs)
	w.Printf(", ReferencedTableID: %d", fk.ReferencedTableID)
	w.Printf(", ReferencedColumnIDs: ")
	formatSafeColumnIDs(w, fk.ReferencedColumnIDs)
	w.Printf(", Validity: %s", fk.Validity.String())
	if m != nil {
		w.Printf(", State: %s, MutationID: %d", m.Direction, m.MutationID)
	}
	w.Printf("}")
}

func formatSafeTableChecks(
	w *redact.StringBuilder, checks []*descpb.TableDescriptor_CheckConstraint,
) {
	for i, c := range checks {
		if i == 0 {
			w.Printf(", Checks: [")
		} else {
			w.Printf(", ")
		}
		formatSafeCheck(w, c, nil)
	}
	if len(checks) > 0 {
		w.Printf("]")
	}
}

func formatSafeTableUniqueWithoutIndexConstraints(
	w *redact.StringBuilder, constraints []descpb.UniqueWithoutIndexConstraint,
) {
	for i := range constraints {
		c := &constraints[i]
		if i == 0 {
			w.Printf(", Unique Without Index Constraints: [")
		} else {
			w.Printf(", ")
		}
		formatSafeUniqueWithoutIndexConstraint(w, c, nil)
	}
	if len(constraints) > 0 {
		w.Printf("]")
	}
}

func formatSafeTableColumnFamilies(w *redact.StringBuilder, desc catalog.TableDescriptor) {
	td := desc.TableDesc()
	w.Printf(", NextFamilyID: %d", td.NextFamilyID)
	for i := range td.Families {
		w.Printf(", ")
		if i == 0 {
			w.Printf("Families: [")
		}
		formatSafeTableColumnFamily(w, &td.Families[i])
	}
	if len(td.Families) > 0 {
		w.Printf("]")
	}
}

func formatSafeTableColumnFamily(w *redact.StringBuilder, f *descpb.ColumnFamilyDescriptor) {
	w.Printf("{")
	w.Printf("ID: %d", f.ID)
	w.Printf(", Columns: ")
	formatSafeColumnIDs(w, f.ColumnIDs)
	w.Printf("}")
}

func formatSafeTableMutationJobs(w *redact.StringBuilder, td catalog.TableDescriptor) {
	mutationJobs := td.GetMutationJobs()
	for i := range mutationJobs {
		w.Printf(", ")
		if i == 0 {
			w.Printf("MutationJobs: [")
		}
		m := &mutationJobs[i]
		w.Printf("{MutationID: %d, JobID: %d}", m.MutationID, m.JobID)
	}
	if len(mutationJobs) > 0 {
		w.Printf("]")
	}
}

func formatSafeMutations(w *redact.StringBuilder, td catalog.TableDescriptor) {
	mutations := td.TableDesc().Mutations
	for i := range mutations {
		w.Printf(", ")
		m := &mutations[i]
		if i == 0 {
			w.Printf("Mutations: [")
		}
		formatSafeMutation(w, m)
	}
	if len(mutations) > 0 {
		w.Printf("]")
	}
}

func formatSafeMutation(w *redact.StringBuilder, m *descpb.DescriptorMutation) {
	w.Printf("{MutationID: %d", m.MutationID)
	w.Printf(", Direction: %s", m.Direction)
	w.Printf(", State: %s", m.State)
	switch md := m.Descriptor_.(type) {
	case *descpb.DescriptorMutation_Constraint:
		w.Printf(", ConstraintType: %s", md.Constraint.ConstraintType)
		if md.Constraint.NotNullColumn != 0 {
			w.Printf(", NotNullColumn: %d", md.Constraint.NotNullColumn)
		}
		switch {
		case !md.Constraint.ForeignKey.Equal(&descpb.ForeignKeyConstraint{}):
			w.Printf(", ForeignKey: ")
			formatSafeFK(w, &md.Constraint.ForeignKey, m)
		case !md.Constraint.Check.Equal(&descpb.TableDescriptor_CheckConstraint{}):
			w.Printf(", Check: ")
			formatSafeCheck(w, &md.Constraint.Check, m)
		}
	case *descpb.DescriptorMutation_Index:
		w.Printf(", Index: ")
		formatSafeIndex(w, md.Index, m)
	case *descpb.DescriptorMutation_Column:
		w.Printf(", Column: ")
		formatSafeColumn(w, md.Column, m)
	case *descpb.DescriptorMutation_PrimaryKeySwap:
		w.Printf(", PrimaryKeySwap: {")
		w.Printf("OldPrimaryIndexID: %d", md.PrimaryKeySwap.OldPrimaryIndexId)
		w.Printf(", OldIndexes: ")
		formatSafeIndexIDs(w, md.PrimaryKeySwap.NewIndexes)
		w.Printf(", NewPrimaryIndexID: %d", md.PrimaryKeySwap.NewPrimaryIndexId)
		w.Printf(", NewIndexes: ")
		formatSafeIndexIDs(w, md.PrimaryKeySwap.NewIndexes)
		w.Printf("}")
	case *descpb.DescriptorMutation_ComputedColumnSwap:
		w.Printf(", ComputedColumnSwap: {OldColumnID: %d, NewColumnID: %d}",
			md.ComputedColumnSwap.OldColumnId, md.ComputedColumnSwap.NewColumnId)
	case *descpb.DescriptorMutation_MaterializedViewRefresh:
		w.Printf(", MaterializedViewRefresh: {")
		w.Printf("NewPrimaryIndex: ")
		formatSafeIndex(w, &md.MaterializedViewRefresh.NewPrimaryIndex, m)
		w.Printf(", NewIndexes: [")
		for i := range md.MaterializedViewRefresh.NewIndexes {
			if i > 0 {
				w.Printf(", ")
			}
			formatSafeIndex(w, &md.MaterializedViewRefresh.NewIndexes[i], m)
		}
		w.Printf("]")
		w.Printf(", AsOf: %s, ShouldBackfill: %b",
			md.MaterializedViewRefresh.AsOf, md.MaterializedViewRefresh.ShouldBackfill)
		w.Printf("}")
	}
	w.Printf("}")
}

func formatSafeCheck(
	w *redact.StringBuilder, c *descpb.TableDescriptor_CheckConstraint, m *descpb.DescriptorMutation,
) {
	// TODO(ajwerner): expose OID hashing to get the OID for the
	// constraint.
	w.Printf("{Columns: ")
	formatSafeColumnIDs(w, c.ColumnIDs)
	w.Printf(", Validity: %s", c.Validity.String())
	if c.Hidden {
		w.Printf(", Hidden: true")
	}
	if m != nil {
		w.Printf(", State: %s, MutationID: %d", m.Direction, m.MutationID)
	}
	w.Printf("}")
}

func formatSafeUniqueWithoutIndexConstraint(
	w *redact.StringBuilder, c *descpb.UniqueWithoutIndexConstraint, m *descpb.DescriptorMutation,
) {
	// TODO(ajwerner): expose OID hashing to get the OID for the
	// constraint.
	w.Printf("{TableID: %d", c.TableID)
	w.Printf(", Columns: ")
	formatSafeColumnIDs(w, c.ColumnIDs)
	w.Printf(", Validity: %s", c.Validity.String())
	if m != nil {
		w.Printf(", State: %s, MutationID: %d", m.Direction, m.MutationID)
	}
	w.Printf("}")
}

func formatSafeColumnIDs(w *redact.StringBuilder, colIDs []descpb.ColumnID) {
	w.Printf("[")
	for i, colID := range colIDs {
		if i > 0 {
			w.Printf(", ")
		}
		w.Printf("%d", colID)
	}
	w.Printf("]")
}

func formatSafeIndexIDs(w *redact.StringBuilder, indexIDs []descpb.IndexID) {
	w.Printf("[")
	for i, idxID := range indexIDs {
		if i > 0 {
			w.Printf(", ")
		}
		w.Printf("%d", idxID)
	}
	w.Printf("]")
}

func formatSafeIDs(w *redact.StringBuilder, ids []descpb.ID) {
	w.Printf("[")
	for i, id := range ids {
		if i > 0 {
			w.Printf(", ")
		}
		w.Printf("%d", id)
	}
	w.Printf("]")
}
