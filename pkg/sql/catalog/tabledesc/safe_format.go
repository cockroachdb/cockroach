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

// SafeMessage makes Immutable a SafeMessager.
func (desc *Immutable) SafeMessage() string {
	return formatSafeTableDesc("tabledesc.Immutable", desc)
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
	for i := range td.Mutations {
		if c := td.Mutations[i].GetColumn(); c != nil {
			formatColumn(c, &td.Mutations[i])
		}
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
	td := desc.TableDesc()
	w.Printf(", PrimaryIndex: %d", td.PrimaryIndex.ID)
	w.Printf(", NextIndexID: %d", td.NextIndexID)
	w.Printf(", Indexes: [")
	formatSafeIndex(w, &td.PrimaryIndex, nil)
	for i := range td.Indexes {
		w.Printf(", ")
		formatSafeIndex(w, &td.Indexes[i], nil)
	}
	for i := range td.Mutations {
		m := &td.Mutations[i]
		if idx := m.GetIndex(); idx != nil {
			w.Printf(", ")
			formatSafeIndex(w, idx, m)
		}
	}
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
	w.Printf(", Columns: [")
	for i := range idx.ColumnIDs {
		if i > 0 {
			w.Printf(", ")
		}
		w.Printf("{ID: %d, Dir: %s}", idx.ColumnIDs[i], idx.ColumnDirections[i])
	}
	w.Printf("]")
	if len(idx.ExtraColumnIDs) > 0 {
		w.Printf(", ExtraColumns: ")
		formatSafeColumnIDs(w, idx.ExtraColumnIDs)
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
	formatSafeTableChecks(w, td.Checks, td.Mutations)
	formatSafeTableInboundFks(w, td)
	formatSafeTableOutboundFks(w, td)
}

func formatSafeTableInboundFks(w *redact.StringBuilder, td *descpb.TableDescriptor) {
	fks := td.InboundFKs
	fks = fks[:len(fks):len(fks)]
	fkMutations := make([]*descpb.DescriptorMutation, len(fks))
	for i := range td.Mutations {
		m := &td.Mutations[i]
		if c := m.GetConstraint(); c != nil &&
			c.ForeignKey.ReferencedTableID == td.ID {
			fks = append(fks, c.ForeignKey)
			fkMutations = append(fkMutations, m)
		}
	}
	for i := range fks {
		w.Printf(", ")
		if i == 0 {
			w.Printf("InboundFKs: [")
		}
		fk := &fks[i]
		formatSafeFK(w, fk, fkMutations[i])
	}
	if len(fks) > 0 {
		w.Printf("]")
	}
}

func formatSafeTableOutboundFks(w *redact.StringBuilder, td *descpb.TableDescriptor) {
	fks := td.InboundFKs
	fks = fks[:len(fks):len(fks)]
	fkMutations := make([]*descpb.DescriptorMutation, len(fks))
	for i := range td.Mutations {
		m := &td.Mutations[i]
		if c := m.GetConstraint(); c != nil &&
			c.ForeignKey.OriginTableID == td.ID {
			fks = append(fks, c.ForeignKey)
			fkMutations = append(fkMutations, m)
		}
	}
	for i := range fks {
		w.Printf(", ")
		if i == 0 {
			w.Printf("OutboundFKs: [")
		}
		fk := &fks[i]
		formatSafeFK(w, fk, fkMutations[i])
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
	w *redact.StringBuilder,
	checks []*descpb.TableDescriptor_CheckConstraint,
	allMutations []descpb.DescriptorMutation,
) {
	mutations := make([]*descpb.DescriptorMutation, len(checks))
	checks = checks[:len(checks):len(checks)] // copy on append
	for i := range allMutations {
		m := &allMutations[i]
		if c := m.GetConstraint(); c != nil &&
			!c.Check.Equal(&descpb.TableDescriptor_CheckConstraint{}) {
			checks = append(checks, &c.Check)
			mutations = append(mutations, m)
		}
	}
	for i, c := range checks {
		if i == 0 {
			w.Printf(", Checks: [")
		} else {
			w.Printf(", ")
		}
		formatSafeCheck(w, c, mutations[i])
	}
	if len(checks) > 0 {
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
