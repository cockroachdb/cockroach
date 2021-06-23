// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ValidateTxnCommit performs pre-transaction-commit checks.
func (desc *wrapper) ValidateTxnCommit(
	vea catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// Check that primary key exists.
	if !desc.HasPrimaryKey() {
		vea.Report(unimplemented.NewWithIssue(48026,
			"primary key dropped without subsequent addition of new primary key in same transaction"))
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *wrapper) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	ids := catalog.MakeDescriptorIDSet(desc.GetID(), desc.GetParentID())
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		ids.Add(desc.GetParentSchemaID())
	}
	// Collect referenced table IDs in foreign keys and interleaves.
	for _, fk := range desc.OutboundFKs {
		ids.Add(fk.ReferencedTableID)
	}
	for _, fk := range desc.InboundFKs {
		ids.Add(fk.OriginTableID)
	}
	for _, idx := range desc.NonDropIndexes() {
		for i := 0; i < idx.NumInterleaveAncestors(); i++ {
			ids.Add(idx.GetInterleaveAncestor(i).TableID)
		}
		for i := 0; i < idx.NumInterleavedBy(); i++ {
			ids.Add(idx.GetInterleavedBy(i).Table)
		}
	}
	// Collect user defined type Oids and sequence references in columns.
	for _, col := range desc.DeletableColumns() {
		children, err := typedesc.GetTypeDescriptorClosure(col.GetType())
		if err != nil {
			return catalog.DescriptorIDSet{}, err
		}
		for id := range children {
			ids.Add(id)
		}
		for i := 0; i < col.NumUsesSequences(); i++ {
			ids.Add(col.GetUsesSequenceID(i))
		}
	}
	// Collect user defined type IDs in expressions.
	// All serialized expressions within a table descriptor are serialized
	// with type annotations as IDs, so this visitor will collect them all.
	visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
	_ = ForEachExprStringInTableDesc(desc, func(expr *string) error {
		if parsedExpr, err := parser.ParseExpr(*expr); err == nil {
			// ignore errors
			tree.WalkExpr(visitor, parsedExpr)
		}
		return nil
	})
	// Add collected Oids to return set.
	for oid := range visitor.OIDs {
		id, err := typedesc.UserDefinedTypeOIDToID(oid)
		if err != nil {
			return catalog.DescriptorIDSet{}, err
		}
		ids.Add(id)
	}
	// Add view dependencies.
	for _, id := range desc.GetDependsOn() {
		ids.Add(id)
	}
	for _, id := range desc.GetDependsOnTypes() {
		ids.Add(id)
	}
	for _, ref := range desc.GetDependedOnBy() {
		ids.Add(ref.ID)
	}
	// Add sequence dependencies
	return ids, nil
}

// ValidateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *wrapper) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check that parent DB exists.
	dbDesc, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(err)
	}

	// Check that parent schema exists.
	if desc.GetParentSchemaID() != keys.PublicSchemaID && !desc.IsTemporary() {
		schemaDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
		if err != nil {
			vea.Report(err)
		}
		if schemaDesc != nil && dbDesc != nil && schemaDesc.GetParentID() != dbDesc.GetID() {
			vea.Report(errors.AssertionFailedf("parent schema %d is in different database %d",
				desc.GetParentSchemaID(), schemaDesc.GetParentID()))
		}
	}

	if dbDesc != nil {
		// Validate the all types present in the descriptor exist.
		typeIDs, err := desc.GetAllReferencedTypeIDs(dbDesc, vdg.GetTypeDescriptor)
		if err != nil {
			vea.Report(err)
		} else {
			for _, id := range typeIDs {
				_, err := vdg.GetTypeDescriptor(id)
				vea.Report(err)
			}
		}

		// Validate table locality.
		if err := desc.validateTableLocalityConfig(dbDesc, vdg); err != nil {
			vea.Report(errors.Wrap(err, "invalid locality config"))
			return
		}
	}

	// Check foreign keys.
	for i := range desc.OutboundFKs {
		vea.Report(desc.validateOutboundFK(&desc.OutboundFKs[i], vdg))
	}
	for i := range desc.InboundFKs {
		vea.Report(desc.validateInboundFK(&desc.InboundFKs[i], vdg))
	}

	// Check partitioning is correctly set.
	// We only check these for active indexes, as inactive indexes may be in the
	// process of being backfilled without PartitionAllBy.
	// This check cannot be performed in ValidateSelf due to a conflict with
	// AllocateIDs.
	if desc.PartitionAllBy {
		for _, indexI := range desc.ActiveIndexes() {
			if !desc.matchingPartitionbyAll(indexI) {
				vea.Report(errors.AssertionFailedf(
					"table has PARTITION ALL BY defined, but index %s does not have matching PARTITION BY",
					indexI.GetName(),
				))
			}
		}
	}

	// Check interleaves.
	for _, indexI := range desc.NonDropIndexes() {
		vea.Report(desc.validateIndexInterleave(indexI, vdg))
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.
}

func (desc *wrapper) validateIndexInterleave(
	indexI catalog.Index, vdg catalog.ValidationDescGetter,
) error {
	// Check interleaves.
	if indexI.NumInterleaveAncestors() > 0 {
		// Only check the most recent ancestor, the rest of them don't point
		// back.
		ancestor := indexI.GetInterleaveAncestor(indexI.NumInterleaveAncestors() - 1)
		targetTable, err := vdg.GetTableDescriptor(ancestor.TableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid interleave: missing table=%d index=%d", ancestor.TableID, errors.Safe(ancestor.IndexID))
		}
		targetIndex, err := targetTable.FindIndexWithID(ancestor.IndexID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid interleave: missing table=%s index=%d", targetTable.GetName(), errors.Safe(ancestor.IndexID))
		}

		found := false
		for j := 0; j < targetIndex.NumInterleavedBy(); j++ {
			backref := targetIndex.GetInterleavedBy(j)
			if backref.Table == desc.ID && backref.Index == indexI.GetID() {
				found = true
				break
			}
		}
		if !found {
			return errors.AssertionFailedf(
				"missing interleave back reference to %q@%q from %q@%q",
				desc.Name, indexI.GetName(), targetTable.GetName(), targetIndex.GetName())
		}
	}

	interleaveBackrefs := make(map[descpb.ForeignKeyReference]struct{})
	for j := 0; j < indexI.NumInterleavedBy(); j++ {
		backref := indexI.GetInterleavedBy(j)
		if _, ok := interleaveBackrefs[backref]; ok {
			return errors.AssertionFailedf("duplicated interleave backreference %+v", backref)
		}
		interleaveBackrefs[backref] = struct{}{}
		targetTable, err := vdg.GetTableDescriptor(backref.Table)
		if err != nil {
			return errors.Wrapf(err,
				"invalid interleave backreference table=%d index=%d",
				backref.Table, backref.Index)
		}
		targetIndex, err := targetTable.FindIndexWithID(backref.Index)
		if err != nil {
			return errors.Wrapf(err,
				"invalid interleave backreference table=%s index=%d",
				targetTable.GetName(), backref.Index)
		}
		if targetIndex.NumInterleaveAncestors() == 0 {
			return errors.AssertionFailedf(
				"broken interleave backward reference from %q@%q to %q@%q",
				desc.Name, indexI.GetName(), targetTable.GetName(), targetIndex.GetName())
		}
		// The last ancestor is required to be a backreference.
		ancestor := targetIndex.GetInterleaveAncestor(targetIndex.NumInterleaveAncestors() - 1)
		if ancestor.TableID != desc.ID || ancestor.IndexID != indexI.GetID() {
			return errors.AssertionFailedf(
				"broken interleave backward reference from %q@%q to %q@%q",
				desc.Name, indexI.GetName(), targetTable.GetName(), targetIndex.GetName())
		}
	}

	return nil
}

func (desc *wrapper) validateOutboundFK(
	fk *descpb.ForeignKeyConstraint, vdg catalog.ValidationDescGetter,
) error {
	referencedTable, err := vdg.GetTableDescriptor(fk.ReferencedTableID)
	if err != nil {
		return errors.Wrapf(err,
			"invalid foreign key: missing table=%d", fk.ReferencedTableID)
	}
	found := false
	_ = referencedTable.ForeachInboundFK(func(backref *descpb.ForeignKeyConstraint) error {
		if !found && backref.OriginTableID == desc.ID && backref.Name == fk.Name {
			found = true
		}
		return nil
	})
	if found {
		return nil
	}
	// In 20.2 we introduced a bug where we fail to upgrade the FK references
	// on the referenced descriptors from their pre-19.2 format when reading
	// them during validation (#57032). So we account for the possibility of
	// un-upgraded foreign key references on the other table. This logic
	// somewhat parallels the logic in maybeUpgradeForeignKeyRepOnIndex.
	unupgradedFKsPresent := false
	if err := catalog.ForEachIndex(referencedTable, catalog.IndexOpts{}, func(referencedIdx catalog.Index) error {
		if found {
			// TODO (lucy): If we ever revisit the tabledesc.immutable methods, add
			// a way to break out of the index loop.
			return nil
		}
		if len(referencedIdx.IndexDesc().ReferencedBy) > 0 {
			unupgradedFKsPresent = true
		} else {
			return nil
		}
		// Determine whether the index on the other table is a unique index that
		// could support this FK constraint.
		if !referencedIdx.IsValidReferencedUniqueConstraint(fk.ReferencedColumnIDs) {
			return nil
		}
		// Now check the backreferences. Backreferences in ReferencedBy only had
		// Index and Table populated.
		for i := range referencedIdx.IndexDesc().ReferencedBy {
			backref := &referencedIdx.IndexDesc().ReferencedBy[i]
			if backref.Table != desc.ID {
				continue
			}
			// Look up the index that the un-upgraded reference refers to and
			// see if that index could support the foreign key reference. (Note
			// that it shouldn't be possible for this index to not exist. See
			// planner.MaybeUpgradeDependentOldForeignKeyVersionTables, which is
			// called from the drop index implementation.)
			originalOriginIndex, err := desc.FindIndexWithID(backref.Index)
			if err != nil {
				return errors.AssertionFailedf(
					"missing index %d on %q from pre-19.2 foreign key "+
						"backreference %q on %q",
					backref.Index, desc.Name, fk.Name, referencedTable.GetName(),
				)
			}
			if originalOriginIndex.IsValidOriginIndex(fk.OriginColumnIDs) {
				found = true
				break
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if found {
		return nil
	}
	if unupgradedFKsPresent {
		return errors.AssertionFailedf("missing fk back reference %q to %q "+
			"from %q (un-upgraded foreign key references present)",
			fk.Name, desc.Name, referencedTable.GetName())
	}
	return errors.AssertionFailedf("missing fk back reference %q to %q from %q",
		fk.Name, desc.Name, referencedTable.GetName())
}

func (desc *wrapper) validateInboundFK(
	backref *descpb.ForeignKeyConstraint, vdg catalog.ValidationDescGetter,
) error {
	originTable, err := vdg.GetTableDescriptor(backref.OriginTableID)
	if err != nil {
		return errors.Wrapf(err,
			"invalid foreign key backreference: missing table=%d", backref.OriginTableID)
	}
	found := false
	_ = originTable.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		if !found && fk.ReferencedTableID == desc.ID && fk.Name == backref.Name {
			found = true
		}
		return nil
	})
	if found {
		return nil
	}
	// In 20.2 we introduced a bug where we fail to upgrade the FK references
	// on the referenced descriptors from their pre-19.2 format when reading
	// them during validation (#57032). So we account for the possibility of
	// un-upgraded foreign key references on the other table. This logic
	// somewhat parallels the logic in maybeUpgradeForeignKeyRepOnIndex.
	unupgradedFKsPresent := false
	if err := catalog.ForEachIndex(originTable, catalog.IndexOpts{}, func(originIdx catalog.Index) error {
		if found {
			// TODO (lucy): If we ever revisit the tabledesc.immutable methods, add
			// a way to break out of the index loop.
			return nil
		}
		fk := originIdx.IndexDesc().ForeignKey
		if fk.IsSet() {
			unupgradedFKsPresent = true
		} else {
			return nil
		}
		// Determine whether the index on the other table is a index that could
		// support this FK constraint on the referencing side. Such an index would
		// have been required in earlier versions.
		if !originIdx.IsValidOriginIndex(backref.OriginColumnIDs) {
			return nil
		}
		if fk.Table != desc.ID {
			return nil
		}
		// Look up the index that the un-upgraded reference refers to and
		// see if that index could support the foreign key reference. (Note
		// that it shouldn't be possible for this index to not exist. See
		// planner.MaybeUpgradeDependentOldForeignKeyVersionTables, which is
		// called from the drop index implementation.)
		originalReferencedIndex, err := desc.FindIndexWithID(fk.Index)
		if err != nil {
			return errors.AssertionFailedf(
				"missing index %d on %q from pre-19.2 foreign key forward reference %q on %q",
				fk.Index, desc.Name, backref.Name, originTable.GetName(),
			)
		}
		if originalReferencedIndex.IsValidReferencedUniqueConstraint(backref.ReferencedColumnIDs) {
			found = true
		}
		return nil
	}); err != nil {
		return err
	}
	if found {
		return nil
	}
	if unupgradedFKsPresent {
		return errors.AssertionFailedf("missing fk forward reference %q to %q from %q "+
			"(un-upgraded foreign key references present)",
			backref.Name, desc.Name, originTable.GetName())
	}
	return errors.AssertionFailedf("missing fk forward reference %q to %q from %q",
		backref.Name, desc.Name, originTable.GetName())
}

func (desc *wrapper) matchingPartitionbyAll(indexI catalog.Index) bool {
	primaryIndexPartitioning := desc.PrimaryIndex.KeyColumnIDs[:desc.PrimaryIndex.Partitioning.NumColumns]
	indexPartitioning := indexI.IndexDesc().KeyColumnIDs[:indexI.GetPartitioning().NumColumns()]
	if len(primaryIndexPartitioning) != len(indexPartitioning) {
		return false
	}
	for i, id := range primaryIndexPartitioning {
		if id != indexPartitioning[i] {
			return false
		}
	}
	return true
}

func validateMutation(m *descpb.DescriptorMutation) error {
	unSetEnums := m.State == descpb.DescriptorMutation_UNKNOWN || m.Direction == descpb.DescriptorMutation_NONE
	switch desc := m.Descriptor_.(type) {
	case *descpb.DescriptorMutation_Column:
		col := desc.Column
		if unSetEnums {
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, col %q, id %v",
				errors.Safe(m.State), errors.Safe(m.Direction), col.Name, errors.Safe(col.ID))
		}
	case *descpb.DescriptorMutation_Index:
		if unSetEnums {
			idx := desc.Index
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, index %s, id %v",
				errors.Safe(m.State), errors.Safe(m.Direction), idx.Name, errors.Safe(idx.ID))
		}
	case *descpb.DescriptorMutation_Constraint:
		if unSetEnums {
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, constraint %v",
				errors.Safe(m.State), errors.Safe(m.Direction), desc.Constraint.Name)
		}
	case *descpb.DescriptorMutation_PrimaryKeySwap:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"primary key swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	case *descpb.DescriptorMutation_ComputedColumnSwap:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"computed column swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	case *descpb.DescriptorMutation_MaterializedViewRefresh:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"materialized view refresh mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	default:
		return errors.AssertionFailedf(
			"mutation in state %s, direction %s, and no column/index descriptor",
			errors.Safe(m.State), errors.Safe(m.Direction))
	}
	return nil
}

// ValidateSelf validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *wrapper) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc.Name, "table"))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid table ID %d", desc.GetID()))
	}
	if desc.GetParentSchemaID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid parent schema ID %d", desc.GetParentSchemaID()))
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.GetParentID() == descpb.InvalidID && !desc.IsVirtualTable() {
		vea.Report(errors.AssertionFailedf("invalid parent ID %d", desc.GetParentID()))
	}

	if desc.IsSequence() {
		return
	}

	if len(desc.Columns) == 0 {
		vea.Report(ErrMissingColumns)
		return
	}

	columnNames := make(map[string]descpb.ColumnID, len(desc.Columns))
	columnIDs := make(map[descpb.ColumnID]*descpb.ColumnDescriptor, len(desc.Columns))
	if err := desc.validateColumns(columnNames, columnIDs); err != nil {
		vea.Report(err)
		return
	}

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// maybeFillInDescriptor missing from some codepath.
	if desc.GetFormatVersion() < descpb.InterleavedFormatVersion {
		vea.Report(errors.AssertionFailedf(
			"table is encoded using using version %d, but this client only supports version %d",
			desc.GetFormatVersion(), descpb.InterleavedFormatVersion))
		return
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		vea.Report(err)
		return
	}

	mutationsHaveErrs := false
	for _, m := range desc.Mutations {
		if err := validateMutation(&m); err != nil {
			vea.Report(err)
			mutationsHaveErrs = true
		}
		switch desc := m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Column:
			col := desc.Column
			columnIDs[col.ID] = col
		}
	}

	if mutationsHaveErrs {
		return
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families, constraints, and indexes if this is
	// actually a table, not if it's just a view.
	if desc.IsPhysicalTable() {
		newErrs := []error{
			desc.validateColumnFamilies(columnIDs),
			desc.validateCheckConstraints(columnIDs),
			desc.validateUniqueWithoutIndexConstraints(columnIDs),
			desc.validateTableIndexes(columnNames),
			desc.validatePartitioning(),
		}
		hasErrs := false
		for _, err := range newErrs {
			if err != nil {
				vea.Report(err)
				hasErrs = true
			}
		}
		if hasErrs {
			return
		}
	}

	// Validate the privilege descriptor.
	vea.Report(desc.Privileges.Validate(desc.GetID(), privilege.Table))

	// Ensure that mutations cannot be queued if a primary key change or
	// an alter column type schema change has either been started in
	// this transaction, or is currently in progress.
	var alterPKMutation descpb.MutationID
	var alterColumnTypeMutation descpb.MutationID
	var foundAlterPK bool
	var foundAlterColumnType bool

	for _, m := range desc.Mutations {
		// If we have seen an alter primary key mutation, then
		// m we are considering right now is invalid.
		if foundAlterPK {
			if alterPKMutation == m.MutationID {
				vea.Report(unimplemented.NewWithIssue(
					45615,
					"cannot perform other schema changes in the same transaction as a primary key change"),
				)
			} else {
				vea.Report(unimplemented.NewWithIssue(
					45615,
					"cannot perform a schema change operation while a primary key change is in progress"),
				)
			}
			return
		}
		if foundAlterColumnType {
			if alterColumnTypeMutation == m.MutationID {
				vea.Report(unimplemented.NewWithIssue(
					47137,
					"cannot perform other schema changes in the same transaction as an ALTER COLUMN TYPE schema change",
				))
			} else {
				vea.Report(unimplemented.NewWithIssue(
					47137,
					"cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress",
				))
			}
			return
		}
		if m.GetPrimaryKeySwap() != nil {
			foundAlterPK = true
			alterPKMutation = m.MutationID
		}
		if m.GetComputedColumnSwap() != nil {
			foundAlterColumnType = true
			alterColumnTypeMutation = m.MutationID
		}
	}

	// Validate that the presence of MutationJobs (from the old schema changer)
	// and the presence of a NewSchemaChangeJobID are mutually exclusive. (Note
	// the jobs themselves can be running simultaneously, since a resumer can
	// still be running after the schema change is complete from the point of view
	// of the descriptor, in both the new and old schema change jobs.)
	if len(desc.MutationJobs) > 0 && desc.NewSchemaChangeJobID != 0 {
		vea.Report(errors.AssertionFailedf(
			"invalid concurrent new-style schema change job %d and old-style schema change jobs %v",
			desc.NewSchemaChangeJobID, desc.MutationJobs))
	}

	// Check that all expression strings can be parsed.
	_ = ForEachExprStringInTableDesc(desc, func(expr *string) error {
		_, err := parser.ParseExpr(*expr)
		vea.Report(err)
		return nil
	})
}

func (desc *wrapper) validateColumns(
	columnNames map[string]descpb.ColumnID, columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	for _, column := range desc.NonDropColumns() {

		if err := catalog.ValidateName(column.GetName(), "column"); err != nil {
			return err
		}
		if column.GetID() == 0 {
			return errors.AssertionFailedf("invalid column ID %d", errors.Safe(column.GetID()))
		}

		if _, columnNameExists := columnNames[column.GetName()]; columnNameExists {
			for i := range desc.Columns {
				if desc.Columns[i].Name == column.GetName() {
					return pgerror.Newf(pgcode.DuplicateColumn,
						"duplicate column name: %q", column.GetName())
				}
			}
			return pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public", column.GetName())
		}
		if colinfo.IsSystemColumnName(column.GetName()) {
			return pgerror.Newf(pgcode.DuplicateColumn,
				"column name %q conflicts with a system column name", column.GetName())
		}
		columnNames[column.GetName()] = column.GetID()

		if other, ok := columnIDs[column.GetID()]; ok {
			return fmt.Errorf("column %q duplicate ID of column %q: %d",
				column.GetName(), other.Name, column.GetID())
		}
		columnIDs[column.GetID()] = column.ColumnDesc()

		if column.GetID() >= desc.NextColumnID {
			return errors.AssertionFailedf("column %q invalid ID (%d) >= next column ID (%d)",
				column.GetName(), errors.Safe(column.GetID()), errors.Safe(desc.NextColumnID))
		}

		if column.IsComputed() {
			// Verify that the computed column expression is valid.
			expr, err := parser.ParseExpr(column.GetComputeExpr())
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("computed column %q refers to unknown columns in expression: %s",
					column.GetName(), column.GetComputeExpr())
			}
		} else if column.IsVirtual() {
			return fmt.Errorf("virtual column %q is not computed", column.GetName())
		}

		if column.IsHidden() && column.IsInaccessible() {
			return fmt.Errorf("column %q cannot be hidden and inaccessible", column.GetName())
		}
	}
	return nil
}

func (desc *wrapper) validateColumnFamilies(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	if len(desc.Families) < 1 {
		return fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != descpb.FamilyID(0) {
		return fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[descpb.FamilyID]string{}
	colIDToFamilyID := map[descpb.ColumnID]descpb.FamilyID{}
	for i := range desc.Families {
		family := &desc.Families[i]
		if err := catalog.ValidateName(family.Name, "family"); err != nil {
			return err
		}

		if i != 0 {
			prevFam := desc.Families[i-1]
			if family.ID < prevFam.ID {
				return errors.Newf(
					"family %s at index %d has id %d less than family %s at index %d with id %d",
					family.Name, i, family.ID, prevFam.Name, i-1, prevFam.ID)
			}
		}

		if _, ok := familyNames[family.Name]; ok {
			return fmt.Errorf("duplicate family name: %q", family.Name)
		}
		familyNames[family.Name] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return fmt.Errorf("family %q duplicate ID of family %q: %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return fmt.Errorf("family %q invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return fmt.Errorf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			col, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf("family %q contains unknown column \"%d\"", family.Name, colID)
			}
			if col.Name != family.ColumnNames[i] {
				return fmt.Errorf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, col.Name, family.ColumnNames[i])
			}
			if col.Virtual {
				return fmt.Errorf("virtual computed column %q cannot be part of a family", col.Name)
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return fmt.Errorf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID, colDesc := range columnIDs {
		if !colDesc.Virtual {
			if _, ok := colIDToFamilyID[colID]; !ok {
				return fmt.Errorf("column %q is not in any column family", colDesc.Name)
			}
		}
	}
	return nil
}

// validateCheckConstraints validates that check constraints are well formed.
// Checks include validating the column IDs and verifying that check expressions
// do not reference non-existent columns.
func (desc *wrapper) validateCheckConstraints(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	for _, chk := range desc.AllActiveAndInactiveChecks() {
		// Verify that the check's column IDs are valid.
		for _, colID := range chk.ColumnIDs {
			_, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf("check constraint %q contains unknown column \"%d\"", chk.Name, colID)
			}
		}

		// Verify that the check's expression is valid.
		expr, err := parser.ParseExpr(chk.Expr)
		if err != nil {
			return err
		}
		valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("check constraint %q refers to unknown columns in expression: %s",
				chk.Name, chk.Expr)
		}
	}
	return nil
}

// validateUniqueWithoutIndexConstraints validates that unique without index
// constraints are well formed. Checks include validating the column IDs and
// column names.
func (desc *wrapper) validateUniqueWithoutIndexConstraints(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	for _, c := range desc.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		if err := catalog.ValidateName(c.Name, "unique without index constraint"); err != nil {
			return err
		}

		// Verify that the table ID is valid.
		if c.TableID != desc.ID {
			return fmt.Errorf(
				"TableID mismatch for unique without index constraint %q: \"%d\" doesn't match descriptor: \"%d\"",
				c.Name, c.TableID, desc.ID,
			)
		}

		// Verify that the constraint's column IDs are valid and unique.
		var seen util.FastIntSet
		for _, colID := range c.ColumnIDs {
			_, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf(
					"unique without index constraint %q contains unknown column \"%d\"", c.Name, colID,
				)
			}
			if seen.Contains(int(colID)) {
				return fmt.Errorf(
					"unique without index constraint %q contains duplicate column \"%d\"", c.Name, colID,
				)
			}
			seen.Add(int(colID))
		}

		if c.IsPartial() {
			expr, err := parser.ParseExpr(c.Predicate)
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf(
					"partial unique without index constraint %q refers to unknown columns in predicate: %s",
					c.Name,
					c.Predicate,
				)
			}
		}
	}

	return nil
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func (desc *wrapper) validateTableIndexes(columnNames map[string]descpb.ColumnID) error {
	if len(desc.PrimaryIndex.KeyColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	columnsByID := make(map[descpb.ColumnID]catalog.Column)
	for _, col := range desc.DeletableColumns() {
		columnsByID[col.GetID()] = col
	}

	// Verify that the primary index columns are not virtual.
	for _, pkID := range desc.PrimaryIndex.KeyColumnIDs {
		if col := columnsByID[pkID]; col != nil && col.IsVirtual() {
			return fmt.Errorf("primary index column %q cannot be virtual", col.GetName())
		}
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[descpb.IndexID]string{}
	for _, idx := range desc.NonDropIndexes() {
		if err := catalog.ValidateName(idx.GetName(), "index"); err != nil {
			return err
		}
		if idx.GetID() == 0 {
			return fmt.Errorf("invalid index ID %d", idx.GetID())
		}

		if _, indexNameExists := indexNames[idx.GetName()]; indexNameExists {
			for i := range desc.Indexes {
				if desc.Indexes[i].Name == idx.GetName() {
					// This error should be caught in MakeIndexDescriptor or NewTableDesc.
					return errors.HandleAsAssertionFailure(fmt.Errorf("duplicate index name: %q", idx.GetName()))
				}
			}
			// This error should be caught in MakeIndexDescriptor.
			return errors.HandleAsAssertionFailure(fmt.Errorf(
				"duplicate: index %q in the middle of being added, not yet public", idx.GetName()))
		}
		indexNames[idx.GetName()] = struct{}{}

		if other, ok := indexIDs[idx.GetID()]; ok {
			return fmt.Errorf("index %q duplicate ID of index %q: %d",
				idx.GetName(), other, idx.GetID())
		}
		indexIDs[idx.GetID()] = idx.GetName()

		if idx.GetID() >= desc.NextIndexID {
			return fmt.Errorf("index %q invalid index ID (%d) > next index ID (%d)",
				idx.GetName(), idx.GetID(), desc.NextIndexID)
		}

		if len(idx.IndexDesc().KeyColumnIDs) != len(idx.IndexDesc().KeyColumnNames) {
			return fmt.Errorf("mismatched column IDs (%d) and names (%d)",
				len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnNames))
		}
		if len(idx.IndexDesc().KeyColumnIDs) != len(idx.IndexDesc().KeyColumnDirections) {
			return fmt.Errorf("mismatched column IDs (%d) and directions (%d)",
				len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnDirections))
		}
		// In the old STORING encoding, stored columns are in ExtraColumnIDs;
		// tolerate a longer list of column names.
		if len(idx.IndexDesc().StoreColumnIDs) > len(idx.IndexDesc().StoreColumnNames) {
			return fmt.Errorf("mismatched STORING column IDs (%d) and names (%d)",
				len(idx.IndexDesc().StoreColumnIDs), len(idx.IndexDesc().StoreColumnNames))
		}

		if len(idx.IndexDesc().KeyColumnIDs) == 0 {
			return fmt.Errorf("index %q must contain at least 1 column", idx.GetName())
		}

		var validateIndexDup catalog.TableColSet
		for i, name := range idx.IndexDesc().KeyColumnNames {
			colID, ok := columnNames[name]
			if !ok {
				return fmt.Errorf("index %q contains unknown column %q", idx.GetName(), name)
			}
			if colID != idx.IndexDesc().KeyColumnIDs[i] {
				return fmt.Errorf("index %q column %q should have ID %d, but found ID %d",
					idx.GetName(), name, colID, idx.IndexDesc().KeyColumnIDs[i])
			}
			if validateIndexDup.Contains(colID) {
				return pgerror.Newf(pgcode.FeatureNotSupported, "index %q contains duplicate column %q", idx.GetName(), name)
			}
			validateIndexDup.Add(colID)
		}
		if idx.IsSharded() {
			if err := desc.ensureShardedIndexNotComputed(idx.IndexDesc()); err != nil {
				return err
			}
			if _, exists := columnNames[idx.GetSharded().Name]; !exists {
				return fmt.Errorf("index %q refers to non-existent shard column %q",
					idx.GetName(), idx.GetSharded().Name)
			}
		}
		if idx.IsPartial() {
			expr, err := parser.ParseExpr(idx.GetPredicate())
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("partial index %q refers to unknown columns in predicate: %s",
					idx.GetName(), idx.GetPredicate())
			}
		}
		// Ensure that indexes do not STORE virtual columns.
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			if col := columnsByID[colID]; col != nil && col.IsVirtual() {
				return fmt.Errorf("index %q cannot store virtual column %d", idx.GetName(), col)
			}
		}
		for i, colID := range idx.IndexDesc().StoreColumnIDs {
			if col := columnsByID[colID]; col != nil && col.IsVirtual() {
				return fmt.Errorf("index %q cannot store virtual column %q",
					idx.GetName(), idx.IndexDesc().StoreColumnNames[i])
			}
		}
		if idx.Primary() {
			if idx.GetVersion() != descpb.PrimaryIndexWithStoredColumnsVersion {
				return errors.AssertionFailedf("primary index %q has invalid version %d, expected %d",
					idx.GetName(), idx.GetVersion(), descpb.PrimaryIndexWithStoredColumnsVersion)
			}
			if idx.IndexDesc().EncodingType != descpb.PrimaryIndexEncoding {
				return errors.AssertionFailedf("primary index %q has invalid encoding type %d in proto, expected %d",
					idx.GetName(), idx.IndexDesc().EncodingType, descpb.PrimaryIndexEncoding)
			}
		}
		// Ensure that index column ID subsets are well formed.
		if idx.GetVersion() < descpb.StrictIndexColumnIDGuaranteesVersion {
			continue
		}
		if !idx.Primary() && idx.Public() {
			if idx.GetVersion() == descpb.PrimaryIndexWithStoredColumnsVersion {
				return errors.AssertionFailedf("secondary index %q has invalid version %d which is for primary indexes",
					idx.GetName(), idx.GetVersion())
			}
		}
		slices := []struct {
			name  string
			slice []descpb.ColumnID
		}{
			{"KeyColumnIDs", idx.IndexDesc().KeyColumnIDs},
			{"KeySuffixColumnIDs", idx.IndexDesc().KeySuffixColumnIDs},
			{"StoreColumnIDs", idx.IndexDesc().StoreColumnIDs},
		}
		allIDs := catalog.MakeTableColSet()
		sets := map[string]catalog.TableColSet{}
		for _, s := range slices {
			set := catalog.MakeTableColSet(s.slice...)
			sets[s.name] = set
			if set.Len() == 0 {
				continue
			}
			if set.Ordered()[0] <= 0 {
				return errors.AssertionFailedf("index %q contains invalid column ID value %d in %s",
					idx.GetName(), set.Ordered()[0], s.name)
			}
			if set.Len() < len(s.slice) {
				return errors.AssertionFailedf("index %q has duplicates in %s: %v",
					idx.GetName(), s.name, s.slice)
			}
			allIDs.UnionWith(set)
		}
		foundIn := make([]string, 0, len(sets))
		for _, colID := range allIDs.Ordered() {
			foundIn = foundIn[:0]
			for _, s := range slices {
				set := sets[s.name]
				if set.Contains(colID) {
					foundIn = append(foundIn, s.name)
				}
			}
			if len(foundIn) > 1 {
				return errors.AssertionFailedf("index %q has column ID %d present in: %v",
					idx.GetName(), colID, foundIn)
			}
		}
	}
	return nil
}

// ensureShardedIndexNotComputed ensures that the sharded index is not based on a computed
// column. This is because the sharded index is based on a hidden computed shard column
// under the hood and we don't support transitively computed columns (computed column A
// based on another computed column B).
func (desc *wrapper) ensureShardedIndexNotComputed(index *descpb.IndexDescriptor) error {
	for _, colName := range index.Sharded.ColumnNames {
		col, err := desc.FindColumnWithName(tree.Name(colName))
		if err != nil {
			return err
		}
		if col.IsComputed() {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				"cannot create a sharded index on a computed column")
		}
	}
	return nil
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// index-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func (desc *wrapper) validatePartitioningDescriptor(
	a *rowenc.DatumAlloc,
	idx catalog.Index,
	part catalog.Partitioning,
	colOffset int,
	partitionNames map[string]string,
) error {
	if part.NumImplicitColumns() > part.NumColumns() {
		return errors.Newf(
			"cannot have implicit partitioning columns (%d) > partitioning columns (%d)",
			part.NumImplicitColumns(),
			part.NumColumns(),
		)
	}
	if part.NumColumns() == 0 {
		return nil
	}

	// TODO(dan): The sqlccl.GenerateSubzoneSpans logic is easier if we disallow
	// setting zone configs on indexes that are interleaved into another index.
	// InterleavedBy is fine, so using the root of the interleave hierarchy will
	// work. It is expected that this is sufficient for real-world use cases.
	// Revisit this restriction if that expectation is wrong.
	if idx.NumInterleaveAncestors() > 0 {
		return errors.Errorf("cannot set a zone config for interleaved index %s; "+
			"set it on the root of the interleaved hierarchy instead", idx.GetName())
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we're
	// only using it to look for collisions and the prefix would be the same for
	// all of them. Faking them out with DNull allows us to make O(list partition)
	// calls to DecodePartitionTuple instead of O(list partition entry).
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	if part.NumLists() == 0 && part.NumRanges() == 0 {
		return fmt.Errorf("at least one of LIST or RANGE partitioning must be used")
	}
	if part.NumLists() > 0 && part.NumRanges() > 0 {
		return fmt.Errorf("only one LIST or RANGE partitioning may used")
	}

	// Do not validate partitions which use unhydrated user-defined types.
	// This should only happen at read time and descriptors should not become
	// invalid at read time, only at write time.
	{
		for i := colOffset; i < colOffset+part.NumColumns(); i++ {
			// The partitioning descriptor may be invalid and refer to columns
			// not stored in the index. In that case, skip this check as the
			// validation will fail later.
			if i >= idx.NumKeyColumns() {
				continue
			}
			col, err := desc.FindColumnWithID(idx.GetKeyColumnID(i))
			if err != nil {
				return err
			}
			if col.GetType().UserDefined() && !col.GetType().IsHydrated() {
				return nil
			}
		}
	}

	checkName := func(name string) error {
		if len(name) == 0 {
			return fmt.Errorf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idx.GetName() {
				return fmt.Errorf("PARTITION %s: name must be unique (used twice in index %q)",
					name, indexName)
			}
		}
		partitionNames[name] = idx.GetName()
		return nil
	}

	// Use the system-tenant SQL codec when validating the keys in the partition
	// descriptor. We just want to know how the partitions relate to one another,
	// so it's fine to ignore the tenant ID prefix.
	codec := keys.SystemSQLCodec

	if part.NumLists() > 0 {
		listValues := make(map[string]struct{}, part.NumLists())
		err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
			if err := checkName(name); err != nil {
				return err
			}

			if len(values) == 0 {
				return fmt.Errorf("PARTITION %s: must contain values", name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range values {
				tuple, keyPrefix, err := rowenc.DecodePartitionTuple(
					a, codec, desc, idx, part, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return fmt.Errorf("PARTITION %s: %v", name, err)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return fmt.Errorf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + part.NumColumns()
			return desc.validatePartitioningDescriptor(
				a, idx, subPartitioning, newColOffset, partitionNames,
			)
		})
		if err != nil {
			return err
		}
	}

	if part.NumRanges() > 0 {
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		err := part.ForEachRange(func(name string, from, to []byte) error {
			if err := checkName(name); err != nil {
				return err
			}

			// NB: key encoding is used to check uniqueness because it has to match
			// the behavior of the value when indexed.
			fromDatums, fromKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx, part, from, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", name, err)
			}
			toDatums, toKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx, part, to, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", name, err)
			}
			pi := partitionInterval{name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return fmt.Errorf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, name)
			}
			if err := tree.Insert(pi, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					name, fromDatums, toDatums)
			} else if errors.Is(err, interval.ErrInvertedRange) {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
					name, fromDatums, toDatums)
			} else if err != nil {
				return errors.Wrapf(err, "PARTITION %s", name)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of `interval.Interface` but unused in validatePartitioningDescriptor.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func (desc *wrapper) validatePartitioning() error {
	partitionNames := make(map[string]string)

	a := &rowenc.DatumAlloc{}
	return catalog.ForEachNonDropIndex(desc, func(idx catalog.Index) error {
		return desc.validatePartitioningDescriptor(
			a, idx, idx.GetPartitioning(), 0 /* colOffset */, partitionNames,
		)
	})
}

// validateTableLocalityConfig validates whether the descriptor's locality
// config is valid under the given database.
func (desc *wrapper) validateTableLocalityConfig(
	db catalog.DatabaseDescriptor, vdg catalog.ValidationDescGetter,
) error {

	if desc.LocalityConfig == nil {
		if db.IsMultiRegion() {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"database %s is multi-region enabled, but table %s has no locality set",
				db.GetName(),
				desc.GetName(),
			)
		}
		// Nothing to validate for non-multi-region databases.
		return nil
	}

	if !db.IsMultiRegion() {
		s := tree.NewFmtCtx(tree.FmtSimple)
		var locality string
		// Formatting the table locality config should never fail; if it does, the
		// error message is more clear if we construct a dummy locality here.
		if err := FormatTableLocalityConfig(desc.LocalityConfig, s); err != nil {
			locality = "INVALID LOCALITY"
		}
		locality = s.String()
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"database %s is not multi-region enabled, but table %s has locality %s set",
			db.GetName(),
			desc.GetName(),
			locality,
		)
	}

	regionsEnumID, err := db.MultiRegionEnumID()
	if err != nil {
		return err
	}
	regionsEnumDesc, err := vdg.GetTypeDescriptor(regionsEnumID)
	if err != nil {
		return errors.Wrapf(err, "multi-region enum with ID %d does not exist", regionsEnumID)
	}

	// Check non-table items have a correctly set locality.
	if desc.IsSequence() {
		if !desc.IsLocalityRegionalByTable() {
			return errors.AssertionFailedf(
				"expected sequence %s to have locality REGIONAL BY TABLE",
				desc.Name,
			)
		}
	}
	if desc.IsView() {
		if desc.MaterializedView() {
			if !desc.IsLocalityGlobal() {
				return errors.AssertionFailedf(
					"expected materialized view %s to have locality GLOBAL",
					desc.Name,
				)
			}
		} else {
			if !desc.IsLocalityRegionalByTable() {
				return errors.AssertionFailedf(
					"expected view %s to have locality REGIONAL BY TABLE",
					desc.Name,
				)
			}
		}
	}

	// REGIONAL BY TABLE tables homed in the primary region should include a
	// reference to the multi-region type descriptor and a corresponding
	// backreference. All other patterns should only contain a reference if there
	// is an explicit column which uses the multi-region type descriptor as its
	// *types.T. While the specific cases are validated below, we search for the
	// region enum ID in the references list just once, up top here.
	typeIDs, err := desc.GetAllReferencedTypeIDs(db, vdg.GetTypeDescriptor)
	if err != nil {
		return err
	}
	regionEnumIDReferenced := false
	for _, typeID := range typeIDs {
		if typeID == regionsEnumID {
			regionEnumIDReferenced = true
			break
		}
	}
	columnTypesTypeIDs, err := desc.getAllReferencedTypesInTableColumns(vdg.GetTypeDescriptor)
	if err != nil {
		return err
	}
	switch lc := desc.LocalityConfig.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		if regionEnumIDReferenced {
			if _, found := columnTypesTypeIDs[regionsEnumID]; !found {
				return errors.AssertionFailedf(
					"expected no region Enum ID to be referenced by a GLOBAL TABLE: %q"+
						" but found: %d",
					desc.GetName(),
					regionsEnumDesc.GetID(),
				)
			}
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		if !desc.IsPartitionAllBy() {
			return errors.AssertionFailedf("expected REGIONAL BY ROW table to have PartitionAllBy set")
		}
		// For REGIONAL BY ROW tables, ensure partitions in the PRIMARY KEY match
		// the database descriptor. Ensure each public region has a partition,
		// and each transitioning region name to possibly have a partition.
		// We do validation that ensures all index partitions are the same on
		// PARTITION ALL BY.
		regions, err := regionsEnumDesc.RegionNames()
		if err != nil {
			return err
		}
		regionNames := make(map[descpb.RegionName]struct{}, len(regions))
		for _, region := range regions {
			regionNames[region] = struct{}{}
		}
		transitioningRegions, err := regionsEnumDesc.TransitioningRegionNames()
		if err != nil {
			return err
		}
		transitioningRegionNames := make(map[descpb.RegionName]struct{}, len(regions))
		for _, region := range transitioningRegions {
			transitioningRegionNames[region] = struct{}{}
		}

		part := desc.GetPrimaryIndex().GetPartitioning()
		err = part.ForEachList(func(name string, _ [][]byte, _ catalog.Partitioning) error {
			regionName := descpb.RegionName(name)
			// Any transitioning region names may exist.
			if _, ok := transitioningRegionNames[regionName]; ok {
				return nil
			}
			// If a region is not found in any of the region names, we have an unknown
			// partition.
			if _, ok := regionNames[regionName]; !ok {
				return errors.AssertionFailedf(
					"unknown partition %s on PRIMARY INDEX of table %s",
					name,
					desc.GetName(),
				)
			}
			delete(regionNames, regionName)
			return nil
		})
		if err != nil {
			return err
		}

		// Any regions that are not deleted from the above loop is missing.
		for regionName := range regionNames {
			return errors.AssertionFailedf(
				"missing partition %s on PRIMARY INDEX of table %s",
				regionName,
				desc.GetName(),
			)
		}

	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:

		// Table is homed in an explicit (non-primary) region.
		if lc.RegionalByTable.Region != nil {
			foundRegion := false
			regions, err := regionsEnumDesc.RegionNamesForValidation()
			if err != nil {
				return err
			}
			for _, r := range regions {
				if *lc.RegionalByTable.Region == r {
					foundRegion = true
					break
				}
			}
			if !foundRegion {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidTableDefinition,
						`region "%s" has not been added to database "%s"`,
						*lc.RegionalByTable.Region,
						db.DatabaseDesc().Name,
					),
					"available regions: %s",
					strings.Join(regions.ToStrings(), ", "),
				)
			}
			if !regionEnumIDReferenced {
				return errors.AssertionFailedf(
					"expected multi-region enum ID %d to be referenced on REGIONAL BY TABLE: %q locality "+
						"config, but did not find it",
					regionsEnumID,
					desc.GetName(),
				)
			}
		} else {
			if regionEnumIDReferenced {
				// It may be the case that the multi-region type descriptor is used
				// as the type of the table column. Validations should only fail if
				// that is not the case.
				if _, found := columnTypesTypeIDs[regionsEnumID]; !found {
					return errors.AssertionFailedf(
						"expected no region Enum ID to be referenced by a REGIONAL BY TABLE: %q homed in the "+
							"primary region, but found: %d",
						desc.GetName(),
						regionsEnumDesc.GetID(),
					)
				}
			}
		}
	default:
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"unknown locality level: %T",
			lc,
		)
	}
	return nil
}
