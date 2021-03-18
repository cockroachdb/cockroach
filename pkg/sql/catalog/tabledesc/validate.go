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
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ValidateTxnCommit performs pre-transaction-commit checks.
func (desc *wrapper) ValidateTxnCommit(
	vea catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	if desc.Dropped() {
		return
	}

	// Check that primary key exists.
	if !desc.HasPrimaryKey() {
		vea.ReportUnimplemented(
			48026,
			"primary key dropped without subsequent addition of new primary key in same transaction",
		)
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *wrapper) GetReferencedDescIDs() catalog.DescriptorIDSet {
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
		for id := range typedesc.GetTypeDescriptorClosure(col.GetType()) {
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
		ids.Add(typedesc.UserDefinedTypeOIDToID(oid))
	}
	// Add view dependencies.
	for _, id := range desc.GetDependsOn() {
		ids.Add(id)
	}
	for _, ref := range desc.GetDependedOnBy() {
		ids.Add(ref.ID)
	}
	// Add sequence dependencies
	return ids
}

const (
	// ValidationTelemetryKeySuffix constants specific to tabledesc.

	missingColumns                    catalog.ValidationTelemetryKeySuffix = `missing_columns`
	missingPK                         catalog.ValidationTelemetryKeySuffix = `missing_pk`
	missingBackReference              catalog.ValidationTelemetryKeySuffix = `missing_back_reference`
	missingUnupgradedBackReference    catalog.ValidationTelemetryKeySuffix = `missing_pre_19_2_back_reference`
	badUnupgradedBackReference        catalog.ValidationTelemetryKeySuffix = `bad_pre_19_2_back_reference`
	missingForwardReference           catalog.ValidationTelemetryKeySuffix = `missing_forward_reference`
	missingUnupgradedForwardReference catalog.ValidationTelemetryKeySuffix = `missing_pre_19_2_forward_reference`
	badUnupgradedForwardReference     catalog.ValidationTelemetryKeySuffix = `bad_pre_19_2_forward_references`
	missingPartitionBy                catalog.ValidationTelemetryKeySuffix = `missing_partition_by`
	missingFamilies                   catalog.ValidationTelemetryKeySuffix = `missing_families`
	notInFamily                       catalog.ValidationTelemetryKeySuffix = `not_in_family`
	virtualColumn                     catalog.ValidationTelemetryKeySuffix = `virtual_column`
	badExpr                           catalog.ValidationTelemetryKeySuffix = `bad_expr`
	badColumnRefs                     catalog.ValidationTelemetryKeySuffix = `bad_column_refs`
	partitioning                      catalog.ValidationTelemetryKeySuffix = `partitioning`
)

// ValidateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *wrapper) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	if desc.Dropped() {
		return
	}

	// Check that the parent database exists.
	dbDesc, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(catalog.NotFound, err)
	}

	// Check that the parent schema exists and shares the same parent database.
	if desc.GetParentSchemaID() != keys.PublicSchemaID && !desc.IsTemporary() {
		schemaDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
		if err != nil {
			vea.Report(catalog.NotFound, err)
		} else if dbDesc != nil && dbDesc.GetID() != schemaDesc.GetParentID() {
			vea.ReportInternalf(catalog.BadParentID,
				"parent database (%d) of parent schema %q (%d) different than table parent database %q (%d)",
				schemaDesc.GetParentID(), schemaDesc.GetName(), schemaDesc.GetID(), dbDesc.GetName(), dbDesc.GetID())
		}
	}

	if dbDesc != nil {
		// Validate that all types present in the descriptor exist.
		referencedTypeIDs, err := desc.GetAllReferencedTypeIDs(dbDesc, vdg.GetTypeDescriptor)
		if err != nil {
			vea.Report(catalog.NotFound, err)
			return
		}

		for _, id := range referencedTypeIDs {
			_, err := vdg.GetTypeDescriptor(id)
			if err != nil {
				vea.Report(catalog.NotFound, err)
				return
			}
		}

		// Validate table locality config.
		if !desc.validateTableLocalityConfigCrossReferences(vea, dbDesc, vdg) {
			return
		}
	}

	// Check foreign keys.
	for i := range desc.OutboundFKs {
		desc.validateOutboundFK(&desc.OutboundFKs[i], vea, vdg)
	}
	for i := range desc.InboundFKs {
		desc.validateInboundFK(&desc.InboundFKs[i], vea, vdg)
	}

	for _, indexI := range desc.NonDropIndexes() {
		func() {
			vea.PushContextf(`index`, "index %q (%d)", indexI.GetName(), indexI.GetID())
			defer vea.PopContext()

			// Check partitioning is correctly set.
			// We only check these for active indexes, as inactive indexes may be in the
			// process of being backfilled without PartitionAllBy.
			// This check cannot be performed in ValidateSelf due to a conflict with
			// AllocateIDs.
			if desc.PartitionAllBy && !indexI.IsMutation() && !desc.matchingPartitionbyAll(indexI) {
				vea.ReportInternalf(missingPartitionBy,
					"table has PARTITION ALL BY defined, but active index does not have matching PARTITION BY")
			}
			// Check interleaves.
			// TODO(dan): Also validate SharedPrefixLen in the interleaves.
			desc.validateIndexInterleave(indexI, vea, vdg)
		}()
	}
}

func (desc *wrapper) validateIndexInterleave(
	indexI catalog.Index, vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check interleave parent.
	if indexI.NumInterleaveAncestors() > 0 {
		// Only check the most recent ancestor, the rest of them don't point
		// back.
		parent := indexI.GetInterleaveAncestor(indexI.NumInterleaveAncestors() - 1)
		func() {
			vea.PushContextf(`interleave_parent`, "interleave parent (%d@%d)", parent.TableID, parent.IndexID)
			defer vea.PopContext()

			targetTable, err := vdg.GetTableDescriptor(parent.TableID)
			if err != nil {
				vea.Report(catalog.NotFound, err)
				return
			}
			targetIndex, err := targetTable.FindIndexWithID(parent.IndexID)
			if err != nil {
				vea.ReportInternalf(catalog.NotFound, "index not found in table %q", targetTable.GetName())
				return
			}
			for j := 0; j < targetIndex.NumInterleavedBy(); j++ {
				backref := targetIndex.GetInterleavedBy(j)
				if backref.Table == desc.ID && backref.Index == indexI.GetID() {
					return
				}
			}
			vea.ReportInternalf(missingBackReference,
				"missing interleave back reference from %q@%q", targetTable.GetName(), targetIndex.GetName())
		}()
	}

	interleaveBackrefs := make(map[descpb.ForeignKeyReference]struct{})
	for j := 0; j < indexI.NumInterleavedBy(); j++ {
		backref := indexI.GetInterleavedBy(j)
		func() {
			vea.PushContextf(`interleave_backref`, "interleave back reference (%d@%d)", backref.Table, backref.Index)
			defer vea.PopContext()

			if _, ok := interleaveBackrefs[backref]; ok {
				vea.ReportInternalf(catalog.NotUnique, "not unique")
				return
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := vdg.GetTableDescriptor(backref.Table)
			if err != nil {
				vea.Report(catalog.NotFound, err)
				return
			}
			targetIndex, err := targetTable.FindIndexWithID(backref.Index)
			if err != nil {
				vea.ReportInternalf(catalog.NotFound, "index not found in table %q", targetTable.GetName())
				return
			}
			if targetIndex.NumInterleaveAncestors() == 0 {
				vea.ReportInternalf(missingForwardReference, "missing interleave parent in %q@%q", targetTable.GetName(), targetIndex.GetName())
				return
			}
			parent := targetIndex.GetInterleaveAncestor(targetIndex.NumInterleaveAncestors() - 1)
			if parent.TableID != desc.ID || parent.IndexID != indexI.GetID() {
				vea.ReportInternalf(missingForwardReference, "%q@%q has interleave parent but is %d@%d",
					targetTable.GetName(), targetIndex.GetName(), parent.TableID, parent.IndexID)
			}
		}()
	}
}

func (desc *wrapper) validateOutboundFK(
	fk *descpb.ForeignKeyConstraint,
	vea catalog.ValidationErrorAccumulator,
	vdg catalog.ValidationDescGetter,
) {
	vea.PushContextf(`outbound_fk`, "foreign key %q", fk.Name)
	defer vea.PopContext()

	referencedTable, err := vdg.GetTableDescriptor(fk.ReferencedTableID)
	if err != nil {
		vea.Report(catalog.NotFound, err)
		return
	}
	found := false
	_ = referencedTable.ForeachInboundFK(func(backref *descpb.ForeignKeyConstraint) error {
		if !found && backref.OriginTableID == desc.ID && backref.Name == fk.Name {
			found = true
		}
		return nil
	})
	if found {
		return
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
					"un-upgraded back reference %q in table %q (%d) refers back to non-existent index %d",
					backref.Name, referencedTable.GetName(), referencedTable.GetID(), backref.Index)
			}
			if originalOriginIndex.IsValidOriginIndex(fk.OriginColumnIDs) {
				found = true
				break
			}
		}
		return nil
	}); err != nil {
		vea.Report(badUnupgradedBackReference, err)
	}
	if found {
		return
	}
	if unupgradedFKsPresent {
		vea.ReportInternalf(missingUnupgradedBackReference,
			"missing foreign key back reference from %q (%d), un-upgraded foreign key back references present",
			referencedTable.GetName(), referencedTable.GetID())
		return
	}
	vea.ReportInternalf(missingBackReference,
		"missing back reference from %q (%d)",
		referencedTable.GetName(), referencedTable.GetID())
}

func (desc *wrapper) validateInboundFK(
	backref *descpb.ForeignKeyConstraint,
	vea catalog.ValidationErrorAccumulator,
	vdg catalog.ValidationDescGetter,
) {
	vea.PushContextf(`inbound_fk`, "foreign key back reference %q", backref.Name)
	defer vea.PopContext()

	originTable, err := vdg.GetTableDescriptor(backref.OriginTableID)
	if err != nil {
		vea.Report(catalog.NotFound, err)
		return
	}
	found := false
	_ = originTable.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		if !found && fk.ReferencedTableID == desc.ID && fk.Name == backref.Name {
			found = true
		}
		return nil
	})
	if found {
		return
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
		vea.Report(badUnupgradedForwardReference, err)
	}
	if found {
		return
	}
	if unupgradedFKsPresent {
		vea.ReportInternalf(missingUnupgradedForwardReference,
			"missing foreign key reference in back referenced table %q (%d), un-upgraded foreign key references present",
			originTable.GetName(), originTable.GetID())
		return
	}
	vea.ReportInternalf(missingForwardReference,
		"missing foreign key reference in back referenced table %q (%d)",
		originTable.GetName(), originTable.GetID())
}

func (desc *wrapper) matchingPartitionbyAll(indexI catalog.Index) bool {
	primaryIndexPartitioning := desc.PrimaryIndex.ColumnIDs[:desc.PrimaryIndex.Partitioning.NumColumns]
	indexPartitioning := indexI.IndexDesc().ColumnIDs[:indexI.GetPartitioning().NumColumns]
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

func validateMutation(
	m *descpb.DescriptorMutation, vea catalog.ValidationErrorAccumulator,
) (pass bool) {
	pass = true
	reportUnsetState := func() {
		if m.State == descpb.DescriptorMutation_UNKNOWN {
			vea.ReportInternalf(catalog.BadState, "mutation state not set")
			pass = false
		}
	}
	reportUnsetDirection := func() {
		if m.Direction == descpb.DescriptorMutation_NONE {
			vea.ReportInternalf(catalog.NotSet, "mutation direction not set")
			pass = false
		}
	}
	switch desc := m.Descriptor_.(type) {
	case *descpb.DescriptorMutation_Column:
		func() {
			vea.PushContextf(`column`, "column %q (%d)", desc.Column.Name, desc.Column.ID)
			defer vea.PopContext()
			reportUnsetState()
			reportUnsetDirection()
		}()
	case *descpb.DescriptorMutation_Index:
		func() {
			vea.PushContextf(`index`, "index %q (%d)", desc.Index.Name, desc.Index.ID)
			defer vea.PopContext()
			reportUnsetState()
			reportUnsetDirection()
		}()
	case *descpb.DescriptorMutation_Constraint:
		func() {
			vea.PushContextf(`constraint`, "constraint %q", desc.Constraint.Name)
			defer vea.PopContext()
			reportUnsetState()
			reportUnsetDirection()
		}()
	case *descpb.DescriptorMutation_PrimaryKeySwap:
		func() {
			vea.PushContextf(`pk_swap`, "primary key swap")
			defer vea.PopContext()
			reportUnsetDirection()
		}()
	case *descpb.DescriptorMutation_ComputedColumnSwap:
		func() {
			vea.PushContextf(`computed_column_swap`,
				"computed column swap (%d, %d)",
				desc.ComputedColumnSwap.OldColumnId, desc.ComputedColumnSwap.NewColumnId)
			defer vea.PopContext()
			reportUnsetDirection()
		}()
	case *descpb.DescriptorMutation_MaterializedViewRefresh:
		func() {
			vea.PushContextf(`mv_refresh`, "materialized view refresh")
			defer vea.PopContext()
			reportUnsetDirection()
		}()
	default:
		vea.ReportInternalf(catalog.Unclassified, "unknown mutation type %T", desc)
		pass = false
	}
	return pass
}

// ValidateSelf validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *wrapper) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.BadName, catalog.ValidateName(desc.Name, "table"))
	if desc.GetID() == descpb.InvalidID {
		vea.ReportInternalf(catalog.BadID, "invalid table ID")
	}
	if desc.GetParentSchemaID() == descpb.InvalidID {
		vea.ReportInternalf(catalog.BadParentSchemaID, "invalid parent schema ID")
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.GetParentID() == descpb.InvalidID && !desc.IsVirtualTable() {
		vea.ReportInternalf(catalog.BadParentID, "invalid parent database ID")
	}

	// Check locality config.
	if desc.GetLocalityConfig() != nil {
		locality := desc.formattedLocality()
		if !desc.IsLocalityRegionalByTable() {
			if desc.IsView() && !desc.MaterializedView() {
				vea.ReportInternalf(catalog.BadState,
					"non-materialized view locality is %s instead of REGIONAL BY TABLE", locality)
			} else if desc.IsSequence() {
				vea.ReportInternalf(catalog.BadState,
					"sequence locality is %s instead of REGIONAL BY TABLE", locality)
			}
		}
		if !desc.IsLocalityGlobal() {
			if desc.IsView() && desc.MaterializedView() {
				vea.ReportInternalf(catalog.BadState,
					"materialized view locality is %s instead of GLOBAL", locality)
			}
		}
		if desc.IsLocalityRegionalByRow() && !desc.IsPartitionAllBy() {
			vea.ReportInternalf(catalog.NotSet, "locality is %s but table is missing a PARTITION ALL BY clause", locality)
		}
	}

	if desc.IsSequence() {
		return
	}

	if len(desc.Columns) == 0 {
		vea.Report(missingColumns, errors.WithAssertionFailure(ErrMissingColumns))
		return
	}

	columnNames := make(map[string]catalog.Column, len(desc.Columns))
	columnIDs := make(map[descpb.ColumnID]catalog.Column, len(desc.Columns))
	for _, col := range desc.NonDropColumns() {
		if !desc.validateNonDropColumn(col, vea, columnNames, columnIDs) {
			return
		}
	}
	for _, col := range desc.AllColumns() {
		if _, ok := columnIDs[col.GetID()]; !ok && !col.IsSystemColumn() {
			columnIDs[col.GetID()] = col
		}
	}

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// maybeFillInDescriptor missing from some codepath.
	if v := desc.GetFormatVersion(); v != descpb.FamilyFormatVersion && v != descpb.InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change maybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		vea.ReportInternalf(catalog.BadFormat,
			"table is encoded using version %d but this client only supports versions %d and %d",
			desc.GetFormatVersion(),
			descpb.FamilyFormatVersion,
			descpb.InterleavedFormatVersion)
		return
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		if pgerror.GetPGCode(err) == pgcode.DuplicateObject {
			vea.Report(catalog.NotUnique, err)
		} else {
			vea.Report(catalog.Unclassified, err)
		}
		return
	}

	mutationsHaveErrs := false
	for i := range desc.Mutations {
		if !validateMutation(&desc.Mutations[i], vea) {
			mutationsHaveErrs = true
		}
	}
	if mutationsHaveErrs {
		return
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families, constraints, and indexes if this is
	// actually a table, not if it's just a view.
	if desc.IsPhysicalTable() && !desc.validatePhysicalTable(vea, columnIDs, columnNames) {
		return
	}

	// Validate the privilege descriptor.
	vea.Report(catalog.BadPrivileges, desc.Privileges.Validate(desc.GetID(), privilege.Table))

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
				vea.ReportUnimplemented(
					45615,
					"cannot perform other schema changes in the same transaction as a primary key change",
				)
			} else {
				vea.ReportUnimplemented(
					45615,
					"cannot perform a schema change operation while a primary key change is in progress",
				)
			}
			return
		}
		if foundAlterColumnType {
			if alterColumnTypeMutation == m.MutationID {
				vea.ReportUnimplemented(
					47137,
					"cannot perform other schema changes in the same transaction as an ALTER COLUMN TYPE schema change",
				)
			} else {
				vea.ReportUnimplemented(
					47137,
					"cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress",
				)
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

	// Check that all expression strings can be parsed.
	_ = ForEachExprStringInTableDesc(desc, func(expr *string) error {
		desc.validateExpr(*expr, vea)
		return nil
	})
}

func (desc *wrapper) validatePhysicalTable(
	vea catalog.ValidationErrorAccumulator,
	columnIDs map[descpb.ColumnID]catalog.Column,
	columnNames map[string]catalog.Column,
) (pass bool) {
	pass = true
	if !desc.validateColumnFamilies(vea, columnIDs) {
		pass = false
	}
	for _, chk := range desc.AllActiveAndInactiveChecks() {
		if !desc.validateCheckConstraint(chk, vea, columnIDs) {
			pass = false
		}
	}
	for _, c := range desc.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		if !desc.validateUniqueWithoutIndexConstraint(c, vea, columnIDs) {
			pass = false
		}
	}

	if desc.GetPrimaryIndex().NumColumns() == 0 {
		vea.Report(missingPK, errors.WithAssertionFailure(ErrMissingPrimaryKey))
		pass = false
	}

	indexNames := map[string]catalog.Index{}
	indexIDs := map[descpb.IndexID]catalog.Index{}
	for _, idx := range desc.NonDropIndexes() {
		if !desc.validateNonDropIndex(idx, vea, columnNames, columnIDs, indexNames, indexIDs) {
			pass = false
		}
	}

	if desc.validatePartitioning(vea) != nil {
		pass = false
	}

	return pass
}

func (desc *wrapper) validateNonDropColumn(
	column catalog.Column,
	vea catalog.ValidationErrorAccumulator,
	columnNames map[string]catalog.Column,
	columnIDs map[descpb.ColumnID]catalog.Column,
) bool {
	vea.PushContextf(`column`, "column %q (%d)", column.GetName(), column.GetID())
	defer vea.PopContext()

	if err := catalog.ValidateName(column.GetName(), "column"); err != nil {
		return !vea.Report(catalog.BadName, err)
	}
	if colinfo.IsSystemColumnName(column.GetName()) {
		return !vea.ReportPG(catalog.NotUnique, pgcode.DuplicateColumn,
			"column name conflicts with a system column name")
	}

	if column.GetID() == 0 {
		return !vea.ReportInternalf(catalog.BadID, "invalid column ID")
	}

	if other, ok := columnIDs[column.GetID()]; ok {
		return !vea.ReportInternalf(catalog.NotUnique, "another column %q has the same ID", other.GetName())
	}
	columnIDs[column.GetID()] = column

	if other, columnNameExists := columnNames[column.GetName()]; columnNameExists {
		err := pgerror.Newf(pgcode.DuplicateColumn,
			"another column (%d) has the same name", other.GetID())
		if other.IsMutation() {
			err = pgerror.Newf(pgcode.DuplicateColumn,
				"another column (%d) with the same name is currently being added and is not yet public",
				other.GetID())
		}
		return !vea.Report(catalog.NotUnique, err)
	}
	columnNames[column.GetName()] = column

	if column.GetID() >= desc.NextColumnID {
		return !vea.ReportInternalf(catalog.BadID, "ID is not less than next column ID (%d)", desc.NextColumnID)
	}

	if column.IsVirtual() && !column.IsComputed() {
		return !vea.ReportInternalf(virtualColumn, "is virtual but is not computed")
	}

	if column.IsComputed() && !desc.validateExpr(column.GetComputeExpr(), vea) {
		return false
	}

	return true
}

func (desc *wrapper) validateColumnFamilies(
	vea catalog.ValidationErrorAccumulator, columnIDs map[descpb.ColumnID]catalog.Column,
) (pass bool) {
	pass = true

	if len(desc.Families) == 0 {
		return !vea.ReportInternalf(missingFamilies, "at least 1 column family must be specified")
	}

	familyNames := map[string]descpb.FamilyID{}
	familyIDs := map[descpb.FamilyID]string{}
	colIDToFamilyID := map[descpb.ColumnID]descpb.FamilyID{}

	for i := range desc.Families {
		if !desc.validateColumnFamily(i, vea, columnIDs, familyNames, familyIDs, colIDToFamilyID) {
			pass = false
		}
	}

	for colID, col := range columnIDs {
		func() {
			vea.PushContextf(`column`, "column %q (%d)", col.GetName(), col.GetID())
			defer vea.PopContext()
			if _, ok := colIDToFamilyID[colID]; !ok && !col.IsVirtual() {
				vea.ReportInternalf(notInFamily, "non-virtual column not in any column family")
				pass = false
			}
		}()
	}

	return pass
}

func (desc *wrapper) validateColumnFamily(
	familyOrdinal int,
	vea catalog.ValidationErrorAccumulator,
	columnIDs map[descpb.ColumnID]catalog.Column,
	familyNames map[string]descpb.FamilyID,
	familyIDs map[descpb.FamilyID]string,
	colIDToFamilyID map[descpb.ColumnID]descpb.FamilyID,
) bool {
	family := &desc.Families[familyOrdinal]
	vea.PushContextf(`family`, "column family %q (%d)", family.Name, family.ID)
	defer vea.PopContext()

	if familyOrdinal == 0 && family.ID != descpb.FamilyID(0) {
		return !vea.ReportInternalf(catalog.BadID, "the 0th family must have ID 0")
	}

	if err := catalog.ValidateName(family.Name, "family"); err != nil {
		return !vea.Report(catalog.BadName, err)
	}

	if familyOrdinal != 0 {
		prevFam := desc.Families[familyOrdinal-1]
		if family.ID < prevFam.ID {
			return !vea.ReportInternalf(catalog.BadOrder,
				"ID less than previous family %q (%d)", prevFam.Name, prevFam.ID)
		}
	}

	if id, ok := familyNames[family.Name]; ok {
		return !vea.ReportPGf(catalog.NotUnique, pgcode.DuplicateObject,
			"another family (%d) has the same name", id)
	}
	familyNames[family.Name] = family.ID

	if other, ok := familyIDs[family.ID]; ok {
		return !vea.ReportInternalf(catalog.NotUnique, "another family %q has the same ID", other)
	}
	familyIDs[family.ID] = family.Name

	if family.ID >= desc.NextFamilyID {
		return !vea.ReportInternalf(catalog.BadID, "ID is not less than next family ID (%d)", desc.NextFamilyID)
	}

	if len(family.ColumnIDs) != len(family.ColumnNames) {
		return !vea.ReportInternalf(catalog.Mismatch,
			"has %d column IDs but %d column names", len(family.ColumnIDs), len(family.ColumnNames))
	}

	for i, id := range family.ColumnIDs {
		name := family.ColumnNames[i]
		if !func() bool {
			vea.PushContextf(`column`, "column %q (%d)", name, id)
			defer vea.PopContext()
			col, ok := columnIDs[id]
			if !ok {
				return !vea.ReportInternalf(catalog.NotFound, "not found")
			}
			if col.GetName() != name {
				return !vea.ReportInternalf(catalog.BadName, "column with same ID in table has different name %q", col.GetName())
			}
			if col.IsVirtual() {
				return !vea.ReportPG(virtualColumn, pgcode.InvalidColumnReference,
					"a virtual computed column cannot be in a family")
			}
			if famID, ok := colIDToFamilyID[id]; ok {
				return !vea.ReportPGf(catalog.NotUnique, pgcode.InvalidColumnReference,
					"also in another family %q (%d)", familyIDs[famID], famID)
			}
			colIDToFamilyID[id] = family.ID
			return true
		}() {
			return false
		}
	}

	return true
}

// validateCheckConstraint validates that the check constraint is well formed.
// Checks include validating the column IDs and verifying that check expressions
// do not reference non-existent columns.
func (desc *wrapper) validateCheckConstraint(
	chk *descpb.TableDescriptor_CheckConstraint,
	vea catalog.ValidationErrorAccumulator,
	columnIDs map[descpb.ColumnID]catalog.Column,
) (pass bool) {
	pass = true

	vea.PushContextf(`check_constraint`, "check constraint %q", chk.Name)
	defer vea.PopContext()

	// Verify that the check's column IDs are valid.
	for _, colID := range chk.ColumnIDs {
		func() {
			vea.PushContextf(`column`, "column (%d)", colID)
			defer vea.PopContext()
			_, ok := columnIDs[colID]
			if ok {
				return
			}
			vea.ReportInternalf(catalog.NotFound, "not found")
			pass = false
		}()
	}

	if pass {
		// Verify that the check's expression is valid.
		pass = desc.validateExpr(chk.Expr, vea)
	}

	return pass
}

// validateUniqueWithoutIndexConstraints validates that unique without index
// constraints are well formed. Checks include validating the column IDs and
// column names.
func (desc *wrapper) validateUniqueWithoutIndexConstraint(
	c *descpb.UniqueWithoutIndexConstraint,
	vea catalog.ValidationErrorAccumulator,
	columnIDs map[descpb.ColumnID]catalog.Column,
) bool {
	vea.PushContextf(`unique_without_index_constraint`, "unique without index constraint %q", c.Name)
	defer vea.PopContext()

	if err := catalog.ValidateName(c.Name, "unique without index constraint"); err != nil {
		return !vea.Report(catalog.BadName, err)
	}

	// Verify that the table ID is valid.
	if c.TableID != desc.ID {
		return !vea.ReportInternalf(catalog.Mismatch, "referenced table ID (%d) does not match this table", c.TableID)
	}

	// Verify that the constraint's column IDs are valid and unique.
	var seen util.FastIntSet
	for _, colID := range c.ColumnIDs {
		if !func() bool {
			vea.PushContextf(`column`, "column (%d)", colID)
			defer vea.PopContext()
			_, ok := columnIDs[colID]
			if !ok {
				return !vea.ReportInternalf(catalog.NotFound, "not found")
			}
			if seen.Contains(int(colID)) {
				return !vea.ReportInternalf(catalog.NotUnique, "not unique in constraint column ID list")
			}
			return true
		}() {
			return false
		}
		seen.Add(int(colID))
	}

	if c.IsPartial() && !desc.validateExpr(c.Predicate, vea) {
		return false
	}

	return true
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func (desc *wrapper) validateNonDropIndex(
	idx catalog.Index,
	vea catalog.ValidationErrorAccumulator,
	columnNames map[string]catalog.Column,
	columnIDs map[descpb.ColumnID]catalog.Column,
	indexNames map[string]catalog.Index,
	indexIDs map[descpb.IndexID]catalog.Index,
) bool {
	vea.PushContextf(`index`, "index %q (%d)", idx.GetName(), idx.GetID())
	defer vea.PopContext()

	if err := catalog.ValidateName(idx.GetName(), "index"); err != nil {
		return !vea.Report(catalog.BadName, err)
	}
	if idx.GetID() == 0 {
		return !vea.ReportInternalf(catalog.BadID, "invalid index ID")
	}

	if other, indexNameExists := indexNames[idx.GetName()]; indexNameExists {
		if other.IsMutation() {
			// This error should be caught in MakeIndexDescriptor.
			return !vea.ReportInternalf(catalog.NotUnique,
				"another index (%d) with the same name is currently being added and is not yet public", other.GetID())
		}
		// This error should be caught in MakeIndexDescriptor or NewTableDesc.
		return !vea.ReportInternalf(catalog.NotUnique, "another index (%d) has the same name", other.GetID())
	}
	indexNames[idx.GetName()] = idx

	if other, indexIDExists := indexIDs[idx.GetID()]; indexIDExists {
		return !vea.ReportInternalf(catalog.NotUnique, "another index %q has the same ID", other.GetName())
	}
	indexIDs[idx.GetID()] = idx

	if idx.GetID() >= desc.NextIndexID {
		return !vea.ReportInternalf(catalog.BadID, "ID is not less than next index ID (%d)", desc.NextIndexID)
	}

	idxDesc := idx.IndexDesc()

	if len(idxDesc.ColumnIDs) != len(idxDesc.ColumnNames) {
		return !vea.ReportInternalf(catalog.Mismatch,
			"has %d column IDs but %d column names", len(idxDesc.ColumnIDs), len(idxDesc.ColumnNames))
	}
	if len(idxDesc.ColumnIDs) != len(idxDesc.ColumnDirections) {
		return !vea.ReportInternalf(catalog.Mismatch,
			"has %d column IDs but %d column directions", len(idxDesc.ColumnIDs), len(idxDesc.ColumnDirections))
	}
	// In the old STORING encoding, stored columns are in ExtraColumnIDs;
	// tolerate a longer list of column names.
	if len(idxDesc.StoreColumnIDs) > len(idxDesc.StoreColumnNames) {
		return !vea.ReportInternalf(catalog.Mismatch,
			"has %d STORING column IDs but %d STORING column names",
			len(idxDesc.StoreColumnIDs), len(idxDesc.StoreColumnNames))
	}

	if idx.NumColumns() == 0 {
		return !vea.ReportInternalf(missingColumns, "index must have at least one column")
	}

	var validateIndexDup catalog.TableColSet
	for i := 0; i < idx.NumColumns(); i++ {
		id := idx.GetColumnID(i)
		name := idx.GetColumnName(i)
		if !func() bool {
			vea.PushContextf(`column`, "column %q (%d)", name, id)
			defer vea.PopContext()
			col, foundByName := columnNames[name]
			if !foundByName {
				return !vea.ReportPG(catalog.NotFound, pgcode.InvalidColumnReference, "not found")
			}
			if id != col.GetID() {
				return !vea.ReportInternalf(catalog.BadID,
					"column with same name in table has different ID (%d)", col.GetID())
			}
			if validateIndexDup.Contains(id) {
				return !vea.ReportPG(catalog.NotUnique, pgcode.DuplicateColumn, "not unique in index")
			}
			validateIndexDup.Add(id)
			if idx.Primary() && col.IsVirtual() {
				return !vea.ReportPG(virtualColumn, pgcode.InvalidColumnReference,
					"virtual column cannot be in primary index")
			}
			return true
		}() {
			return false
		}
	}

	if idx.IsSharded() {
		if err := desc.ensureShardedIndexNotComputed(idx); err != nil {
			return !vea.Report(catalog.BadState, err)
		}
		if _, exists := columnNames[idx.GetSharded().Name]; !exists {
			return !vea.ReportInternalf(catalog.NotFound, "shard column %q not found", idx.GetSharded().Name)
		}
	}

	if idx.IsPartial() {
		if !desc.validateExpr(idx.GetPredicate(), vea) {
			return false
		}
	}

	getAndCheckCol := func(expectedID descpb.ColumnID, maybeExpectedName string) catalog.Column {
		actualByID, foundByID := columnIDs[expectedID]
		if !foundByID {
			vea.ReportInternalf(catalog.NotFound, "not found")
			return nil
		}
		if maybeExpectedName == "" {
			return actualByID
		}
		if maybeExpectedName != actualByID.GetName() {
			vea.ReportPGf(catalog.BadName, pgcode.InvalidColumnReference,
				"column with same ID in table has different name %q", actualByID.GetName())
			return nil
		}
		actualByName, foundByName := columnNames[maybeExpectedName]
		if !foundByName {
			return actualByID
		}
		if expectedID != actualByName.GetID() {
			vea.ReportPGf(catalog.BadID, pgcode.InvalidColumnReference,
				"column with same name in table has different ID (%d)", actualByName.GetID())
			return nil
		}
		return actualByID
	}

	// Ensure that indexes do not STORE virtual columns.
	for i := 0; i < idx.NumExtraColumns(); i++ {
		id := idx.GetExtraColumnID(i)
		if !func() bool {
			vea.PushContextf(`extra_column`, "extra column (%d)", id)
			defer vea.PopContext()

			col := getAndCheckCol(id, "")
			if col == nil {
				return false
			}
			if col.IsVirtual() {
				return !vea.ReportPGf(virtualColumn, pgcode.InvalidColumnReference,
					"virtual column %q cannot be stored", col.GetName())
			}
			return true
		}() {
			return false
		}
	}

	for i := 0; i < idx.NumStoredColumns(); i++ {
		id := idx.GetStoredColumnID(i)
		name := idx.GetStoredColumnName(i)
		if !func() bool {
			vea.PushContextf(`stored_column`, "STORING column %q (%d)", name, id)
			defer vea.PopContext()
			col := getAndCheckCol(id, name)
			if col == nil {
				return false
			}
			if col.IsVirtual() {
				return !vea.ReportPG(virtualColumn, pgcode.InvalidColumnReference,
					"virtual column cannot be stored")
			}
			return true
		}() {
			return false
		}
	}

	return true
}

func (desc *wrapper) validateExpr(expr string, vea catalog.ValidationErrorAccumulator) bool {
	vea.PushContextf(`expr`, "expression %q", expr)
	defer vea.PopContext()

	parsedExpr, err := parser.ParseExpr(expr)
	if err != nil {
		return !vea.Report(badExpr, err)
	}
	valid, err := schemaexpr.HasValidColumnReferences(desc, parsedExpr)
	if err != nil {
		return !vea.Report(badColumnRefs, errors.WithAssertionFailure(err))
	}
	if !valid {
		return !vea.ReportPG(badColumnRefs, pgcode.InvalidColumnReference, "invalid column reference")
	}
	return true
}

// ensureShardedIndexNotComputed ensures that the sharded index is not based on a computed
// column. This is because the sharded index is based on a hidden computed shard column
// under the hood and we don't support transitively computed columns (computed column A
// based on another computed column B).
func (desc *wrapper) ensureShardedIndexNotComputed(index catalog.Index) error {
	for _, colName := range index.GetSharded().ColumnNames {
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
	partDesc *descpb.PartitioningDescriptor,
	colOffset int,
	partitionNames map[string]string,
) error {
	if partDesc.NumImplicitColumns > partDesc.NumColumns {
		return errors.Newf(
			"cannot have implicit partitioning columns (%d) > partitioning columns (%d)",
			partDesc.NumImplicitColumns,
			partDesc.NumColumns,
		)
	}
	if partDesc.NumColumns == 0 {
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

	if len(partDesc.List) == 0 && len(partDesc.Range) == 0 {
		return fmt.Errorf("at least one of LIST or RANGE partitioning must be used")
	}
	if len(partDesc.List) > 0 && len(partDesc.Range) > 0 {
		return fmt.Errorf("only one LIST or RANGE partitioning may used")
	}

	// Do not validate partitions which use unhydrated user-defined types.
	// This should only happen at read time and descriptors should not become
	// invalid at read time, only at write time.
	{
		numColumns := int(partDesc.NumColumns)
		for i := colOffset; i < colOffset+numColumns; i++ {
			// The partitioning descriptor may be invalid and refer to columns
			// not stored in the index. In that case, skip this check as the
			// validation will fail later.
			if i >= idx.NumColumns() {
				continue
			}
			col, err := desc.FindColumnWithID(idx.GetColumnID(i))
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

	if len(partDesc.List) > 0 {
		listValues := make(map[string]struct{}, len(partDesc.List))
		for _, p := range partDesc.List {
			if err := checkName(p.Name); err != nil {
				return err
			}

			if len(p.Values) == 0 {
				return fmt.Errorf("PARTITION %s: must contain values", p.Name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range p.Values {
				tuple, keyPrefix, err := rowenc.DecodePartitionTuple(
					a, codec, desc, idx.IndexDesc(), partDesc, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return fmt.Errorf("PARTITION %s: %v", p.Name, err)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return fmt.Errorf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + int(partDesc.NumColumns)
			if err := desc.validatePartitioningDescriptor(
				a, idx, &p.Subpartitioning, newColOffset, partitionNames,
			); err != nil {
				return err
			}
		}
	}

	if len(partDesc.Range) > 0 {
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		for _, p := range partDesc.Range {
			if err := checkName(p.Name); err != nil {
				return err
			}

			// NB: key encoding is used to check uniqueness because it has to match
			// the behavior of the value when indexed.
			fromDatums, fromKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx.IndexDesc(), partDesc, p.FromInclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			toDatums, toKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx.IndexDesc(), partDesc, p.ToExclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			pi := partitionInterval{p.Name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return fmt.Errorf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, p.Name)
			}
			if err := tree.Insert(pi, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if errors.Is(err, interval.ErrInvertedRange) {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if err != nil {
				return errors.Wrapf(err, "PARTITION %s", p.Name)
			}
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
func (desc *wrapper) validatePartitioning(maybeVea catalog.ValidationErrorAccumulator) error {
	partitionNames := make(map[string]string)

	a := &rowenc.DatumAlloc{}
	return catalog.ForEachNonDropIndex(desc, func(idx catalog.Index) error {
		if maybeVea != nil {
			maybeVea.PushContextf(`index`, "index %q (%d)", idx.GetName(), idx.GetID())
			defer maybeVea.PopContext()
		}
		err := desc.validatePartitioningDescriptor(
			a, idx, &idx.IndexDesc().Partitioning, 0 /* colOffset */, partitionNames)
		if maybeVea != nil {
			maybeVea.Report(partitioning, err)
		}
		return err
	})
}

func (desc *wrapper) formattedLocality() string {
	if desc.LocalityConfig == nil {
		return ""
	}
	s := tree.NewFmtCtx(tree.FmtSimple)
	if err := FormatTableLocalityConfig(desc.LocalityConfig, s); err != nil {
		return fmt.Sprintf("UNKNOWN LOCALITY TYPE %T", desc.LocalityConfig.Locality)
	}
	return s.String()
}

// validateTableLocalityConfigCrossReferences validates whether the descriptor's
// locality config is valid under the given database.
func (desc *wrapper) validateTableLocalityConfigCrossReferences(
	vea catalog.ValidationErrorAccumulator,
	db catalog.DatabaseDescriptor,
	vdg catalog.ValidationDescGetter,
) bool {

	if desc.LocalityConfig == nil && !db.IsMultiRegion() {
		// Nothing to validate for non-multi-region databases.
		return true
	}
	vea.PushContextf(`parent_database`, "parent database %q (%d)", db.GetName(), db.GetID())
	defer vea.PopContext()

	if !db.IsMultiRegion() {
		vea.ReportPGf(catalog.NotUnset, pgcode.InvalidTableDefinition,
			"not multi-region enabled but table has locality %s", desc.formattedLocality())
		return false
	}

	if !desc.IsLocalityRegionalByTable() ||
		desc.GetLocalityConfig().GetRegionalByTable().Region == nil {
		// No further cross-reference checks except for REGIONAL BY TABLE homed in
		// an explicit (non-primary) region.
		return true
	}

	multiRegionEnumID, err := db.MultiRegionEnumID()
	if err != nil {
		vea.Report(catalog.Unclassified, err)
		return false
	}
	multiRegionEnum, err := vdg.GetTypeDescriptor(multiRegionEnumID)
	if err != nil {
		vea.Report(catalog.NotFound, err)
		return false
	}
	regions, err := multiRegionEnum.RegionNamesIncludingTransitioning()
	if err != nil {
		vea.Report(catalog.Unclassified, err)
		return false
	}
	for _, r := range regions {
		if *desc.GetLocalityConfig().GetRegionalByTable().Region == r {
			return true
		}
	}
	vea.Report(catalog.NotFound, errors.WithHintf(pgerror.Newf(pgcode.InvalidTableDefinition,
		`homing region %q for REGIONAL BY TABLE table not found in multi-region enum %q (%d)`,
		*desc.GetLocalityConfig().GetRegionalByTable().Region, multiRegionEnum.GetName(), multiRegionEnum.GetID()),
		"available regions: %s", strings.Join(regions.ToStrings(), ", ")))
	return false
}
