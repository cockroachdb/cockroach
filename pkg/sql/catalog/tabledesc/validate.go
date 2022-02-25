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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	// Check that the mutation ID values are appropriately set when a declarative
	// schema change is underway.
	if n := len(desc.Mutations); n > 0 && desc.GetDeclarativeSchemaChangerState() != nil {
		lastMutationID := desc.Mutations[n-1].MutationID
		if lastMutationID != desc.NextMutationID {
			vea.Report(errors.AssertionFailedf(
				"expected next mutation ID to be %d in table undergoing declarative schema change, found %d instead",
				lastMutationID, desc.NextMutationID))
		}
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *wrapper) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	ids := catalog.MakeDescriptorIDSet(desc.GetID(), desc.GetParentID())
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		ids.Add(desc.GetParentSchemaID())
	}
	// Collect referenced table IDs in foreign keys.
	for _, fk := range desc.OutboundFKs {
		ids.Add(fk.ReferencedTableID)
	}
	for _, fk := range desc.InboundFKs {
		ids.Add(fk.OriginTableID)
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
		if !types.IsOIDUserDefinedType(oid) {
			continue
		}
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
	} else if dbDesc.Dropped() {
		vea.Report(errors.AssertionFailedf("parent database %q (%d) is dropped",
			dbDesc.GetName(), dbDesc.GetID()))
	}

	// Check that parent schema exists.
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID && !desc.IsTemporary() {
		schemaDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
		if err != nil {
			vea.Report(err)
		}
		if schemaDesc != nil && dbDesc != nil && schemaDesc.GetParentID() != dbDesc.GetID() {
			vea.Report(errors.AssertionFailedf("parent schema %d is in different database %d",
				desc.GetParentSchemaID(), schemaDesc.GetParentID()))
		}
		if schemaDesc != nil && schemaDesc.Dropped() {
			vea.Report(errors.AssertionFailedf("parent schema %q (%d) is dropped",
				schemaDesc.GetName(), schemaDesc.GetID()))
		}
	}

	if dbDesc != nil {
		// Validate the all types present in the descriptor exist.
		typeIDs, _, err := desc.GetAllReferencedTypeIDs(dbDesc, vdg.GetTypeDescriptor)
		if err != nil {
			vea.Report(err)
		} else {
			for _, id := range typeIDs {
				_, err := vdg.GetTypeDescriptor(id)
				vea.Report(err)
			}
		}

		// Validate table locality.
		if err := multiregion.ValidateTableLocalityConfig(desc, dbDesc, vdg); err != nil {
			vea.Report(errors.Wrap(err, "invalid locality config"))
			return
		}
	}

	// For views, check dependent relations.
	if desc.IsView() {
		for _, id := range desc.DependsOn {
			vea.Report(desc.validateOutboundTableRef(id, vdg))
		}
		for _, id := range desc.DependsOnTypes {
			vea.Report(desc.validateOutboundTypeRef(id, vdg))
		}
	}

	for _, by := range desc.DependedOnBy {
		vea.Report(desc.validateInboundTableRef(by, vdg))
	}

	// For row-level TTL, only ascending PKs are permitted.
	if desc.HasRowLevelTTL() {
		pk := desc.GetPrimaryIndex()
		if col, err := desc.FindColumnWithName(colinfo.TTLDefaultExpirationColumnName); err != nil {
			vea.Report(errors.Wrapf(err, "expected column %s", colinfo.TTLDefaultExpirationColumnName))
		} else {
			intervalExpr := desc.GetRowLevelTTL().DurationExpr
			expectedStr := `current_timestamp():::TIMESTAMPTZ + ` + string(intervalExpr)
			if col.GetDefaultExpr() != expectedStr {
				vea.Report(pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"expected DEFAULT expression of %s to be %s",
					colinfo.TTLDefaultExpirationColumnName,
					expectedStr,
				))
			}
			if col.GetOnUpdateExpr() != expectedStr {
				vea.Report(pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"expected ON UPDATE expression of %s to be %s",
					colinfo.TTLDefaultExpirationColumnName,
					expectedStr,
				))
			}
		}

		for i := 0; i < pk.NumKeyColumns(); i++ {
			dir := pk.GetKeyColumnDirection(i)
			if dir != descpb.IndexDescriptor_ASC {
				vea.Report(unimplemented.NewWithIssuef(
					76912,
					`non-ascending ordering on PRIMARY KEYs are not supported`,
				))
			}
		}
		if len(desc.OutboundFKs) > 0 || len(desc.InboundFKs) > 0 {
			vea.Report(unimplemented.NewWithIssuef(
				76407,
				`foreign keys to/from table with TTL "%s" are not permitted`,
				desc.Name,
			))
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
}

func (desc *wrapper) validateOutboundTableRef(
	id descpb.ID, vdg catalog.ValidationDescGetter,
) error {
	referencedTable, err := vdg.GetTableDescriptor(id)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on relation reference")
	}
	if referencedTable.Dropped() {
		return errors.AssertionFailedf("depends-on relation %q (%d) is dropped",
			referencedTable.GetName(), referencedTable.GetID())
	}
	for _, by := range referencedTable.TableDesc().DependedOnBy {
		if by.ID == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("depends-on relation %q (%d) has no corresponding depended-on-by back reference",
		referencedTable.GetName(), id)
}

func (desc *wrapper) validateOutboundTypeRef(id descpb.ID, vdg catalog.ValidationDescGetter) error {
	typ, err := vdg.GetTypeDescriptor(id)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on type reference")
	}
	if typ.Dropped() {
		return errors.AssertionFailedf("depends-on type %q (%d) is dropped",
			typ.GetName(), typ.GetID())
	}
	// TODO(postamar): maintain back-references in type, and validate these.
	return nil
}

func (desc *wrapper) validateInboundTableRef(
	by descpb.TableDescriptor_Reference, vdg catalog.ValidationDescGetter,
) error {
	backReferencedTable, err := vdg.GetTableDescriptor(by.ID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depended-on-by relation back reference")
	}
	if backReferencedTable.Dropped() {
		return errors.AssertionFailedf("depended-on-by relation %q (%d) is dropped",
			backReferencedTable.GetName(), backReferencedTable.GetID())
	}
	if desc.IsSequence() {
		// The ColumnIDs field takes a different meaning when the validated
		// descriptor is for a sequence. In this case, they refer to the columns
		// in the referenced descriptor instead.
		for _, colID := range by.ColumnIDs {
			col, _ := backReferencedTable.FindColumnWithID(colID)
			if col == nil {
				return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have a column with ID %d",
					backReferencedTable.GetName(), by.ID, colID)
			}
			var found bool
			for i := 0; i < col.NumUsesSequences(); i++ {
				if col.GetUsesSequenceID(i) == desc.GetID() {
					found = true
					break
				}
			}
			if found {
				continue
			}
			return errors.AssertionFailedf(
				"depended-on-by relation %q (%d) has no reference to this sequence in column %q (%d)",
				backReferencedTable.GetName(), by.ID, col.GetName(), col.GetID())
		}
	}

	// View back-references need corresponding forward reference.
	if !backReferencedTable.IsView() {
		return nil
	}
	for _, id := range backReferencedTable.TableDesc().DependsOn {
		if id == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("depended-on-by view %q (%d) has no corresponding depends-on forward reference",
		backReferencedTable.GetName(), by.ID)
}

func (desc *wrapper) validateOutboundFK(
	fk *descpb.ForeignKeyConstraint, vdg catalog.ValidationDescGetter,
) error {
	referencedTable, err := vdg.GetTableDescriptor(fk.ReferencedTableID)
	if err != nil {
		return errors.Wrapf(err,
			"invalid foreign key: missing table=%d", fk.ReferencedTableID)
	}
	if referencedTable.Dropped() {
		return errors.AssertionFailedf("referenced table %q (%d) is dropped",
			referencedTable.GetName(), referencedTable.GetID())
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
	if originTable.Dropped() {
		return errors.AssertionFailedf("origin table %q (%d) is dropped",
			originTable.GetName(), originTable.GetID())
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
	case *descpb.DescriptorMutation_ModifyRowLevelTTL:
		if m.Direction == descpb.DescriptorMutation_NONE {
			return errors.AssertionFailedf(
				"modify row level TTL mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
		}
	default:
		return errors.AssertionFailedf(
			"mutation in state %s, direction %s, and no column/index descriptor",
			errors.Safe(m.State), errors.Safe(m.Direction))
	}

	switch m.State {
	case descpb.DescriptorMutation_BACKFILLING:
		if _, ok := m.Descriptor_.(*descpb.DescriptorMutation_Index); !ok {
			return errors.AssertionFailedf("non-index mutation in state %s", errors.Safe(m.State))
		}
	case descpb.DescriptorMutation_MERGING:
		if _, ok := m.Descriptor_.(*descpb.DescriptorMutation_Index); !ok {
			return errors.AssertionFailedf("non-index mutation in state %s", errors.Safe(m.State))
		}
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

	// Validate the privilege descriptor.
	if desc.Privileges == nil {
		vea.Report(errors.AssertionFailedf("privileges not set"))
	} else {
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, privilege.Table))
	}

	// Validate that the depended-on-by references are well-formed.
	for _, ref := range desc.DependedOnBy {
		if ref.ID == descpb.InvalidID {
			vea.Report(errors.AssertionFailedf(
				"invalid relation ID %d in depended-on-by references",
				ref.ID))
		}
		if len(ref.ColumnIDs) > catalog.MakeTableColSet(ref.ColumnIDs...).Len() {
			vea.Report(errors.AssertionFailedf("duplicate column IDs found in depended-on-by references: %v",
				ref.ColumnIDs))
		}
	}

	if !desc.IsView() {
		if len(desc.DependsOn) > 0 {
			vea.Report(errors.AssertionFailedf(
				"has depends-on references despite not being a view"))
		}
		if len(desc.DependsOnTypes) > 0 {
			vea.Report(errors.AssertionFailedf(
				"has depends-on-types references despite not being a view"))
		}
	}

	if desc.IsSequence() {
		return
	}

	// Validate the depended-on-by references to this table's columns and indexes
	// for non-sequence relations.
	// The ColumnIDs field takes a different meaning when the validated
	// descriptor is for a sequence, in that case they refer to the columns of the
	// referenced relation. This case is handled during cross-reference
	// validation.
	for _, ref := range desc.DependedOnBy {
		if ref.IndexID != 0 {
			if idx, _ := desc.FindIndexWithID(ref.IndexID); idx == nil {
				vea.Report(errors.AssertionFailedf(
					"index ID %d found in depended-on-by references, no such index in this relation",
					ref.IndexID))
			}
		}
		for _, colID := range ref.ColumnIDs {
			if col, _ := desc.FindColumnWithID(colID); col == nil {
				vea.Report(errors.AssertionFailedf(
					"column ID %d found in depended-on-by references, no such column in this relation",
					colID))
			}
		}
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
		desc.validateConstraintIDs(vea)
	}

	// Ensure that mutations cannot be queued if a primary key change, TTL change
	// or an alter column type schema change has either been started in
	// this transaction, or is currently in progress.
	var alterPKMutation descpb.MutationID
	var alterColumnTypeMutation descpb.MutationID
	var modifyTTLMutation descpb.MutationID
	var foundAlterPK bool
	var foundAlterColumnType bool
	var foundModifyTTL bool

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
		if foundModifyTTL {
			if modifyTTLMutation == m.MutationID {
				vea.Report(pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot perform other schema changes in the same transaction as a TTL mutation",
				))
			} else {
				vea.Report(pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot perform a schema change operation while a TTL change is in progress",
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
		if m.GetModifyRowLevelTTL() != nil {
			foundModifyTTL = true
			modifyTTLMutation = m.MutationID
		}
	}

	// Validate that the presence of MutationJobs (from the old schema changer)
	// and the presence of a DeclarativeSchemaChangeJobID are mutually exclusive. (Note
	// the jobs themselves can be running simultaneously, since a resumer can
	// still be running after the schema change is complete from the point of view
	// of the descriptor, in both the new and old schema change jobs.)
	if dscs := desc.DeclarativeSchemaChangerState; dscs != nil && len(desc.MutationJobs) > 0 {
		vea.Report(errors.AssertionFailedf(
			"invalid concurrent declarative schema change job %d and legacy schema change jobs %v",
			dscs.JobID, desc.MutationJobs))
	}

	// Check that all expression strings can be parsed.
	_ = ForEachExprStringInTableDesc(desc, func(expr *string) error {
		_, err := parser.ParseExpr(*expr)
		vea.Report(err)
		return nil
	})

	vea.Report(ValidateRowLevelTTL(desc.GetRowLevelTTL()))

	// Validate that there are no column with both a foreign key ON UPDATE and an
	// ON UPDATE expression. This check is made to ensure that we know which ON
	// UPDATE action to perform when a FK UPDATE happens.
	ValidateOnUpdate(desc, vea.Report)
}

// ValidateOnUpdate returns an error if there is a column with both a foreign
// key constraint and an ON UPDATE expression, nil otherwise.
func ValidateOnUpdate(desc catalog.TableDescriptor, errReportFn func(err error)) {
	var onUpdateCols catalog.TableColSet
	for _, col := range desc.AllColumns() {
		if col.HasOnUpdate() {
			onUpdateCols.Add(col.GetID())
		}
	}

	_ = desc.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		if fk.OnUpdate == catpb.ForeignKeyAction_NO_ACTION ||
			fk.OnUpdate == catpb.ForeignKeyAction_RESTRICT {
			return nil
		}
		for _, fkCol := range fk.OriginColumnIDs {
			if onUpdateCols.Contains(fkCol) {
				col, err := desc.FindColumnWithID(fkCol)
				if err != nil {
					return err
				}
				errReportFn(pgerror.Newf(pgcode.InvalidTableDefinition,
					"cannot specify both ON UPDATE expression and a foreign key"+
						" ON UPDATE action for column %q",
					col.ColName(),
				))
			}
		}
		return nil
	})
}

func (desc *wrapper) validateConstraintIDs(vea catalog.ValidationErrorAccumulator) {
	if !vea.IsActive(ConstraintIDsAddedToTableDescsVersion) {
		return
	}
	if !desc.IsTable() {
		return
	}
	constraints, err := desc.GetConstraintInfo()
	if err != nil {
		vea.Report(err)
		return
	}
	// Sort the names to get deterministic behaviour, since
	// constraints are stored in a map.
	orderedNames := make([]string, 0, len(constraints))
	for name := range constraints {
		orderedNames = append(orderedNames, name)
	}
	sort.Strings(orderedNames)
	for _, name := range orderedNames {
		constraint := constraints[name]
		if constraint.ConstraintID == 0 {
			vea.Report(errors.AssertionFailedf("constraint id was missing for constraint: %s with name %q",
				constraint.Kind,
				name))

		}
	}
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
			return errors.Newf("column %q duplicate ID of column %q: %d",
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
				return errors.Newf("computed column %q refers to unknown columns in expression: %s",
					column.GetName(), column.GetComputeExpr())
			}
		} else if column.IsVirtual() {
			return errors.Newf("virtual column %q is not computed", column.GetName())
		}

		if column.IsComputed() {
			if column.HasDefault() {
				return pgerror.Newf(pgcode.InvalidTableDefinition,
					"computed column %q cannot also have a DEFAULT expression",
					column.GetName(),
				)
			}
			if column.HasOnUpdate() {
				return pgerror.Newf(pgcode.InvalidTableDefinition,
					"computed column %q cannot also have an ON UPDATE expression",
					column.GetName(),
				)
			}
		}

		if column.IsHidden() && column.IsInaccessible() {
			return errors.Newf("column %q cannot be hidden and inaccessible", column.GetName())
		}

		if column.IsComputed() && column.IsGeneratedAsIdentity() {
			return errors.Newf("both generated identity and computed expression specified for column %q", column.GetName())
		}

		if column.IsNullable() && column.IsGeneratedAsIdentity() {
			return errors.Newf("conflicting NULL/NOT NULL declarations for column %q", column.GetName())
		}

		if column.HasOnUpdate() && column.IsGeneratedAsIdentity() {
			return errors.Newf("both generated identity and on update expression specified for column %q", column.GetName())
		}
	}
	return nil
}

func (desc *wrapper) validateColumnFamilies(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	if len(desc.Families) < 1 {
		return errors.Newf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != descpb.FamilyID(0) {
		return errors.Newf("the 0th family must have ID 0")
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
			return errors.Newf("duplicate family name: %q", family.Name)
		}
		familyNames[family.Name] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return errors.Newf("family %q duplicate ID of family %q: %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return errors.Newf("family %q invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return errors.Newf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			col, ok := columnIDs[colID]
			if !ok {
				return errors.Newf("family %q contains unknown column \"%d\"", family.Name, colID)
			}
			if col.Name != family.ColumnNames[i] {
				return errors.Newf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, col.Name, family.ColumnNames[i])
			}
			if col.Virtual {
				return errors.Newf("virtual computed column %q cannot be part of a family", col.Name)
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return errors.Newf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID, colDesc := range columnIDs {
		if !colDesc.Virtual {
			if _, ok := colIDToFamilyID[colID]; !ok {
				return errors.Newf("column %q is not in any column family", colDesc.Name)
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
				return errors.Newf("check constraint %q contains unknown column \"%d\"", chk.Name, colID)
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
			return errors.Newf("check constraint %q refers to unknown columns in expression: %s",
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
			return errors.Newf(
				"TableID mismatch for unique without index constraint %q: \"%d\" doesn't match descriptor: \"%d\"",
				c.Name, c.TableID, desc.ID,
			)
		}

		// Verify that the constraint's column IDs are valid and unique.
		var seen util.FastIntSet
		for _, colID := range c.ColumnIDs {
			_, ok := columnIDs[colID]
			if !ok {
				return errors.Newf(
					"unique without index constraint %q contains unknown column \"%d\"", c.Name, colID,
				)
			}
			if seen.Contains(int(colID)) {
				return errors.Newf(
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
				return errors.Newf(
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

	indexNames := map[string]struct{}{}
	indexIDs := map[descpb.IndexID]string{}
	for _, idx := range desc.NonDropIndexes() {
		if err := catalog.ValidateName(idx.GetName(), "index"); err != nil {
			return err
		}
		if idx.GetID() == 0 {
			return errors.Newf("invalid index ID %d", idx.GetID())
		}

		if idx.IndexDesc().ForeignKey.IsSet() || len(idx.IndexDesc().ReferencedBy) > 0 {
			return errors.AssertionFailedf("index %q contains deprecated foreign key representation", idx.GetName())
		}

		if len(idx.IndexDesc().Interleave.Ancestors) > 0 || len(idx.IndexDesc().InterleavedBy) > 0 {
			return errors.Newf("index is interleaved")
		}

		if _, indexNameExists := indexNames[idx.GetName()]; indexNameExists {
			for i := range desc.Indexes {
				if desc.Indexes[i].Name == idx.GetName() {
					// This error should be caught in MakeIndexDescriptor or NewTableDesc.
					return errors.HandleAsAssertionFailure(errors.Newf("duplicate index name: %q", idx.GetName()))
				}
			}
			// This error should be caught in MakeIndexDescriptor.
			return errors.HandleAsAssertionFailure(errors.Newf(
				"duplicate: index %q in the middle of being added, not yet public", idx.GetName()))
		}
		indexNames[idx.GetName()] = struct{}{}

		if other, ok := indexIDs[idx.GetID()]; ok {
			return errors.Newf("index %q duplicate ID of index %q: %d",
				idx.GetName(), other, idx.GetID())
		}
		indexIDs[idx.GetID()] = idx.GetName()

		if idx.GetID() >= desc.NextIndexID {
			return errors.Newf("index %q invalid index ID (%d) > next index ID (%d)",
				idx.GetName(), idx.GetID(), desc.NextIndexID)
		}

		if len(idx.IndexDesc().KeyColumnIDs) != len(idx.IndexDesc().KeyColumnNames) {
			return errors.Newf("mismatched column IDs (%d) and names (%d)",
				len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnNames))
		}
		if len(idx.IndexDesc().KeyColumnIDs) != len(idx.IndexDesc().KeyColumnDirections) {
			return errors.Newf("mismatched column IDs (%d) and directions (%d)",
				len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnDirections))
		}
		// In the old STORING encoding, stored columns are in ExtraColumnIDs;
		// tolerate a longer list of column names.
		if len(idx.IndexDesc().StoreColumnIDs) > len(idx.IndexDesc().StoreColumnNames) {
			return errors.Newf("mismatched STORING column IDs (%d) and names (%d)",
				len(idx.IndexDesc().StoreColumnIDs), len(idx.IndexDesc().StoreColumnNames))
		}

		if len(idx.IndexDesc().KeyColumnIDs) == 0 {
			return errors.Newf("index %q must contain at least 1 column", idx.GetName())
		}

		var validateIndexDup catalog.TableColSet
		for i, name := range idx.IndexDesc().KeyColumnNames {
			inIndexColID := idx.IndexDesc().KeyColumnIDs[i]
			colID, ok := columnNames[name]
			if !ok {
				return errors.Newf("index %q contains unknown column %q", idx.GetName(), name)
			}
			if colID != inIndexColID {
				return errors.Newf("index %q column %q should have ID %d, but found ID %d",
					idx.GetName(), name, colID, inIndexColID)
			}
			if validateIndexDup.Contains(colID) {
				col, _ := desc.FindColumnWithID(colID)
				if col.IsExpressionIndexColumn() {
					return pgerror.Newf(pgcode.FeatureNotSupported,
						"index %q contains duplicate expression %q",
						idx.GetName(), col.GetComputeExpr(),
					)
				}
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"index %q contains duplicate column %q",
					idx.GetName(), name,
				)
			}
			validateIndexDup.Add(colID)
		}
		for i, colID := range idx.IndexDesc().StoreColumnIDs {
			inIndexColName := idx.IndexDesc().StoreColumnNames[i]
			col, exists := columnsByID[colID]
			if !exists {
				return errors.Newf("index %q contains stored column %q with unknown ID %d", idx.GetName(), inIndexColName, colID)
			}
			if col.GetName() != inIndexColName {
				return errors.Newf("index %q stored column ID %d should have name %q, but found name %q",
					idx.GetName(), colID, col.ColName(), inIndexColName)
			}
		}
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			if _, exists := columnsByID[colID]; !exists {
				return errors.Newf("index %q key suffix column ID %d is invalid",
					idx.GetName(), colID)
			}
		}

		if idx.IsSharded() {
			if err := desc.ensureShardedIndexNotComputed(idx.IndexDesc()); err != nil {
				return err
			}
			if _, exists := columnNames[idx.GetSharded().Name]; !exists {
				return errors.Newf("index %q refers to non-existent shard column %q",
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
				return errors.Newf("partial index %q refers to unknown columns in predicate: %s",
					idx.GetName(), idx.GetPredicate())
			}
		}

		if !idx.IsMutation() {
			if idx.IndexDesc().UseDeletePreservingEncoding {
				return errors.Newf("public index %q is using the delete preserving encoding", idx.GetName())
			}
		}

		// Ensure that indexes do not STORE virtual columns as suffix columns unless
		// they are primary key columns or future primary key columns (when `ALTER
		// PRIMARY KEY` is executed and a primary key mutation exists).
		curPKColIDs := catalog.MakeTableColSet(desc.PrimaryIndex.KeyColumnIDs...)
		newPKColIDs := catalog.MakeTableColSet()
		for _, mut := range desc.Mutations {
			if mut.GetPrimaryKeySwap() != nil {
				newPKIdxID := mut.GetPrimaryKeySwap().NewPrimaryIndexId
				newPK, err := desc.FindIndexWithID(newPKIdxID)
				if err != nil {
					return err
				}
				newPKColIDs.UnionWith(newPK.CollectKeyColumnIDs())
			}
		}
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			if _, ok := columnsByID[colID]; !ok {
				return errors.Newf("column %d does not exist in table %s", colID, desc.Name)
			}
			col := columnsByID[colID]
			if !col.IsVirtual() {
				continue
			}

			// When newPKColIDs is empty, it means there's no `ALTER PRIMARY KEY` in
			// progress.
			if newPKColIDs.Len() == 0 && curPKColIDs.Contains(colID) {
				continue
			}

			// When newPKColIDs is not empty, it means there is an in-progress `ALTER
			// PRIMARY KEY`. We don't allow queueing schema changes when there's a
			// primary key mutation, so it's safe to make the assumption that `Adding`
			// indexes are associated with the new primary key because they are
			// rewritten and `Non-adding` indexes should only contain virtual column
			// from old primary key.
			isOldPKCol := !idx.Adding() && curPKColIDs.Contains(colID)
			isNewPKCol := idx.Adding() && newPKColIDs.Contains(colID)
			if newPKColIDs.Len() > 0 && (isOldPKCol || isNewPKCol) {
				continue
			}

			return errors.Newf("index %q cannot store virtual column %q", idx.GetName(), col.GetName())
		}

		// Ensure that indexes do not STORE virtual columns.
		for i, colID := range idx.IndexDesc().StoreColumnIDs {
			if col := columnsByID[colID]; col != nil && col.IsVirtual() {
				return errors.Newf("index %q cannot store virtual column %q",
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
	a *tree.DatumAlloc,
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

	// We don't need real prefixes in the DecodePartitionTuple calls because we're
	// only using it to look for collisions and the prefix would be the same for
	// all of them. Faking them out with DNull allows us to make O(list partition)
	// calls to DecodePartitionTuple instead of O(list partition entry).
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	if part.NumLists() == 0 && part.NumRanges() == 0 {
		return errors.Newf("at least one of LIST or RANGE partitioning must be used")
	}
	if part.NumLists() > 0 && part.NumRanges() > 0 {
		return errors.Newf("only one LIST or RANGE partitioning may used")
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
			return errors.Newf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idx.GetName() {
				return errors.Newf("PARTITION %s: name must be unique (used twice in index %q)",
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
				return errors.Newf("PARTITION %s: must contain values", name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range values {
				tuple, keyPrefix, err := rowenc.DecodePartitionTuple(
					a, codec, desc, idx, part, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return errors.Wrapf(err, "PARTITION %s", name)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return errors.Newf("%s cannot be present in more than one partition", tuple)
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
				return errors.Wrapf(err, "PARTITION %s", name)
			}
			toDatums, toKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idx, part, to, fakePrefixDatums)
			if err != nil {
				return errors.Wrapf(err, "PARTITION %s", name)
			}
			pi := partitionInterval{name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return errors.Newf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, name)
			}
			if err := tree.Insert(pi, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
				return errors.Newf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					name, fromDatums, toDatums)
			} else if errors.Is(err, interval.ErrInvertedRange) {
				return errors.Newf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
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

	a := &tree.DatumAlloc{}
	return catalog.ForEachNonDropIndex(desc, func(idx catalog.Index) error {
		return desc.validatePartitioningDescriptor(
			a, idx, idx.GetPartitioning(), 0 /* colOffset */, partitionNames,
		)
	})
}
