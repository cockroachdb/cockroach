// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/errors"
)

// Validate validates that the table descriptor is well formed. Checks include
// both single table and cross table invariants.
func ValidateTableAndCrossReferences(
	ctx context.Context, desc *sqlbase.TableDescriptor, txn *kv.Txn, codec keys.SQLCodec,
) error {
	err := ValidateTable(desc)
	if err != nil {
		return err
	}
	if desc.Dropped() {
		return nil
	}
	return validateCrossReferences(ctx, desc, txn, codec)
}

// ValidateTable validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func ValidateTable(desc *sqlbase.TableDescriptor) error {
	if err := sqlbase.ValidateName(desc.Name, "table"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return errors.AssertionFailedf("invalid table ID %d", errors.Safe(desc.ID))
	}

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return nil
	}

	if desc.IsSequence() {
		return nil
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.ParentID == 0 {
		return errors.AssertionFailedf("invalid parent ID %d", errors.Safe(desc.ParentID))
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// MaybeFillInDescriptor missing from some codepath.
	if v := desc.GetFormatVersion(); v != sqlbase.FamilyFormatVersion && v != sqlbase.InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change maybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		return errors.AssertionFailedf(
			"table %q is encoded using using version %d, but this client only supports version %d and %d",
			desc.Name, errors.Safe(desc.GetFormatVersion()),
			errors.Safe(sqlbase.FamilyFormatVersion), errors.Safe(sqlbase.InterleavedFormatVersion))
	}

	if len(desc.Columns) == 0 {
		return sqlbase.ErrMissingColumns
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		return err
	}

	columnNames := make(map[string]sqlbase.ColumnID, len(desc.Columns))
	columnIDs := make(map[sqlbase.ColumnID]string, len(desc.Columns))
	for _, column := range desc.AllNonDropColumns() {
		if err := sqlbase.ValidateName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return errors.AssertionFailedf("invalid column ID %d", errors.Safe(column.ID))
		}

		if _, columnNameExists := columnNames[column.Name]; columnNameExists {
			for i := range desc.Columns {
				if desc.Columns[i].Name == column.Name {
					return pgerror.Newf(pgcode.DuplicateColumn,
						"duplicate column name: %q", column.Name)
				}
			}
			return pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public", column.Name)
		}
		columnNames[column.Name] = column.ID

		if other, ok := columnIDs[column.ID]; ok {
			return fmt.Errorf("column %q duplicate ID of column %q: %d",
				column.Name, other, column.ID)
		}
		columnIDs[column.ID] = column.Name

		if column.ID >= desc.NextColumnID {
			return errors.AssertionFailedf("column %q invalid ID (%d) >= next column ID (%d)",
				column.Name, errors.Safe(column.ID), errors.Safe(desc.NextColumnID))
		}
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == sqlbase.DescriptorMutation_UNKNOWN || m.Direction == sqlbase.DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *sqlbase.DescriptorMutation_Column:
			col := desc.Column
			if unSetEnums {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, col %q, id %v",
					errors.Safe(m.State), errors.Safe(m.Direction), col.Name, errors.Safe(col.ID))
			}
			columnIDs[col.ID] = col.Name
		case *sqlbase.DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, index %s, id %v",
					errors.Safe(m.State), errors.Safe(m.Direction), idx.Name, errors.Safe(idx.ID))
			}
		case *sqlbase.DescriptorMutation_Constraint:
			if unSetEnums {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, constraint %v",
					errors.Safe(m.State), errors.Safe(m.Direction), desc.Constraint.Name)
			}
		case *sqlbase.DescriptorMutation_PrimaryKeySwap:
			if m.Direction == sqlbase.DescriptorMutation_NONE {
				return errors.AssertionFailedf(
					"primary key swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		case *sqlbase.DescriptorMutation_ComputedColumnSwap:
			if m.Direction == sqlbase.DescriptorMutation_NONE {
				return errors.AssertionFailedf(
					"computed column swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		default:
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, and no column/index descriptor",
				errors.Safe(m.State), errors.Safe(m.Direction))
		}
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families and indexes if this is actually a table, not
	// if it's just a view.
	if desc.IsPhysicalTable() {
		if err := validateColumnFamilies(desc, columnIDs); err != nil {
			return err
		}

		if err := validateTableIndexes(desc, columnNames); err != nil {
			return err
		}
		if err := validatePartitioning(desc); err != nil {
			return err
		}
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID())

	// Ensure that mutations cannot be queued if a primary key change or
	// an alter column type schema change has either been started in
	// this transaction, or is currently in progress.
	var alterPKMutation sqlbase.MutationID
	var alterColumnTypeMutation sqlbase.MutationID
	var foundAlterPK bool
	var foundAlterColumnType bool

	for _, m := range desc.Mutations {
		// If we have seen an alter primary key mutation, then
		// m we are considering right now is invalid.
		if foundAlterPK {
			if alterPKMutation == m.MutationID {
				return unimplemented.NewWithIssue(
					45615,
					"cannot perform other schema changes in the same transaction as a primary key change",
				)
			}
			return unimplemented.NewWithIssue(
				45615,
				"cannot perform a schema change operation while a primary key change is in progress",
			)
		}
		if foundAlterColumnType {
			if alterColumnTypeMutation == m.MutationID {
				return unimplemented.NewWithIssue(
					47137,
					"cannot perform other schema changes in the same transaction as an ALTER COLUMN TYPE schema change",
				)
			}
			return unimplemented.NewWithIssue(
				47137,
				"cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress",
			)
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

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// ValidateIndexNameIsUnique validates that the index name does not exist.
func ValidateIndexNameIsUnique(desc *sqlbase.TableDescriptor, indexName string) error {
	for _, index := range desc.AllNonDropIndexes() {
		if indexName == index.Name {
			return sqlbase.NewRelationAlreadyExistsError(indexName)
		}
	}
	return nil
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func validateCrossReferences(
	ctx context.Context, desc *sqlbase.TableDescriptor, txn *kv.Txn, codec keys.SQLCodec,
) error {
	// Check that parent DB exists.
	{
		res, err := txn.Get(ctx, sqlbase.MakeDescMetadataKey(codec, desc.ParentID))
		if err != nil {
			return err
		}
		if !res.Exists() {
			return errors.AssertionFailedf("parentID %d does not exist", errors.Safe(desc.ParentID))
		}
	}

	tablesByID := map[sqlbase.ID]*sqlbase.TableDescriptor{desc.ID: desc}
	getTable := func(id sqlbase.ID) (*sqlbase.TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := sqlbase.GetTableDescFromID(ctx, txn, codec, id)
		if err != nil {
			return nil, err
		}
		tablesByID[id] = table
		return table, nil
	}

	findTargetIndex := func(tableID sqlbase.ID, indexID sqlbase.IndexID) (*sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error) {
		targetTable, err := getTable(tableID)
		if err != nil {
			return nil, nil, errors.Wrapf(err,
				"missing table=%d index=%d", errors.Safe(tableID), errors.Safe(indexID))
		}
		targetIndex, err := targetTable.FindIndexByID(indexID)
		if err != nil {
			return nil, nil, errors.Wrapf(err,
				"missing table=%s index=%d", targetTable.Name, errors.Safe(indexID))
		}
		return targetTable, targetIndex, nil
	}

	// Check foreign keys.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		referencedTable, err := getTable(fk.ReferencedTableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid foreign key: missing table=%d", errors.Safe(fk.ReferencedTableID))
		}
		found := false
		for i := range referencedTable.InboundFKs {
			backref := &referencedTable.InboundFKs[i]
			if backref.OriginTableID == desc.ID && backref.Name == fk.Name {
				found = true
				break
			}
		}
		if !found {
			return errors.AssertionFailedf("missing fk back reference %q to %q from %q",
				fk.Name, desc.Name, referencedTable.Name)
		}
	}
	for i := range desc.InboundFKs {
		backref := &desc.InboundFKs[i]
		originTable, err := getTable(backref.OriginTableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid foreign key backreference: missing table=%d", errors.Safe(backref.OriginTableID))
		}
		found := false
		for i := range originTable.OutboundFKs {
			fk := &originTable.OutboundFKs[i]
			if fk.ReferencedTableID == desc.ID && fk.Name == backref.Name {
				found = true
				break
			}
		}
		if !found {
			return errors.AssertionFailedf("missing fk forward reference %q to %q from %q",
				backref.Name, desc.Name, originTable.Name)
		}
	}

	for _, index := range desc.AllNonDropIndexes() {
		// Check interleaves.
		if len(index.Interleave.Ancestors) > 0 {
			// Only check the most recent ancestor, the rest of them don't point
			// back.
			ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
			targetTable, targetIndex, err := findTargetIndex(ancestor.TableID, ancestor.IndexID)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave")
			}
			found := false
			for _, backref := range targetIndex.InterleavedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.AssertionFailedf(
					"missing interleave back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		interleaveBackrefs := make(map[sqlbase.ForeignKeyReference]struct{})
		for _, backref := range index.InterleavedBy {
			if _, ok := interleaveBackrefs[backref]; ok {
				return errors.AssertionFailedf("duplicated interleave backreference %+v", backref)
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err,
					"invalid interleave backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return errors.Wrapf(err,
					"invalid interleave backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if len(targetIndex.Interleave.Ancestors) == 0 {
				return errors.AssertionFailedf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
			// The last ancestor is required to be a backreference.
			ancestor := targetIndex.Interleave.Ancestors[len(targetIndex.Interleave.Ancestors)-1]
			if ancestor.TableID != desc.ID || ancestor.IndexID != index.ID {
				return errors.AssertionFailedf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.

	// Validate the all types present in the descriptor exist. typeMap caches
	// accesses to TypeDescriptors, and is wrapped by getType.
	typeMap := make(map[sqlbase.ID]*sqlbase.TypeDescriptor)
	getType := func(id sqlbase.ID) (*sqlbase.TypeDescriptor, error) {
		if typeDesc, ok := typeMap[id]; ok {
			return typeDesc, nil
		}
		typeDesc, err := sqlbase.GetTypeDescFromID(ctx, txn, codec, id)
		if err != nil {
			return nil, errors.Wrapf(err, "type ID %d in descriptor not found", id)
		}
		typeMap[id] = typeDesc.TypeDesc()
		return typeDesc.TypeDesc(), nil
	}
	typeIDs, err := desc.GetAllReferencedTypeIDs(getType)
	if err != nil {
		return err
	}
	for _, id := range typeIDs {
		if _, err := getType(id); err != nil {
			return err
		}
	}

	return nil
}

func validateColumnFamilies(
	desc *sqlbase.TableDescriptor, columnIDs map[sqlbase.ColumnID]string,
) error {
	if len(desc.Families) < 1 {
		return fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != sqlbase.FamilyID(0) {
		return fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[sqlbase.FamilyID]string{}
	colIDToFamilyID := map[sqlbase.ColumnID]sqlbase.FamilyID{}
	for i := range desc.Families {
		family := &desc.Families[i]
		if err := sqlbase.ValidateName(family.Name, "family"); err != nil {
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
			name, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf("family %q contains unknown column \"%d\"", family.Name, colID)
			}
			if name != family.ColumnNames[i] {
				return fmt.Errorf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, name, family.ColumnNames[i])
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return fmt.Errorf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID := range columnIDs {
		if _, ok := colIDToFamilyID[colID]; !ok {
			return fmt.Errorf("column %d is not in any column family", colID)
		}
	}
	return nil
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func validateTableIndexes(
	desc *sqlbase.TableDescriptor, columnNames map[string]sqlbase.ColumnID,
) error {
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return sqlbase.ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[sqlbase.IndexID]string{}
	for _, index := range desc.AllNonDropIndexes() {
		if err := sqlbase.ValidateName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID %d", index.ID)
		}

		if _, indexNameExists := indexNames[index.Name]; indexNameExists {
			for i := range desc.Indexes {
				if desc.Indexes[i].Name == index.Name {
					// This error should be caught in MakeIndexDescriptor.
					return errors.HandleAsAssertionFailure(fmt.Errorf("duplicate index name: %q", index.Name))
				}
			}
			// This error should be caught in MakeIndexDescriptor.
			return errors.HandleAsAssertionFailure(fmt.Errorf(
				"duplicate: index %q in the middle of being added, not yet public", index.Name))
		}
		indexNames[index.Name] = struct{}{}

		if other, ok := indexIDs[index.ID]; ok {
			return fmt.Errorf("index %q duplicate ID of index %q: %d",
				index.Name, other, index.ID)
		}
		indexIDs[index.ID] = index.Name

		if index.ID >= desc.NextIndexID {
			return fmt.Errorf("index %q invalid index ID (%d) > next index ID (%d)",
				index.Name, index.ID, desc.NextIndexID)
		}

		if len(index.ColumnIDs) != len(index.ColumnNames) {
			return fmt.Errorf("mismatched column IDs (%d) and names (%d)",
				len(index.ColumnIDs), len(index.ColumnNames))
		}
		if len(index.ColumnIDs) != len(index.ColumnDirections) {
			return fmt.Errorf("mismatched column IDs (%d) and directions (%d)",
				len(index.ColumnIDs), len(index.ColumnDirections))
		}

		if len(index.ColumnIDs) == 0 {
			return fmt.Errorf("index %q must contain at least 1 column", index.Name)
		}

		validateIndexDup := make(map[sqlbase.ColumnID]struct{})
		for i, name := range index.ColumnNames {
			colID, ok := columnNames[name]
			if !ok {
				return fmt.Errorf("index %q contains unknown column %q", index.Name, name)
			}
			if colID != index.ColumnIDs[i] {
				return fmt.Errorf("index %q column %q should have ID %d, but found ID %d",
					index.Name, name, colID, index.ColumnIDs[i])
			}
			if _, ok := validateIndexDup[colID]; ok {
				return fmt.Errorf("index %q contains duplicate column %q", index.Name, name)
			}
			validateIndexDup[colID] = struct{}{}
		}
		if index.IsSharded() {
			if err := ensureShardedIndexNotComputed(desc, index); err != nil {
				return err
			}
			if _, exists := columnNames[index.Sharded.Name]; !exists {
				return fmt.Errorf("index %q refers to non-existent shard column %q",
					index.Name, index.Sharded.Name)
			}
		}
	}

	return nil
}

// ensureShardedIndexNotComputed ensures that the sharded index is not based on a computed
// column. This is because the sharded index is based on a hidden computed shard column
// under the hood and we don't support transitively computed columns (computed column A
// based on another computed column B).
func ensureShardedIndexNotComputed(
	desc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) error {
	for _, colName := range index.Sharded.ColumnNames {
		col, _, err := desc.FindColumnByName(tree.Name(colName))
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

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func validatePartitioning(desc *sqlbase.TableDescriptor) error {
	partitionNames := make(map[string]string)

	a := &sqlbase.DatumAlloc{}
	return desc.ForeachNonDropIndex(func(idxDesc *sqlbase.IndexDescriptor) error {
		return validatePartitioningDescriptor(
			desc, a, idxDesc, &idxDesc.Partitioning, 0 /* colOffset */, partitionNames,
		)
	})
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// index-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func validatePartitioningDescriptor(
	desc *sqlbase.TableDescriptor,
	a *sqlbase.DatumAlloc,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	colOffset int,
	partitionNames map[string]string,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// TODO(dan): The sqlccl.GenerateSubzoneSpans logic is easier if we disallow
	// setting zone configs on indexes that are interleaved into another index.
	// InterleavedBy is fine, so using the root of the interleave hierarchy will
	// work. It is expected that this is sufficient for real-world use cases.
	// Revisit this restriction if that expectation is wrong.
	if len(idxDesc.Interleave.Ancestors) > 0 {
		return errors.Errorf("cannot set a zone config for interleaved index %s; "+
			"set it on the root of the interleaved hierarchy instead", idxDesc.Name)
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

	checkName := func(name string) error {
		if len(name) == 0 {
			return fmt.Errorf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idxDesc.Name {
				return fmt.Errorf("PARTITION %s: name must be unique (used twice in index %q)",
					name, indexName)
			}
		}
		partitionNames[name] = idxDesc.Name
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
				tuple, keyPrefix, err := sqlbase.DecodePartitionTuple(
					a, codec, desc, idxDesc, partDesc, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return fmt.Errorf("PARTITION %s: %v", p.Name, err)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return fmt.Errorf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + int(partDesc.NumColumns)
			if err := validatePartitioningDescriptor(
				desc, a, idxDesc, &p.Subpartitioning, newColOffset, partitionNames,
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
			fromDatums, fromKey, err := sqlbase.DecodePartitionTuple(
				a, codec, desc, idxDesc, partDesc, p.FromInclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			toDatums, toKey, err := sqlbase.DecodePartitionTuple(
				a, codec, desc, idxDesc, partDesc, p.ToExclusive, fakePrefixDatums)
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
