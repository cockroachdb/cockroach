// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sqlbase

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID uint32

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID uint32

// FamilyID is a custom type for ColumnFamilyDescriptor IDs.
type FamilyID uint32

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID uint32

// DescriptorVersion is a custom type for TableDescriptor Versions.
type DescriptorVersion uint32

// FormatVersion is a custom type for TableDescriptor versions of the sql to
// key:value mapping.
//go:generate stringer -type=FormatVersion
type FormatVersion uint32

const (
	_ FormatVersion = iota
	// BaseFormatVersion corresponds to the encoding described in
	// https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/.
	BaseFormatVersion
	// FamilyFormatVersion corresponds to the encoding described in
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/sql_column_families.md
	FamilyFormatVersion
	// InterleavedFormatVersion corresponds to the encoding described in
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/sql_interleaved_tables.md
	InterleavedFormatVersion
)

// MutationID is custom type for TableDescriptor mutations.
type MutationID uint32

// InvalidMutationID is the uninitialised mutation id.
const InvalidMutationID MutationID = 0

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
)

// DescriptorStatus is the status for a descriptor.
type DescriptorStatus int

const (
	_ DescriptorStatus = iota
	// DescriptorAbsent for a descriptor that doesn't exist.
	DescriptorAbsent
	// DescriptorIncomplete for a descriptor that is a part of a
	// schema change, and is still being processed.
	DescriptorIncomplete
	// DescriptorActive for a descriptor that is completely active
	// for read/write and delete operations.
	DescriptorActive
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

// ToEncodingDirection converts a direction from the proto to an encoding.Direction.
func (dir IndexDescriptor_Direction) ToEncodingDirection() (encoding.Direction, error) {
	switch dir {
	case IndexDescriptor_ASC:
		return encoding.Ascending, nil
	case IndexDescriptor_DESC:
		return encoding.Descending, nil
	default:
		return encoding.Ascending, errors.Errorf("invalid direction: %s", dir)
	}
}

// ErrDescriptorNotFound is returned by GetTableDescFromID to signal that a
// descriptor could not be found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Teturns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(txn *client.Txn, id ID) (*TableDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(descKey, desc); err != nil {
		return nil, err
	}
	table := desc.GetTable()
	if table == nil {
		return nil, ErrDescriptorNotFound
	}
	return table, nil
}

// allocateName sets desc.Name to a value that is not EqualName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func (desc *IndexDescriptor) allocateName(tableDesc *TableDescriptor) {
	segments := make([]string, 0, len(desc.ColumnNames)+2)
	segments = append(segments, tableDesc.Name)
	segments = append(segments, desc.ColumnNames...)
	if desc.Unique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}

	baseName := strings.Join(segments, "_")
	name := baseName

	exists := func(name string) bool {
		_, _, err := tableDesc.FindIndexByName(name)
		return err == nil
	}
	for i := 1; exists(name); i++ {
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	desc.Name = name
}

// FillColumns sets the column names and directions in desc.
func (desc *IndexDescriptor) FillColumns(elems parser.IndexElemList) error {
	desc.ColumnNames = make([]string, 0, len(elems))
	desc.ColumnDirections = make([]IndexDescriptor_Direction, 0, len(elems))
	for _, c := range elems {
		desc.ColumnNames = append(desc.ColumnNames, string(c.Column))
		switch c.Direction {
		case parser.Ascending, parser.DefaultDirection:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
		case parser.Descending:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_DESC)
		default:
			return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
		}
	}
	return nil
}

// ContainsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs or the implicit "extra" column
// IDs.
func (desc *IndexDescriptor) ContainsColumnID(colID ColumnID) bool {
	for _, id := range desc.ColumnIDs {
		if id == colID {
			return true
		}
	}
	for _, id := range desc.ImplicitColumnIDs {
		if id == colID {
			return true
		}
	}
	return false
}

// FullColumnIDs returns the index column IDs including any implicit column IDs
// for non-unique indexes. It also returns the direction with which each column
// was encoded.
func (desc *IndexDescriptor) FullColumnIDs() ([]ColumnID, []encoding.Direction) {
	dirs := make([]encoding.Direction, 0, len(desc.ColumnIDs))
	for _, dir := range desc.ColumnDirections {
		convertedDir, err := dir.ToEncodingDirection()
		if err != nil {
			panic(err)
		}
		dirs = append(dirs, convertedDir)
	}
	if desc.Unique {
		return desc.ColumnIDs, dirs
	}
	// Non-unique indexes have some of the primary-key columns appended to
	// their key.
	columnIDs := append([]ColumnID(nil), desc.ColumnIDs...)
	columnIDs = append(columnIDs, desc.ImplicitColumnIDs...)
	for range desc.ImplicitColumnIDs {
		// Implicit columns are encoded ascendingly.
		dirs = append(dirs, encoding.Ascending)
	}
	return columnIDs, dirs
}

// SetID implements the DescriptorProto interface.
func (desc *TableDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *TableDescriptor) TypeName() string {
	return "table"
}

// SetName implements the DescriptorProto interface.
func (desc *TableDescriptor) SetName(name string) {
	desc.Name = name
}

// allNonDropColumns returns all the columns, including those being added
// in the mutations.
func (desc *TableDescriptor) allNonDropColumns() []ColumnDescriptor {
	cols := make([]ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	cols = append(cols, desc.Columns...)
	for _, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil {
			if m.Direction == DescriptorMutation_ADD {
				cols = append(cols, *col)
			}
		}
	}
	return cols
}

// AllNonDropIndexes returns all the indexes, including those being added
// in the mutations.
func (desc *TableDescriptor) AllNonDropIndexes() []IndexDescriptor {
	indexes := make([]IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	indexes = append(indexes, desc.PrimaryIndex)
	indexes = append(indexes, desc.Indexes...)
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if m.Direction == DescriptorMutation_ADD {
				indexes = append(indexes, *idx)
			}
		}
	}
	return indexes
}

func generatedFamilyName(familyID FamilyID, columnNames []string) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "fam_%d", familyID)
	for _, n := range columnNames {
		buf.WriteString(`_`)
		buf.WriteString(n)
	}
	return buf.String()
}

// MaybeUpgradeFormatVersion transforms the TableDescriptor to the latest
// FormatVersion (if it's not already there) and returns true if any changes
// were made.
func (desc *TableDescriptor) MaybeUpgradeFormatVersion() bool {
	if desc.FormatVersion >= FamilyFormatVersion {
		return false
	}

	primaryIndexColumnIds := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColumnIds[colID] = struct{}{}
	}

	desc.Families = []ColumnFamilyDescriptor{
		{ID: 0, Name: "primary"},
	}
	desc.NextFamilyID = desc.Families[0].ID + 1
	addFamilyForCol := func(col ColumnDescriptor) {
		if _, ok := primaryIndexColumnIds[col.ID]; ok {
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		colNames := []string{col.Name}
		family := ColumnFamilyDescriptor{
			ID:              FamilyID(col.ID),
			Name:            generatedFamilyName(FamilyID(col.ID), colNames),
			ColumnNames:     colNames,
			ColumnIDs:       []ColumnID{col.ID},
			DefaultColumnID: col.ID,
		}
		desc.Families = append(desc.Families, family)
		if family.ID >= desc.NextFamilyID {
			desc.NextFamilyID = family.ID + 1
		}
	}

	for _, c := range desc.Columns {
		addFamilyForCol(c)
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			addFamilyForCol(*c)
		}
	}

	desc.FormatVersion = FamilyFormatVersion

	return true
}

// AllocateIDs allocates column, family, and index ids for any column, family,
// or index which has an ID of 0.
func (desc *TableDescriptor) AllocateIDs() error {
	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
	}
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == InvalidMutationID {
		desc.NextMutationID = 1
	}

	columnNames := map[string]ColumnID{}
	fillColumnID := func(c *ColumnDescriptor) {
		columnID := c.ID
		if columnID == 0 {
			columnID = desc.NextColumnID
			desc.NextColumnID++
		}
		columnNames[NormalizeName(c.Name)] = columnID
		c.ID = columnID
	}
	for i := range desc.Columns {
		fillColumnID(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			fillColumnID(c)
		}
	}

	// Keep track of unnamed indexes.
	anonymousIndexes := make([]*IndexDescriptor, 0, len(desc.Indexes)+len(desc.Mutations))

	// Create a slice of modifiable index descriptors.
	indexes := make([]*IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	indexes = append(indexes, &desc.PrimaryIndex)
	collectIndexes := func(index *IndexDescriptor) {
		if len(index.Name) == 0 {
			anonymousIndexes = append(anonymousIndexes, index)
		}
		indexes = append(indexes, index)
	}
	for i := range desc.Indexes {
		collectIndexes(&desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if index := m.GetIndex(); index != nil {
			collectIndexes(index)
		}
	}

	for _, index := range anonymousIndexes {
		index.allocateName(desc)
	}

	// Populate IDs.
	for _, index := range indexes {
		if index.ID == 0 {
			index.ID = desc.NextIndexID
			desc.NextIndexID++
		}
		for j, colName := range index.ColumnNames {
			if len(index.ColumnIDs) <= j {
				index.ColumnIDs = append(index.ColumnIDs, 0)
			}
			if index.ColumnIDs[j] == 0 {
				index.ColumnIDs[j] = columnNames[NormalizeName(colName)]
			}
		}
		if index != &desc.PrimaryIndex {
			// Need to clear ImplicitColumnIDs because it is used by
			// ContainsColumnID.
			index.ImplicitColumnIDs = nil
			var implicitColumnIDs []ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.ContainsColumnID(primaryColID) {
					implicitColumnIDs = append(implicitColumnIDs, primaryColID)
				}
			}
			index.ImplicitColumnIDs = implicitColumnIDs

			for _, colName := range index.StoreColumnNames {
				status, i, err := desc.FindColumnByName(colName)
				if err != nil {
					return err
				}
				var col *ColumnDescriptor
				if status == DescriptorActive {
					col = &desc.Columns[i]
				} else {
					col = desc.Mutations[i].GetColumn()
				}
				if desc.PrimaryIndex.ContainsColumnID(col.ID) {
					continue
				}
				if index.ContainsColumnID(col.ID) {
					return fmt.Errorf("index \"%s\" already contains column \"%s\"", index.Name, col.Name)
				}
				index.ImplicitColumnIDs = append(index.ImplicitColumnIDs, col.ID)
			}
		}
	}

	primaryIndexColIDs := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs[colID] = struct{}{}
	}

	columnsInFamilies := make(map[ColumnID]struct{}, len(desc.Columns))
	for i, family := range desc.Families {
		if family.ID == 0 && i != 0 {
			family.ID = desc.NextFamilyID
			desc.NextFamilyID++
		}

		for j, colName := range family.ColumnNames {
			if len(family.ColumnIDs) <= j {
				family.ColumnIDs = append(family.ColumnIDs, 0)
			}
			if family.ColumnIDs[j] == 0 {
				family.ColumnIDs[j] = columnNames[NormalizeName(colName)]
			}
			columnsInFamilies[family.ColumnIDs[j]] = struct{}{}
		}

		desc.Families[i] = family
	}

	ensureColumnInFamily := func(col *ColumnDescriptor) {
		if _, ok := columnsInFamilies[col.ID]; ok {
			return
		}
		if _, ok := primaryIndexColIDs[col.ID]; ok {
			// Primary index columns are required to be assigned to family 0.
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		var familyID FamilyID
		if desc.ParentID == keys.SystemDatabaseID {
			// TODO(dan): This assigns families such that the encoding is exactly the
			// same as before column families. It's used for all system tables because
			// reads of them don't go through the normal sql layer, which is where the
			// knowledge of families lives. Fix that and remove this workaround.
			familyID = FamilyID(col.ID)
			desc.Families = append(desc.Families, ColumnFamilyDescriptor{
				ID:          familyID,
				ColumnNames: []string{col.Name},
				ColumnIDs:   []ColumnID{col.ID},
			})
		} else {
			idx, ok := fitColumnToFamily(*desc, *col)
			if !ok {
				idx = len(desc.Families)
				desc.Families = append(desc.Families, ColumnFamilyDescriptor{
					ID:          desc.NextFamilyID,
					ColumnNames: []string{},
					ColumnIDs:   []ColumnID{},
				})
			}
			familyID = desc.Families[idx].ID
			desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col.Name)
			desc.Families[idx].ColumnIDs = append(desc.Families[idx].ColumnIDs, col.ID)
		}
		if familyID >= desc.NextFamilyID {
			desc.NextFamilyID = familyID + 1
		}
	}
	for i := range desc.Columns {
		ensureColumnInFamily(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			ensureColumnInFamily(c)
		}
	}

	for i, family := range desc.Families {
		if len(family.Name) == 0 {
			family.Name = generatedFamilyName(family.ID, family.ColumnNames)
		}

		if family.DefaultColumnID == 0 {
			defaultColumnID := ColumnID(0)
			for _, colID := range family.ColumnIDs {
				if _, ok := primaryIndexColIDs[colID]; !ok {
					if defaultColumnID == 0 {
						defaultColumnID = colID
					} else {
						defaultColumnID = ColumnID(0)
						break
					}
				}
			}
			family.DefaultColumnID = defaultColumnID
		}

		desc.Families[i] = family
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MaxReservedDescID + 1
	}
	err := desc.ValidateTable()
	desc.ID = savedID
	return err
}

// Validate validates that the table descriptor is well formed. Checks include
// both single table and cross table invariants.
func (desc *TableDescriptor) Validate(txn *client.Txn) error {
	err := desc.ValidateTable()
	if err != nil {
		return err
	}
	return desc.validateCrossReferences(txn)
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *TableDescriptor) validateCrossReferences(txn *client.Txn) error {
	tablesByID := map[ID]*TableDescriptor{desc.ID: desc}
	getTable := func(id ID) (*TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := GetTableDescFromID(txn, id)
		if err != nil {
			return nil, err
		}
		tablesByID[id] = table
		return table, nil
	}

	findTargetIndex := func(tableID ID, indexID IndexID) (*TableDescriptor, *IndexDescriptor, error) {
		targetTable, err := getTable(tableID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "missing table=%d index=%d", tableID, indexID)
		}
		targetIndex, err := targetTable.FindIndexByID(indexID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "missing table=%s index=%d", targetTable.Name, indexID)
		}
		return targetTable, targetIndex, nil
	}

	for _, index := range desc.AllNonDropIndexes() {
		// Check foreign keys.
		if index.ForeignKey.IsSet() {
			targetTable, targetIndex, err := findTargetIndex(
				index.ForeignKey.Table, index.ForeignKey.Index)
			if err != nil {
				return errors.Wrap(err, "invalid foreign key")
			}
			found := false
			for _, backref := range targetIndex.ReferencedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.Errorf("missing fk back reference to %s.%s from %s.%s",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		fkBackrefs := make(map[ForeignKeyReference]struct{})
		for _, backref := range index.ReferencedBy {
			if _, ok := fkBackrefs[backref]; ok {
				return errors.Errorf("duplicated fk backreference %+v", backref)
			}
			fkBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err, "invalid fk backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return errors.Wrapf(err, "invalid fk backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if fk := targetIndex.ForeignKey; fk.Table != desc.ID || fk.Index != index.ID {
				return errors.Errorf("broken fk backward reference from %s.%s to %s.%s",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}

		// Check interleaves.
		if len(index.Interleave.Ancestors) > 0 {
			// Only check the most recent ancestor, the rest of them don't point
			// back.
			ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
			targetTable, targetIndex, err := findTargetIndex(ancestor.TableID, ancestor.IndexID)
			if err != nil {
				return errors.Wrap(err, "invalid interleave")
			}
			found := false
			for _, backref := range targetIndex.InterleavedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.Errorf(
					"missing interleave back reference to %s.%s from %s.%s",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		interleaveBackrefs := make(map[ForeignKeyReference]struct{})
		for _, backref := range index.InterleavedBy {
			if _, ok := interleaveBackrefs[backref]; ok {
				return errors.Errorf("duplicated interleave backreference %+v", backref)
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if len(targetIndex.Interleave.Ancestors) == 0 {
				return errors.Errorf(
					"broken interleave backward reference from %s.%s to %s.%s",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
			// The last ancestor is required to be a backreference.
			ancestor := targetIndex.Interleave.Ancestors[len(targetIndex.Interleave.Ancestors)-1]
			if ancestor.TableID != desc.ID || ancestor.IndexID != index.ID {
				return errors.Errorf(
					"broken interleave backward reference from %s.%s to %s.%s",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.
	return nil
}

// ValidateTable validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
func (desc *TableDescriptor) ValidateTable() error {
	if err := validateName(desc.Name, "table"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid table ID %d", desc.ID)
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.ParentID == 0 {
		return fmt.Errorf("invalid parent ID %d", desc.ParentID)
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// MaybeUpgradeFormatVersion missing from some codepath.
	if v := desc.GetFormatVersion(); v != FamilyFormatVersion && v != InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change MaybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		return fmt.Errorf(
			"table %q is encoded using using version %d, but this client only supports version %d and %d",
			desc.Name, desc.GetFormatVersion(), FamilyFormatVersion, InterleavedFormatVersion)
	}

	if len(desc.Columns) == 0 {
		return ErrMissingColumns
	}

	constraintNames := make(map[string]struct{}, len(desc.Columns))
	uniqConstraint := func(s string) error {
		s = strings.ToLower(s)
		if s != "" {
			if _, ok := constraintNames[s]; ok {
				return fmt.Errorf("duplicate constraint name: %q", s)
			}
			constraintNames[s] = struct{}{}
		}
		return nil
	}

	for _, check := range desc.Checks {
		if err := uniqConstraint(check.Name); err != nil {
			return err
		}
	}

	columnNames := make(map[string]ColumnID, len(desc.Columns))
	columnIDs := make(map[ColumnID]string, len(desc.Columns))
	for _, column := range desc.allNonDropColumns() {
		if err := validateName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return fmt.Errorf("invalid column ID %d", column.ID)
		}

		normName := NormalizeName(column.Name)
		if _, ok := columnNames[normName]; ok {
			return fmt.Errorf("duplicate column name: \"%s\"", column.Name)
		}
		columnNames[normName] = column.ID

		if other, ok := columnIDs[column.ID]; ok {
			return fmt.Errorf("column \"%s\" duplicate ID of column \"%s\": %d",
				column.Name, other, column.ID)
		}
		columnIDs[column.ID] = column.Name

		if column.ID >= desc.NextColumnID {
			return fmt.Errorf("column \"%s\" invalid ID (%d) > next column ID (%d)",
				column.Name, column.ID, desc.NextColumnID)
		}
		if err := uniqConstraint(column.DefaultExprConstraintName); err != nil {
			return err
		}
		if err := uniqConstraint(column.NullableConstraintName); err != nil {
			return err
		}
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == DescriptorMutation_UNKNOWN || m.Direction == DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			col := desc.Column
			if unSetEnums {
				return errors.Errorf("mutation in state %s, direction %s, col %s, id %v", m.State, m.Direction, col.Name, col.ID)
			}
			columnIDs[col.ID] = col.Name
		case *DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return errors.Errorf("mutation in state %s, direction %s, index %s, id %v", m.State, m.Direction, idx.Name, idx.ID)
			}
		default:
			return errors.Errorf("mutation in state %s, direction %s, and no column/index descriptor", m.State, m.Direction)
		}
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	if len(desc.Families) < 1 {
		return fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != FamilyID(0) {
		return fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[FamilyID]string{}
	colIDToFamilyID := map[ColumnID]FamilyID{}
	for _, family := range desc.Families {
		if err := validateName(family.Name, "family"); err != nil {
			return err
		}

		normName := NormalizeName(family.Name)
		if _, ok := familyNames[normName]; ok {
			return fmt.Errorf("duplicate family name: \"%s\"", family.Name)
		}
		familyNames[normName] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return fmt.Errorf("family \"%s\" duplicate ID of family \"%s\": %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return fmt.Errorf("family \"%s\" invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return fmt.Errorf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			name, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf("family \"%s\" contains unknown column \"%d\"", family.Name, colID)
			}
			if NormalizeName(name) != NormalizeName(family.ColumnNames[i]) {
				return fmt.Errorf("family \"%s\" column %d should have name %q, but found name %q",
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

	// TODO(pmattis): Check that the indexes are unique. That is, no 2 indexes
	// should contain identical sets of columns.
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[IndexID]string{}
	for _, index := range desc.AllNonDropIndexes() {
		if err := validateName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID %d", index.ID)
		}

		normName := NormalizeName(index.Name)
		if _, ok := indexNames[normName]; ok {
			return fmt.Errorf("duplicate index name: \"%s\"", index.Name)
		}
		indexNames[normName] = struct{}{}

		if index.Unique {
			// TODO(dt): Should probably add a separate constraintName to the idx.
			if err := uniqConstraint(index.Name); err != nil {
				return err
			}
		}

		if index.ForeignKey.IsSet() {
			if err := uniqConstraint(index.ForeignKey.Name); err != nil {
				return err
			}
		}

		if other, ok := indexIDs[index.ID]; ok {
			return fmt.Errorf("index \"%s\" duplicate ID of index \"%s\": %d",
				index.Name, other, index.ID)
		}
		indexIDs[index.ID] = index.Name

		if index.ID >= desc.NextIndexID {
			return fmt.Errorf("index \"%s\" invalid index ID (%d) > next index ID (%d)",
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
			return fmt.Errorf("index \"%s\" must contain at least 1 column", index.Name)
		}

		for i, name := range index.ColumnNames {
			colID, ok := columnNames[NormalizeName(name)]
			if !ok {
				return fmt.Errorf("index \"%s\" contains unknown column \"%s\"", index.Name, name)
			}
			if colID != index.ColumnIDs[i] {
				return fmt.Errorf("index \"%s\" column \"%s\" should have ID %d, but found ID %d",
					index.Name, name, colID, index.ColumnIDs[i])
			}
		}
	}

	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		famID, ok := colIDToFamilyID[colID]
		if !ok || famID != FamilyID(0) {
			return fmt.Errorf("primary key column %d is not in column family 0", colID)
		}
	}

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// FamilyHeuristicTargetBytes is the target total byte size of columns that the
// current heuristic will assign to a family.
const FamilyHeuristicTargetBytes = 256

// upperBoundColumnValueEncodedSize returns the maximum encoded size of the
// given column using the "value" encoding. If the size is unbounded, false is returned.
func upperBoundColumnValueEncodedSize(col ColumnDescriptor) (int, bool) {
	var typ encoding.Type
	var size int
	switch col.Type.Kind {
	case ColumnType_BOOL:
		typ = encoding.True
	case ColumnType_INT, ColumnType_DATE, ColumnType_TIMESTAMP, ColumnType_TIMESTAMPTZ:
		typ, size = encoding.Int, int(col.Type.Width)
	case ColumnType_FLOAT:
		typ = encoding.Float
	case ColumnType_INTERVAL:
		typ = encoding.Duration
	case ColumnType_STRING, ColumnType_BYTES:
		// STRINGs are counted as runes, so this isn't totally correct, but this
		// seems better than always assuming the maximum rune width.
		typ, size = encoding.Bytes, int(col.Type.Width)
	case ColumnType_DECIMAL:
		typ, size = encoding.Decimal, int(col.Type.Precision)
	default:
		panic(errors.Errorf("unknown column type: %s", col.Type.Kind))
	}
	return encoding.UpperBoundValueEncodingSize(uint32(col.ID), typ, size)
}

// fitColumnToFamily attempts to fit a new column into the existing column
// families. If the heuristics find a fit, true is returned along with the
// index of the selected family. Otherwise, false is returned and the column
// should be put in a new family.
//
// Current heuristics:
// - If the column is unbounded size (bytes, string, decimal) put it in a new
//   family.
// - If the column is bounded size (int, float, duration, date, timestamp, bool,
//   or user bounded string/decimal), find the first family where the storage
//   size of the existing columns plus the new column is not greater than
//   FamilyHeuristicTargetBytes. The maximum size of the value encoding is used
//   as the size of columns.
// - Otherwise, the column doesn't fit in any existing family, spill it over
//   into a new family.
//
// TODO(dan): Calling this function repeatedly to add columns is N^2. It
// shouldn't be a problem because the number of columns is small, but if it
// becomes an issue, make the bookkeeping of the columnSizesByID incremental.
func fitColumnToFamily(desc TableDescriptor, col ColumnDescriptor) (int, bool) {
	size, isBounded := upperBoundColumnValueEncodedSize(col)
	if size > FamilyHeuristicTargetBytes {
		return 0, false
	}

	primaryIndexColIDs := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs[colID] = struct{}{}
	}

	columnSizesByID := make(map[ColumnID]int, len(desc.Columns))
	for _, c := range desc.Columns {
		if _, ok := primaryIndexColIDs[c.ID]; ok {
			// Primary key columns are stored in the key, so they don't count
			// against the heuristic limit.
			columnSizesByID[c.ID] = 0
			continue
		}
		var bounded bool
		if columnSizesByID[c.ID], bounded = upperBoundColumnValueEncodedSize(c); !bounded {
			// Not bounded in size, so exceed the heuristic max to avoid assigning to
			// a family that this column is in.
			columnSizesByID[c.ID] = FamilyHeuristicTargetBytes + 1
		}
	}

	// TODO(dan): This naively places columns in the first family they'll fit in,
	// which is likely to lead to fragmentation. Consider something smarter like
	// picking the most full family that will fit the column.
	for i, family := range desc.Families {
		var familySize int
		for _, colID := range family.ColumnIDs {
			familySize += columnSizesByID[colID]
			if familySize > FamilyHeuristicTargetBytes {
				break
			}
		}
		if familySize == 0 || (isBounded && familySize+size <= FamilyHeuristicTargetBytes) {
			return i, true
		}
	}
	return 0, false
}

// AddColumn adds a column to the table.
func (desc *TableDescriptor) AddColumn(col ColumnDescriptor) {
	desc.Columns = append(desc.Columns, col)
}

// AddFamily adds a family to the table.
func (desc *TableDescriptor) AddFamily(fam ColumnFamilyDescriptor) {
	desc.Families = append(desc.Families, fam)
}

// AddIndex adds an index to the table.
func (desc *TableDescriptor) AddIndex(idx IndexDescriptor, primary bool) error {
	if primary {
		// PrimaryIndex is unset.
		if desc.PrimaryIndex.Name == "" {
			if idx.Name == "" {
				// Only override the index name if it hasn't been set by the user.
				idx.Name = PrimaryKeyIndexName
			}
			desc.PrimaryIndex = idx
		} else {
			return fmt.Errorf("multiple primary keys for table %q are not allowed", desc.Name)
		}
	} else {
		desc.Indexes = append(desc.Indexes, idx)
	}
	return nil
}

// AddColumnToFamilyMaybeCreate adds the specified column to the specified
// family. If it doesn't exist and create is true, creates it. If it does exist
// adds it unless "strict" create (`true` for create but `false` for
// ifNotExists) is specified.
//
// AllocateIDs must be called before the TableDesciptor will be valid.
func (desc *TableDescriptor) AddColumnToFamilyMaybeCreate(
	col string, family string, create bool, ifNotExists bool,
) error {
	idx := int(-1)
	if len(family) > 0 {
		normName := NormalizeName(family)
		for i := range desc.Families {
			if NormalizeName(desc.Families[i].Name) == normName {
				idx = i
				break
			}
		}
	}

	if idx == -1 {
		if create {
			// NB: When AllocateIDs encounters an empty `Name`, it'll generate one.
			desc.AddFamily(ColumnFamilyDescriptor{Name: family, ColumnNames: []string{col}})
			return nil
		}
		return fmt.Errorf("unknown family %q", family)
	}

	if create && !ifNotExists {
		return fmt.Errorf("family %q already exists", family)
	}
	desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col)
	return nil
}

// RemoveColumnFromFamily removes a colID from the family it's assigned to.
func (desc *TableDescriptor) RemoveColumnFromFamily(colID ColumnID) {
	for i, family := range desc.Families {
		for j, c := range family.ColumnIDs {
			if c == colID {
				desc.Families[i].ColumnIDs = append(
					desc.Families[i].ColumnIDs[:j], desc.Families[i].ColumnIDs[j+1:]...)
				desc.Families[i].ColumnNames = append(
					desc.Families[i].ColumnNames[:j], desc.Families[i].ColumnNames[j+1:]...)
				if len(desc.Families[i].ColumnIDs) == 0 {
					desc.Families = append(desc.Families[:i], desc.Families[i+1:]...)
				}
				return
			}
		}
	}
}

// RenameColumn updates all references to a column name in indexes and families.
func (desc *TableDescriptor) RenameColumn(colID ColumnID, newColName string) {
	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	renameColumnInIndex := func(idx *IndexDescriptor) {
		for i, id := range idx.ColumnIDs {
			if id == colID {
				idx.ColumnNames[i] = newColName
			}
		}
	}
	for i := range desc.Indexes {
		renameColumnInIndex(&desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			renameColumnInIndex(idx)
		}
	}
}

// FindActiveColumnsByNames finds all requested columns (in the requested order)
// or returns an error.
func (desc *TableDescriptor) FindActiveColumnsByNames(names parser.NameList) ([]ColumnDescriptor, error) {
	cols := make([]ColumnDescriptor, len(names))
	for i := range names {
		c, err := desc.FindActiveColumnByName(names[i])
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}
	return cols, nil
}

// FindColumnByName finds the column with the specified name. It returns
// DescriptorStatus for the column, and an index into either the columns
// (status == DescriptorActive) or mutations (status == DescriptorIncomplete).
func (desc *TableDescriptor) FindColumnByName(name string) (DescriptorStatus, int, error) {
	normName := NormalizeName(name)
	for i, c := range desc.Columns {
		if NormalizeName(c.Name) == normName {
			return DescriptorActive, i, nil
		}
	}
	for i, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if NormalizeName(c.Name) == normName {
				return DescriptorIncomplete, i, nil
			}
		}
	}
	return DescriptorAbsent, -1, fmt.Errorf("column %q does not exist", name)
}

// FindActiveColumnByName finds an active column with the specified name.
func (desc *TableDescriptor) FindActiveColumnByName(name string) (ColumnDescriptor, error) {
	normName := NormalizeName(name)
	for _, c := range desc.Columns {
		if NormalizeName(c.Name) == normName {
			return c, nil
		}
	}
	return ColumnDescriptor{}, fmt.Errorf("column %q does not exist", name)
}

// FindColumnByID finds the column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if c.ID == id {
				return c, nil
			}
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindActiveColumnByID finds the active column with specified ID.
func (desc *TableDescriptor) FindActiveColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindFamilyByID finds the family with specified ID.
func (desc *TableDescriptor) FindFamilyByID(id FamilyID) (*ColumnFamilyDescriptor, error) {
	for i, f := range desc.Families {
		if f.ID == id {
			return &desc.Families[i], nil
		}
	}
	return nil, fmt.Errorf("family-id \"%d\" does not exist", id)
}

// FindIndexByName finds the index with the specified name. It returns
// DescriptorStatus for the index, and an index into either the indexes
// (status == DescriptorActive) or mutations (status == DescriptorIncomplete).
func (desc *TableDescriptor) FindIndexByName(name string) (DescriptorStatus, int, error) {
	normName := NormalizeName(name)
	for i, idx := range desc.Indexes {
		if NormalizeName(idx.Name) == normName {
			return DescriptorActive, i, nil
		}
	}
	for i, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if NormalizeName(idx.Name) == normName {
				return DescriptorIncomplete, i, nil
			}
		}
	}
	return DescriptorAbsent, -1, fmt.Errorf("index %q does not exist", name)
}

// FindIndexByID finds an index (active or inactive) with the specified ID.
// Must return a pointer to the IndexDescriptor in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *TableDescriptor) FindIndexByID(id IndexID) (*IndexDescriptor, error) {
	if desc.PrimaryIndex.ID == id {
		return &desc.PrimaryIndex, nil
	}
	for i, c := range desc.Indexes {
		if c.ID == id {
			return &desc.Indexes[i], nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.ID == id {
			return idx, nil
		}
	}
	return nil, fmt.Errorf("index-id \"%d\" does not exist", id)
}

// IsInterleaved returns true if any part of this this table is interleaved with
// another table's data.
func (desc *TableDescriptor) IsInterleaved() bool {
	for _, index := range desc.AllNonDropIndexes() {
		if len(index.Interleave.Ancestors) > 0 {
			return true
		}
		if len(index.InterleavedBy) > 0 {
			return true
		}
	}
	return false
}

// MakeMutationComplete updates the descriptor upon completion of a mutation.
func (desc *TableDescriptor) MakeMutationComplete(m DescriptorMutation) {
	switch m.Direction {
	case DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			desc.AddColumn(*t.Column)

		case *DescriptorMutation_Index:
			if err := desc.AddIndex(*t.Index, false); err != nil {
				panic(err)
			}
		}

	case DescriptorMutation_DROP:
		switch t := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			desc.RemoveColumnFromFamily(t.Column.ID)
		}
		// Nothing else to be done. The column/index was already removed from the
		// set of column/index descriptors at mutation creation time.
	}
}

// AddColumnMutation adds a column mutation to desc.Mutations.
func (desc *TableDescriptor) AddColumnMutation(c ColumnDescriptor, direction DescriptorMutation_Direction) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Column{Column: &c}, Direction: direction}
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *TableDescriptor) AddIndexMutation(idx IndexDescriptor, direction DescriptorMutation_Direction) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Index{Index: &idx}, Direction: direction}
	desc.addMutation(m)
}

func (desc *TableDescriptor) addMutation(m DescriptorMutation) {
	switch m.Direction {
	case DescriptorMutation_ADD:
		m.State = DescriptorMutation_DELETE_ONLY

	case DescriptorMutation_DROP:
		m.State = DescriptorMutation_WRITE_ONLY
	}
	m.MutationID = desc.NextMutationID
	desc.Mutations = append(desc.Mutations, m)
}

// FinalizeMutation returns the id that has been used by mutations appended
// with addMutation() since the last time this function was called.
// Future mutations will use a new ID.
func (desc *TableDescriptor) FinalizeMutation() (MutationID, error) {
	if err := desc.SetUpVersion(); err != nil {
		return InvalidMutationID, err
	}
	mutationID := desc.NextMutationID
	desc.NextMutationID++
	return mutationID, nil
}

// Deleted returns true if the table is being deleted.
func (desc *TableDescriptor) Deleted() bool {
	return desc.State == TableDescriptor_DROP
}

// Renamed returns true if the table is being renamed.
func (desc *TableDescriptor) Renamed() bool {
	return len(desc.Renames) > 0
}

// SetUpVersion sets the up_version marker on the table descriptor (see the proto
func (desc *TableDescriptor) SetUpVersion() error {
	if desc.Deleted() {
		// We don't allow the version to be incremented any more once a table
		// has been deleted. This will block new mutations from being queued on the
		// table; it'd be misleading to allow them to be queued, since the
		// respective schema change will never run.
		return fmt.Errorf("table %q has been deleted", desc.Name)
	}
	desc.UpVersion = true
	return nil
}

// VisibleColumns returns all non hidden columns.
func (desc *TableDescriptor) VisibleColumns() []ColumnDescriptor {
	var cols []ColumnDescriptor
	for _, col := range desc.Columns {
		if !col.Hidden {
			cols = append(cols, col)
		}
	}
	return cols
}

// ColumnsSelectors generates Select expressions for cols.
func ColumnsSelectors(cols []ColumnDescriptor) parser.SelectExprs {
	exprs := make(parser.SelectExprs, len(cols))
	qnames := make([]parser.QualifiedName, len(cols))
	for i, col := range cols {
		qnames[i].Base = parser.Name(col.Name)
		exprs[i].Expr = &qnames[i]
	}
	return exprs
}

// IsEmpty checks if the descriptor is uninitialized.
func (desc *TableDescriptor) IsEmpty() bool {
	// Valid tables cannot have an ID of 0.
	return desc.ID == 0
}

// SQLString returns the SQL string corresponding to the type.
func (c *ColumnType) SQLString() string {
	switch c.Kind {
	case ColumnType_INT, ColumnType_STRING:
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d)", c.Kind.String(), c.Width)
		}
	case ColumnType_FLOAT:
		if c.Precision > 0 {
			return fmt.Sprintf("%s(%d)", c.Kind.String(), c.Precision)
		}
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			if c.Width > 0 {
				return fmt.Sprintf("%s(%d,%d)", c.Kind.String(), c.Precision, c.Width)
			}
			return fmt.Sprintf("%s(%d)", c.Kind.String(), c.Precision)
		}
	case ColumnType_TIMESTAMPTZ:
		return "TIMESTAMP WITH TIME ZONE"
	}
	return c.Kind.String()
}

// ToDatumType converts the ColumnType_Kind to the correct type Datum, or
// nil if there is no correspondence.
func (k ColumnType_Kind) ToDatumType() parser.Datum {
	switch k {
	case ColumnType_BOOL:
		return parser.TypeBool
	case ColumnType_INT:
		return parser.TypeInt
	case ColumnType_FLOAT:
		return parser.TypeFloat
	case ColumnType_DECIMAL:
		return parser.TypeDecimal
	case ColumnType_STRING:
		return parser.TypeString
	case ColumnType_BYTES:
		return parser.TypeBytes
	case ColumnType_DATE:
		return parser.TypeDate
	case ColumnType_TIMESTAMP:
		return parser.TypeTimestamp
	case ColumnType_TIMESTAMPTZ:
		return parser.TypeTimestampTZ
	case ColumnType_INTERVAL:
		return parser.TypeInterval
	}
	return nil
}

// ToDatumType converts the ColumnType to the correct type Datum, or
// nil if there is no correspondence.
func (c *ColumnType) ToDatumType() parser.Datum {
	return c.Kind.ToDatumType()
}

// SetID implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// SetName implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetName(name string) {
	desc.Name = name
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *DatabaseDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid database ID %d", desc.ID)
	}
	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// GetID returns the ID of the descriptor.
func (desc *Descriptor) GetID() ID {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.ID
	case *Descriptor_Database:
		return t.Database.ID
	default:
		return 0
	}
}

// GetName returns the Name of the descriptor.
func (desc *Descriptor) GetName() string {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.Name
	case *Descriptor_Database:
		return t.Database.Name
	default:
		return ""
	}
}

// IsSet returns whether or not the foreign key actually references a table.
func (f ForeignKeyReference) IsSet() bool {
	return f.Table != 0
}
