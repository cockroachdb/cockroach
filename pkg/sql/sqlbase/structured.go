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
	"unicode/utf8"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID parser.ID

// IDs is a sortable list of IDs.
type IDs []ID

func (ids IDs) Len() int           { return len(ids) }
func (ids IDs) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids IDs) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID parser.ColumnID

// FamilyID is a custom type for ColumnFamilyDescriptor IDs.
type FamilyID uint32

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID parser.IndexID

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

// GetDatabaseDescFromID retrieves the database descriptor for the database
// ID passed in using an existing txn. Returns an error if the descriptor
// doesn't exist or if it exists and is not a database.
func GetDatabaseDescFromID(
	ctx context.Context, txn *client.Txn, id ID,
) (*DatabaseDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	db := desc.GetDatabase()
	if db == nil {
		return nil, ErrDescriptorNotFound
	}
	return db, nil
}

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(ctx context.Context, txn *client.Txn, id ID) (*TableDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	table := desc.GetTable()
	if table == nil {
		return nil, ErrDescriptorNotFound
	}
	return table, nil
}

// RunOverAllColumns applies its argument fn to each of the column IDs in desc.
// If there is an error, that error is returned immediately.
func (desc *IndexDescriptor) RunOverAllColumns(fn func(id ColumnID) error) error {
	for _, colID := range desc.ColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.ExtraColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.StoreColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	return nil
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
		_, _, err := tableDesc.FindIndexByNormalizedName(name)
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

type returnTrue struct{}

func (returnTrue) Error() string { panic("unimplemented") }

var returnTruePseudoError error = returnTrue{}

// ContainsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs, the extra column IDs, or the
// stored column IDs.
func (desc *IndexDescriptor) ContainsColumnID(colID ColumnID) bool {
	return desc.RunOverAllColumns(func(id ColumnID) error {
		if id == colID {
			return returnTruePseudoError
		}
		return nil
	}) != nil
}

// FullColumnIDs returns the index column IDs including any extra (implicit or
// stored (old STORING encoding)) column IDs for non-unique indexes. It also
// returns the direction with which each column was encoded.
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
	columnIDs = append(columnIDs, desc.ExtraColumnIDs...)
	for range desc.ExtraColumnIDs {
		// Extra columns are encoded ascendingly.
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
	if desc.IsView() {
		return "view"
	}
	return "table"
}

// SetName implements the DescriptorProto interface.
func (desc *TableDescriptor) SetName(name string) {
	desc.Name = name
}

// IsEmpty checks if the descriptor is uninitialized.
func (desc *TableDescriptor) IsEmpty() bool {
	// Valid tables cannot have an ID of 0.
	return desc.ID == 0
}

// IsTable returns true if the TableDescriptor actually describes a
// Table resource, as opposed to a different resource (like a View).
func (desc *TableDescriptor) IsTable() bool {
	return !desc.IsView()
}

// IsView returns true if the TableDescriptor actually describes a
// View resource rather than a Table.
func (desc *TableDescriptor) IsView() bool {
	return desc.ViewQuery != ""
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the information_schema tables) and thus doesn't
// need to be physically stored.
func (desc *TableDescriptor) IsVirtualTable() bool {
	return desc.ID == keys.VirtualDescriptorID
}

// IsPhysicalTable returns true if the TableDescriptor actually describes a
// physical Table that needs to be stored in the kv layer, as opposed to a
// different resource like a view or a virtual table. Physical tables have
// primary keys, column families, and indexes (unlike virtual tables).
func (desc *TableDescriptor) IsPhysicalTable() bool {
	return desc.IsTable() && !desc.IsVirtualTable()
}

// KeysPerRow returns the maximum number of keys used to encode a row for the
// given index. For secondary indexes, we always only use one, but for primary
// indexes, we can encode up to one kv per column family.
func (desc *TableDescriptor) KeysPerRow(indexID IndexID) int {
	if desc.PrimaryIndex.ID == indexID {
		return len(desc.Families)
	}
	return 1
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
	if desc.IsPhysicalTable() {
		indexes = append(indexes, desc.PrimaryIndex)
	}
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

// ForeachNonDropIndex runs a function on all indexes, including those being
// added in the mutations.
func (desc *TableDescriptor) ForeachNonDropIndex(f func(*IndexDescriptor) error) error {
	if desc.IsPhysicalTable() {
		if err := f(&desc.PrimaryIndex); err != nil {
			return err
		}
	}
	for i := range desc.Indexes {
		if err := f(&desc.Indexes[i]); err != nil {
			return err
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && m.Direction == DescriptorMutation_ADD {
			if err := f(idx); err != nil {
				return err
			}
		}
	}
	return nil
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
	if desc.FormatVersion >= InterleavedFormatVersion {
		return false
	}
	desc.maybeUpgradeToFamilyFormatVersion()
	desc.FormatVersion = InterleavedFormatVersion
	return true
}

func (desc *TableDescriptor) maybeUpgradeToFamilyFormatVersion() bool {
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
	// Only physical tables can have / need a primary key.
	if desc.IsPhysicalTable() {
		if err := desc.ensurePrimaryKey(); err != nil {
			return err
		}
	}

	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
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
		columnNames[parser.ReNormalizeName(c.Name)] = columnID
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

	// Only physical tables can have / need indexes and column families.
	if desc.IsPhysicalTable() {
		if err := desc.allocateIndexIDs(columnNames); err != nil {
			return err
		}
		desc.allocateColumnFamilyIDs(columnNames)
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

func (desc *TableDescriptor) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		s := "unique_rowid()"
		col := ColumnDescriptor{
			Name: "rowid",
			Type: ColumnType{
				Kind: ColumnType_INT,
			},
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return err
		}
	}
	return nil
}

// HasCompositeKeyEncoding returns true if key columns of the given kind can
// have a composite encoding. For such types, it can be decided on a
// case-by-base basis whether a given Datum requires the composite encoding.
func HasCompositeKeyEncoding(kind ColumnType_Kind) bool {
	switch kind {
	case ColumnType_COLLATEDSTRING,
		ColumnType_FLOAT,
		ColumnType_DECIMAL:
		return true
	}
	return false
}

// HasOldStoredColumns returns whether the index has stored columns in the old
// format (data encoded the same way as if they were in an implicit column).
func (desc *IndexDescriptor) HasOldStoredColumns() bool {
	return len(desc.ExtraColumnIDs) > 0 && len(desc.StoreColumnIDs) < len(desc.StoreColumnNames)
}

func (desc *TableDescriptor) allocateIndexIDs(columnNames map[string]ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
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

	isCompositeColumn := make(map[ColumnID]struct{})
	for _, col := range desc.Columns {
		if HasCompositeKeyEncoding(col.Type.Kind) {
			isCompositeColumn[col.ID] = struct{}{}
		}
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
				index.ColumnIDs[j] = columnNames[parser.ReNormalizeName(colName)]
			}
		}

		if index != &desc.PrimaryIndex {
			indexHasOldStoredColumns := index.HasOldStoredColumns()
			// Need to clear ExtraColumnIDs and StoreColumnIDs because they are used
			// by ContainsColumnID.
			index.ExtraColumnIDs = nil
			index.StoreColumnIDs = nil
			var extraColumnIDs []ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.ContainsColumnID(primaryColID) {
					extraColumnIDs = append(extraColumnIDs, primaryColID)
				}
			}
			index.ExtraColumnIDs = extraColumnIDs

			for _, colName := range index.StoreColumnNames {
				status, i, err := desc.FindColumnByNormalizedName(parser.ReNormalizeName(colName))
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
				if indexHasOldStoredColumns {
					index.ExtraColumnIDs = append(index.ExtraColumnIDs, col.ID)
				} else {
					index.StoreColumnIDs = append(index.StoreColumnIDs, col.ID)
				}
			}
		}

		index.CompositeColumnIDs = nil
		for _, colID := range index.ColumnIDs {
			if _, ok := isCompositeColumn[colID]; ok {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
		for _, colID := range index.ExtraColumnIDs {
			if _, ok := isCompositeColumn[colID]; ok {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
	}
	return nil
}

func (desc *TableDescriptor) allocateColumnFamilyIDs(columnNames map[string]ColumnID) {
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
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
				family.ColumnIDs[j] = columnNames[parser.ReNormalizeName(colName)]
			}
			columnsInFamilies[family.ColumnIDs[j]] = struct{}{}
		}

		desc.Families[i] = family
	}

	primaryIndexColIDs := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs[colID] = struct{}{}
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
}

// Validate validates that the table descriptor is well formed. Checks include
// both single table and cross table invariants.
func (desc *TableDescriptor) Validate(ctx context.Context, txn *client.Txn) error {
	err := desc.ValidateTable()
	if err != nil {
		return err
	}
	if desc.Dropped() {
		return nil
	}
	return desc.validateCrossReferences(ctx, txn)
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *TableDescriptor) validateCrossReferences(ctx context.Context, txn *client.Txn) error {
	// Check that parent DB exists.
	{
		res, err := txn.Get(ctx, MakeDescMetadataKey(desc.ParentID))
		if err != nil {
			return err
		}
		if !res.Exists() {
			return errors.Errorf("parentID %d does not exist", desc.ParentID)
		}
	}

	tablesByID := map[ID]*TableDescriptor{desc.ID: desc}
	getTable := func(id ID) (*TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := GetTableDescFromID(ctx, txn, id)
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

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return nil
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

	if err := desc.CheckUniqueConstraints(); err != nil {
		return err
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

		normName := parser.ReNormalizeName(column.Name)
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

	// Only validate column families and indexes if this is actually a table, not
	// if it's just a view.
	if desc.IsPhysicalTable() {
		colIDToFamilyID, err := desc.validateColumnFamilies(columnIDs)
		if err != nil {
			return err
		}
		if err := desc.validateTableIndexes(columnNames, colIDToFamilyID); err != nil {
			return err
		}
	}

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

func (desc *TableDescriptor) validateColumnFamilies(
	columnIDs map[ColumnID]string,
) (map[ColumnID]FamilyID, error) {
	if len(desc.Families) < 1 {
		return nil, fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != FamilyID(0) {
		return nil, fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[FamilyID]string{}
	colIDToFamilyID := map[ColumnID]FamilyID{}
	for _, family := range desc.Families {
		if err := validateName(family.Name, "family"); err != nil {
			return nil, err
		}

		normName := parser.ReNormalizeName(family.Name)
		if _, ok := familyNames[normName]; ok {
			return nil, fmt.Errorf("duplicate family name: \"%s\"", family.Name)
		}
		familyNames[normName] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return nil, fmt.Errorf("family \"%s\" duplicate ID of family \"%s\": %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return nil, fmt.Errorf("family \"%s\" invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return nil, fmt.Errorf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			name, ok := columnIDs[colID]
			if !ok {
				return nil, fmt.Errorf("family \"%s\" contains unknown column \"%d\"", family.Name, colID)
			}
			if parser.ReNormalizeName(name) != parser.ReNormalizeName(family.ColumnNames[i]) {
				return nil, fmt.Errorf("family \"%s\" column %d should have name %q, but found name %q",
					family.Name, colID, name, family.ColumnNames[i])
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return nil, fmt.Errorf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID := range columnIDs {
		if _, ok := colIDToFamilyID[colID]; !ok {
			return nil, fmt.Errorf("column %d is not in any column family", colID)
		}
	}
	return colIDToFamilyID, nil
}

func (desc *TableDescriptor) validateTableIndexes(
	columnNames map[string]ColumnID, colIDToFamilyID map[ColumnID]FamilyID,
) error {
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

		normName := parser.ReNormalizeName(index.Name)
		if _, ok := indexNames[normName]; ok {
			return fmt.Errorf("duplicate index name: \"%s\"", index.Name)
		}
		indexNames[normName] = struct{}{}

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
			colID, ok := columnNames[parser.ReNormalizeName(name)]
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

	return nil
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
	case ColumnType_INT, ColumnType_DATE, ColumnType_TIMESTAMP,
		ColumnType_TIMESTAMPTZ, ColumnType_OID:
		typ, size = encoding.Int, int(col.Type.Width)
	case ColumnType_FLOAT:
		typ = encoding.Float
	case ColumnType_INTERVAL:
		typ = encoding.Duration
	case ColumnType_STRING, ColumnType_BYTES, ColumnType_COLLATEDSTRING, ColumnType_NAME:
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
// - Put all columns in family 0.
func fitColumnToFamily(desc TableDescriptor, col ColumnDescriptor) (int, bool) {
	// Fewer column families means fewer kv entries, which is generally faster.
	// On the other hand, an update to any column in a family requires that they
	// all are read and rewritten, so large (or numerous) columns that are not
	// updated at the same time as other columns in the family make things
	// slower.
	//
	// The initial heuristic used for family assignment tried to pack
	// fixed-width columns into families up to a certain size and would put any
	// variable-width column into its own family. This was conservative to
	// guarantee that we avoid the worst-case behavior of a very large immutable
	// blob in the same family as frequently updated columns.
	//
	// However, our initial customers have revealed that this is backward.
	// Repeatedly, they have recreated existing schemas without any tuning and
	// found lackluster performance. Each of these has turned out better as a
	// single family (sometimes 100% faster or more), the most aggressive tuning
	// possible.
	//
	// Further, as the WideTable benchmark shows, even the worst-case isn't that
	// bad (33% slower with an immutable 1MB blob, which is the upper limit of
	// what we'd recommend for column size regardless of families). This
	// situation also appears less frequent than we feared.
	//
	// The result is that we put all columns in one family and require the user
	// to manually specify family assignments when this is incorrect.
	return 0, true
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
		normName := parser.ReNormalizeName(family)
		for i := range desc.Families {
			if parser.ReNormalizeName(desc.Families[i].Name) == normName {
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

// RenameColumnNormalized updates all references to a column name in indexes and families.
func (desc *TableDescriptor) RenameColumnNormalized(colID ColumnID, newColName string) {
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
	renameColumnInIndex(&desc.PrimaryIndex)
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
func (desc *TableDescriptor) FindActiveColumnsByNames(
	names parser.NameList,
) ([]ColumnDescriptor, error) {
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

// FindColumnByNormalizedName finds the column with the specified name. It returns
// DescriptorStatus for the column, and an index into either the columns
// (status == DescriptorActive) or mutations (status == DescriptorIncomplete).
func (desc *TableDescriptor) FindColumnByNormalizedName(
	normName string,
) (DescriptorStatus, int, error) {
	for i, c := range desc.Columns {
		if parser.ReNormalizeName(c.Name) == normName {
			return DescriptorActive, i, nil
		}
	}
	for i, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if parser.ReNormalizeName(c.Name) == normName {
				return DescriptorIncomplete, i, nil
			}
		}
	}
	return DescriptorAbsent, -1, fmt.Errorf("column %q does not exist", normName)
}

// FindColumnByName calls FindColumnByNormalizedName with a normalized argument.
func (desc *TableDescriptor) FindColumnByName(name parser.Name) (DescriptorStatus, int, error) {
	return desc.FindColumnByNormalizedName(name.Normalize())
}

// FindActiveColumnByNormalizedName finds an active column with the specified normalized name.
func (desc *TableDescriptor) FindActiveColumnByNormalizedName(
	normName string,
) (ColumnDescriptor, error) {
	for _, c := range desc.Columns {
		if parser.ReNormalizeName(c.Name) == normName {
			return c, nil
		}
	}
	return ColumnDescriptor{}, fmt.Errorf("column %q does not exist", normName)
}

// FindActiveColumnByName calls FindActiveColumnByNormalizedName on a normalized argument.
func (desc *TableDescriptor) FindActiveColumnByName(name parser.Name) (ColumnDescriptor, error) {
	return desc.FindActiveColumnByNormalizedName(name.Normalize())
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

// FindIndexByNormalizedName finds the index with the specified name. It returns
// DescriptorStatus for the index, and an index into either the indexes
// (status == DescriptorActive) or mutations (status == DescriptorIncomplete).
func (desc *TableDescriptor) FindIndexByNormalizedName(
	normName string,
) (DescriptorStatus, int, error) {
	for i, idx := range desc.Indexes {
		if parser.ReNormalizeName(idx.Name) == normName {
			return DescriptorActive, i, nil
		}
	}
	for i, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if parser.ReNormalizeName(idx.Name) == normName {
				return DescriptorIncomplete, i, nil
			}
		}
	}
	return DescriptorAbsent, -1, fmt.Errorf("index %q does not exist", normName)
}

// FindIndexByName calls FindIndexByNormalizedName on a normalized argument.
func (desc *TableDescriptor) FindIndexByName(name parser.Name) (DescriptorStatus, int, error) {
	return desc.FindIndexByNormalizedName(name.Normalize())
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
func (desc *TableDescriptor) AddColumnMutation(
	c ColumnDescriptor, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Column{Column: &c}, Direction: direction}
	m.ResumeSpans = append(m.ResumeSpans, desc.PrimaryIndexSpan())
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *TableDescriptor) AddIndexMutation(
	idx IndexDescriptor, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Index{Index: &idx}, Direction: direction}
	m.ResumeSpans = append(m.ResumeSpans, desc.PrimaryIndexSpan())
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

// Dropped returns true if the table is being dropped.
func (desc *TableDescriptor) Dropped() bool {
	return desc.State == TableDescriptor_DROP
}

// Adding returns true if the table is being added.
func (desc *TableDescriptor) Adding() bool {
	return desc.State == TableDescriptor_ADD
}

// Renamed returns true if the table is being renamed.
func (desc *TableDescriptor) Renamed() bool {
	return len(desc.Renames) > 0
}

// SetUpVersion sets the up_version marker on the table descriptor (see the proto
func (desc *TableDescriptor) SetUpVersion() error {
	if desc.Dropped() {
		// We don't allow the version to be incremented any more once a table
		// has been deleted. This will block new mutations from being queued on the
		// table; it'd be misleading to allow them to be queued, since the
		// respective schema change will never run.
		return fmt.Errorf("table %q is being dropped", desc.Name)
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
	colItems := make([]parser.ColumnItem, len(cols))
	for i, col := range cols {
		colItems[i].ColumnName = parser.Name(col.Name)
		exprs[i].Expr = &colItems[i]
	}
	return exprs
}

// SQLString returns the SQL string corresponding to the type.
func (c *ColumnType) SQLString() string {
	switch c.Kind {
	case ColumnType_INT:
		if c.Width > 0 {
			// A non-zero width indicates a bit array. The syntax "INT(N)"
			// is invalid so be sure to use "BIT".
			return fmt.Sprintf("BIT(%d)", c.Width)
		}
	case ColumnType_STRING:
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
	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d) COLLATE %s", ColumnType_STRING.String(), c.Width, *c.Locale)
		}
		return fmt.Sprintf("%s COLLATE %s", ColumnType_STRING.String(), *c.Locale)
	case ColumnType_INT_ARRAY:
		return "INT[]"
	}
	return c.Kind.String()
}

// MaxCharacterLength returns the declared maximum length of characters if the
// ColumnType is a character or bit string data type. Returns false if the data
// type is not a character or bit string, or if the string's length is not bounded.
func (c *ColumnType) MaxCharacterLength() (int32, bool) {
	switch c.Kind {
	case ColumnType_INT, ColumnType_STRING, ColumnType_COLLATEDSTRING:
		if c.Width > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// MaxOctetLength returns the maximum the maximum possible length in octets of a
// datum if the ColumnType is a character string. Returns false if the data type
// is not a character string, or if the string's length is not bounded.
func (c *ColumnType) MaxOctetLength() (int32, bool) {
	switch c.Kind {
	case ColumnType_STRING, ColumnType_COLLATEDSTRING:
		if c.Width > 0 {
			return c.Width * utf8.UTFMax, true
		}
	}
	return 0, false
}

// NumericPrecision returns the declared or implicit precision of numeric
// data types. Returns false if the data type is not numeric, or if the precision
// of the numeric type is not bounded.
func (c *ColumnType) NumericPrecision() (int32, bool) {
	switch c.Kind {
	case ColumnType_INT:
		return 64, true
	case ColumnType_FLOAT:
		if c.Precision > 0 {
			return c.Precision, true
		}
		return 53, true
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			return c.Precision, true
		}
	}
	return 0, false
}

// NumericScale returns the declared or implicit precision of exact numeric
// data types. Returns false if the data type is not an exact numeric, or if the
// scale of the exact numeric type is not bounded.
func (c *ColumnType) NumericScale() (int32, bool) {
	switch c.Kind {
	case ColumnType_INT:
		return 0, true
	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// DatumTypeToColumnType converts a parser Type to a ColumnType.
func DatumTypeToColumnType(ptyp parser.Type) ColumnType {
	var ctyp ColumnType
	switch ptyp {
	case parser.TypeBool:
		ctyp.Kind = ColumnType_BOOL
	case parser.TypeInt:
		ctyp.Kind = ColumnType_INT
	case parser.TypeFloat:
		ctyp.Kind = ColumnType_FLOAT
	case parser.TypeDecimal:
		ctyp.Kind = ColumnType_DECIMAL
	case parser.TypeBytes:
		ctyp.Kind = ColumnType_BYTES
	case parser.TypeString:
		ctyp.Kind = ColumnType_STRING
	case parser.TypeName:
		ctyp.Kind = ColumnType_NAME
	case parser.TypeDate:
		ctyp.Kind = ColumnType_DATE
	case parser.TypeTimestamp:
		ctyp.Kind = ColumnType_TIMESTAMP
	case parser.TypeTimestampTZ:
		ctyp.Kind = ColumnType_TIMESTAMPTZ
	case parser.TypeInterval:
		ctyp.Kind = ColumnType_INTERVAL
	case parser.TypeOid:
		ctyp.Kind = ColumnType_OID
	case parser.TypeIntArray:
		ctyp.Kind = ColumnType_INT_ARRAY
	case parser.TypeIntVector:
		ctyp.Kind = ColumnType_INT2VECTOR
	default:
		if t, ok := ptyp.(parser.TCollatedString); ok {
			ctyp.Kind = ColumnType_COLLATEDSTRING
			ctyp.Locale = &t.Locale
		} else {
			panic(fmt.Sprintf("unsupported result type: %s", ptyp))
		}
	}
	return ctyp
}

// ToDatumType converts the ColumnType to the correct type, or nil if there is
// no correspondence.
func (c *ColumnType) ToDatumType() parser.Type {
	switch c.Kind {
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
	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		return parser.TCollatedString{Locale: *c.Locale}
	case ColumnType_NAME:
		return parser.TypeName
	case ColumnType_OID:
		return parser.TypeOid
	case ColumnType_INT_ARRAY:
		return parser.TypeIntArray
	case ColumnType_INT2VECTOR:
		return parser.TypeIntVector
	}
	return nil
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

// InvalidateFKConstraints sets all FK constraints to un-validated.
func (desc *TableDescriptor) InvalidateFKConstraints() {
	// We don't use GetConstraintInfo because we want to edit the passed desc.
	if desc.PrimaryIndex.ForeignKey.IsSet() {
		desc.PrimaryIndex.ForeignKey.Validity = ConstraintValidity_Unvalidated
	}
	for i := range desc.Indexes {
		if desc.Indexes[i].ForeignKey.IsSet() {
			desc.Indexes[i].ForeignKey.Validity = ConstraintValidity_Unvalidated
		}
	}
}

// AllIndexSpans returns the Spans for each index in the table, including those
// being added in the mutations.
func (desc *TableDescriptor) AllIndexSpans() roachpb.Spans {
	var spans roachpb.Spans
	err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		spans = append(spans, desc.IndexSpan(index.ID))
		return nil
	})
	if err != nil {
		panic(err)
	}
	return spans
}

// PrimaryIndexSpan returns the Span that corresponds to the entire primary
// index; can be used for a full table scan.
func (desc *TableDescriptor) PrimaryIndexSpan() roachpb.Span {
	return desc.IndexSpan(desc.PrimaryIndex.ID)
}

// IndexSpan returns the Span that corresponds to an entire index; can be used
// for a full index scan.
func (desc *TableDescriptor) IndexSpan(indexID IndexID) roachpb.Span {
	prefix := roachpb.Key(MakeIndexKeyPrefix(desc, indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan returns the Span that corresponds to the entire table.
func (desc *TableDescriptor) TableSpan() roachpb.Span {
	prefix := roachpb.Key(keys.MakeTablePrefix(uint32(desc.ID)))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// GetDescMetadataKey returns the descriptor key for the table.
func (desc TableDescriptor) GetDescMetadataKey() roachpb.Key {
	return MakeDescMetadataKey(desc.ID)
}

// GetNameMetadataKey returns the namespace key for the table.
func (desc TableDescriptor) GetNameMetadataKey() roachpb.Key {
	return MakeNameMetadataKey(desc.ParentID, desc.Name)
}
