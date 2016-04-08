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

package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// ID, ColumnID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID uint32

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID uint32

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
)

// MutationID is custom type for TableDescriptor mutations.
type MutationID uint32

const invalidMutationID MutationID = 0

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

var errMissingColumns = errors.New("table must contain at least 1 column")
var errMissingPrimaryKey = errors.New("table must contain a primary key")

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

// toEncodingDirection converts a direction from the proto to an encoding.Direction.
func (dir IndexDescriptor_Direction) toEncodingDirection() (encoding.Direction, error) {
	switch dir {
	case IndexDescriptor_ASC:
		return encoding.Ascending, nil
	case IndexDescriptor_DESC:
		return encoding.Descending, nil
	default:
		return encoding.Ascending, util.Errorf("invalid direction: %s", dir)
	}
}

// allocateName sets desc.Name to a value that is not equalName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func (desc *IndexDescriptor) allocateName(tableDesc *TableDescriptor) {
	segments := make([]string, 0, len(desc.ColumnIDs)+2)
	segments = append(segments, tableDesc.Name)
	for _, columnName := range desc.ColumnNames {
		segments = append(segments, columnName)
	}
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

// Fill in column names and directions.
func (desc *IndexDescriptor) fillColumns(elems parser.IndexElemList) error {
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

// containsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs or the implicit "extra" column
// IDs.
func (desc *IndexDescriptor) containsColumnID(colID ColumnID) bool {
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

// fullColumnIDs returns the index column IDs including any implicit column IDs
// for non-unique indexes. It also returns the direction with which each column
// was encoded.
func (desc *IndexDescriptor) fullColumnIDs() ([]ColumnID, []encoding.Direction) {
	dirs := make([]encoding.Direction, 0, len(desc.ColumnIDs))
	for _, dir := range desc.ColumnDirections {
		convertedDir, err := dir.toEncodingDirection()
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

// SetID implements the descriptorProto interface.
func (desc *TableDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *TableDescriptor) TypeName() string {
	return "table"
}

// SetName implements the descriptorProto interface.
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

// allNonDropIndexes returns all the indexes, including those being added
// in the mutations.
func (desc *TableDescriptor) allNonDropIndexes() []IndexDescriptor {
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

// AllocateIDs allocates column and index ids for any column or index which has
// an ID of 0.
func (desc *TableDescriptor) AllocateIDs() error {
	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == invalidMutationID {
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
			// containsColumnID.
			index.ImplicitColumnIDs = nil
			var implicitColumnIDs []ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.containsColumnID(primaryColID) {
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
				if desc.PrimaryIndex.containsColumnID(col.ID) {
					continue
				}
				if index.containsColumnID(col.ID) {
					return fmt.Errorf("index \"%s\" already contains column \"%s\"", index.Name, col.Name)
				}
				index.ImplicitColumnIDs = append(index.ImplicitColumnIDs, col.ID)
			}
		}
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MaxReservedDescID + 1
	}
	err := desc.Validate()
	desc.ID = savedID
	return err
}

// Validate validates that the table descriptor is well formed. Checks include
// validating the table, column and index names, verifying that column names
// and index names are unique and verifying that column IDs and index IDs are
// consistent.
func (desc *TableDescriptor) Validate() error {
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

	if desc.GetFormatVersion() != BaseFormatVersion {
		return fmt.Errorf(
			"table %q is encoded using using version %d, but this client only supports version %d",
			desc.Name, desc.GetFormatVersion(), BaseFormatVersion)
	}

	if len(desc.Columns) == 0 {
		return errMissingColumns
	}

	columnNames := map[string]ColumnID{}
	columnIDs := map[ColumnID]string{}
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
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == DescriptorMutation_UNKNOWN || m.Direction == DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			if unSetEnums {
				col := desc.Column
				return util.Errorf("mutation in state %s, direction %s, col %s, id %v", m.State, m.Direction, col.Name, col.ID)
			}
		case *DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return util.Errorf("mutation in state %s, direction %s, index %s, id %v", m.State, m.Direction, idx.Name, idx.ID)
			}
		default:
			return util.Errorf("mutation in state %s, direction %s, and no column/index descriptor", m.State, m.Direction)
		}
	}

	// TODO(pmattis): Check that the indexes are unique. That is, no 2 indexes
	// should contain identical sets of columns.
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return errMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[IndexID]string{}
	for _, index := range desc.allNonDropIndexes() {
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

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// AddColumn adds a column to the table.
func (desc *TableDescriptor) AddColumn(col ColumnDescriptor) {
	desc.Columns = append(desc.Columns, col)
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

// FindColumnByID finds the active column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
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

// FindIndexByID finds the active index with specified ID.
func (desc *TableDescriptor) FindIndexByID(id IndexID) (*IndexDescriptor, error) {
	indexes := append(desc.Indexes, desc.PrimaryIndex)

	for i, c := range indexes {
		if c.ID == id {
			return &indexes[i], nil
		}
	}
	return nil, fmt.Errorf("index-id \"%d\" does not exist", id)
}

func (desc *TableDescriptor) makeMutationComplete(m DescriptorMutation) {
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
		// Nothing to be done. The column/index was already
		// removed from the set of column/index descriptors
		// at mutation creation time.
	}
}

func (desc *TableDescriptor) addColumnMutation(c ColumnDescriptor, direction DescriptorMutation_Direction) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Column{Column: &c}, Direction: direction}
	desc.addMutation(m)
}

func (desc *TableDescriptor) addIndexMutation(idx IndexDescriptor, direction DescriptorMutation_Direction) {
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

func (desc *TableDescriptor) allColumnsSelector() parser.SelectExprs {
	exprs := make(parser.SelectExprs, len(desc.Columns))
	qnames := make([]parser.QualifiedName, len(desc.Columns))
	for i, col := range desc.Columns {
		qnames[i].Base = parser.Name(col.Name)
		exprs[i].Expr = &qnames[i]
	}
	return exprs
}

func (desc *TableDescriptor) isEmpty() bool {
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
	}
	return c.Kind.String()
}

// SetID implements the descriptorProto interface.
func (desc *DatabaseDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// SetName implements the descriptorProto interface.
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
