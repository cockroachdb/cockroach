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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
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

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
	// MaxReservedDescID is the maximum reserved descriptor ID.
	// All objects with ID <= MaxReservedDescID are system object
	// with special rules.
	MaxReservedDescID ID = 999
	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID ID = 0

	// System IDs should be kept in sync with IsSystem methods on
	// DatabaseDescriptor and TableDescriptor.
	// systemDatabaseID is the ID of the system database.
	systemDatabaseID ID = 1
	// namespaceTableID is the ID of the namespace table.
	namespaceTableID ID = 2
	// descriptorTableID is the ID of the descriptor table.
	descriptorTableID ID = 3
)

// ErrMissingPrimaryKey exported to the sql package.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

// SystemDB is the descriptor for the system database.
var SystemDB = DatabaseDescriptor{
	Name:       "system",
	ID:         systemDatabaseID,
	Privileges: NewSystemObjectPrivilegeDescriptor(),
}

// NamespaceTable is the descriptor for the namespace table.
var NamespaceTable TableDescriptor

// DescriptorTable is the descriptor for the descriptor table.
var DescriptorTable TableDescriptor

func init() {
	const sql = `
CREATE TABLE system.namespace (
  "parentID" INT,
  "name"     CHAR,
  "id"       INT,
  PRIMARY KEY (parentID, name)
);

CREATE TABLE system.descriptor (
  "id"   INT PRIMARY KEY,
  "desc" BLOB
);
`
	stmts, err := parser.Parse(sql)
	if err != nil {
		log.Fatal(err)
	}

	NamespaceTable, err = makeTableDesc(stmts[0].(*parser.CreateTable))
	if err != nil {
		log.Fatal(err)
	}
	NamespaceTable.Privileges = SystemDB.Privileges
	NamespaceTable.ID = namespaceTableID
	if err := NamespaceTable.AllocateIDs(); err != nil {
		log.Fatal(err)
	}

	DescriptorTable, err = makeTableDesc(stmts[1].(*parser.CreateTable))
	if err != nil {
		log.Fatal(err)
	}
	DescriptorTable.Privileges = SystemDB.Privileges
	DescriptorTable.ID = descriptorTableID
	if err := DescriptorTable.AllocateIDs(); err != nil {
		log.Fatal(err)
	}
}

// IsSystemID returns true if this ID is reserved for system objects.
func IsSystemID(id ID) bool {
	return id > 0 && id <= MaxReservedDescID
}

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
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
// for non-unique indexes.
func (desc *IndexDescriptor) fullColumnIDs() []ColumnID {
	if desc.Unique {
		return desc.ColumnIDs
	}
	// Non-unique indexes have the some of the primary-key columns appended to
	// their key.
	columnIDs := append([]ColumnID(nil), desc.ColumnIDs...)
	columnIDs = append(columnIDs, desc.ImplicitColumnIDs...)
	return columnIDs
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

// AllocateIDs allocates column and index ids for any column or index which has
// an ID of 0.
func (desc *TableDescriptor) AllocateIDs() error {
	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}

	columnNames := map[string]ColumnID{}
	for i := range desc.Columns {
		columnID := desc.Columns[i].ID
		if columnID == 0 {
			columnID = desc.NextColumnID
			desc.NextColumnID++
		}
		columnNames[desc.Columns[i].Name] = columnID
		desc.Columns[i].ID = columnID
	}

	// Create a slice of modifiable index descriptors.
	var indexes []*IndexDescriptor
	indexes = append(indexes, &desc.PrimaryIndex)
	for i := range desc.Indexes {
		indexes = append(indexes, &desc.Indexes[i])
	}
	// Populate IDs
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
				index.ColumnIDs[j] = columnNames[colName]
			}
		}
		if index != &desc.PrimaryIndex {
			var extraColumnIDs []ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.containsColumnID(primaryColID) {
					extraColumnIDs = append(extraColumnIDs, primaryColID)
				}
			}
			index.ImplicitColumnIDs = extraColumnIDs
		}
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = MaxReservedDescID + 1
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
		return fmt.Errorf("invalid table ID 0")
	}

	if len(desc.Columns) == 0 {
		return fmt.Errorf("table must contain at least 1 column")
	}

	columnNames := map[string]ColumnID{}
	columnIDs := map[ColumnID]string{}
	for _, column := range desc.Columns {
		if err := validateName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return fmt.Errorf("invalid column ID 0")
		}

		if _, ok := columnNames[column.Name]; ok {
			return fmt.Errorf("duplicate column name: \"%s\"", column.Name)
		}
		columnNames[column.Name] = column.ID

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

	// TODO(pmattis): Check that the indexes are unique. That is, no 2 indexes
	// should contain identical sets of columns.
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[IndexID]string{}
	for _, index := range append([]IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		if err := validateName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID 0")
		}

		if _, ok := indexNames[index.Name]; ok {
			return fmt.Errorf("duplicate index name: \"%s\"", index.Name)
		}
		indexNames[index.Name] = struct{}{}

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

		if len(index.ColumnIDs) == 0 {
			return fmt.Errorf("index \"%s\" must contain at least 1 column", index.Name)
		}

		for i, name := range index.ColumnNames {
			colID, ok := columnNames[name]
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
	return desc.Privileges.Validate(IsSystemID(desc.GetID()))
}

// FindColumnByName finds the column with specified name.
func (desc *TableDescriptor) FindColumnByName(name string) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if equalName(c.Name, name) {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column \"%s\" does not exist", name)
}

// FindColumnByID finds the column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindIndexByName finds the index with specified name.
func (desc *TableDescriptor) FindIndexByName(name string) (*IndexDescriptor, error) {
	for i, idx := range desc.Indexes {
		if equalName(idx.Name, name) {
			return &desc.Indexes[i], nil
		}
	}
	return nil, fmt.Errorf("index \"%s\" does not exist", name)
}

// SQLString returns the SQL string corresponding to the type.
func (c *ColumnType) SQLString() string {
	switch c.Kind {
	case ColumnType_BIT, ColumnType_INT, ColumnType_CHAR:
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
		return fmt.Errorf("invalid database ID 0")
	}
	// Validate the privilege descriptor.
	return desc.Privileges.Validate(IsSystemID(desc.GetID()))
}
