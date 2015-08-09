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

package structured

import (
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID uint32

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
	// MaxReservedDescID is the maximum reserved descriptor ID.
	MaxReservedDescID ID = 999
	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID ID = 0
)

// ErrMissingPrimaryKey exported to the sql package.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

// SetID implements the sql.descriptorProto interface.
func (desc *TableDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *TableDescriptor) TypeName() string {
	return "table"
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

	columnNames := map[string]ID{}
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
	}

	// This is sort of ugly. We want to make sure the descriptor is valid, except
	// for checking the table ID. So we whack in a valid table ID for the
	// duration of the call to Validate.
	savedID := desc.ID
	desc.ID = 1
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

	columnNames := map[string]ID{}
	columnIDs := map[ID]string{}
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
	indexIDs := map[ID]string{}
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
	return nil
}

// FindColumnByName finds the column with specified name.
func (desc *TableDescriptor) FindColumnByName(name string) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.Name == name {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column \"%s\" does not exist", name)
}

// FindColumnByID finds the column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
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

// SetID implements the sql.descriptorProto interface.
func (desc *DatabaseDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
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
	if len(desc.Read) == 0 {
		return fmt.Errorf("empty Read permissions")
	}
	if len(desc.Write) == 0 {
		return fmt.Errorf("empty Write permissions")
	}
	return nil
}

// Grant adds new privileges to this descriptor for a given list of users.
func (p *PrivilegeDescriptor) Grant(n *parser.Grant) error {
	permissions, err := permissionsFromDescriptor(p)
	if err != nil {
		return err
	}

	if err := permissions.Grant(n); err != nil {
		return err
	}

	if err := permissions.FillDescriptor(p); err != nil {
		return err
	}
	return nil
}

// Revoke removes privileges from this descriptor for a given list of users.
func (p *PrivilegeDescriptor) Revoke(n *parser.Revoke) error {
	permissions, err := permissionsFromDescriptor(p)
	if err != nil {
		return err
	}

	if err := permissions.Revoke(n); err != nil {
		return err
	}

	if err := permissions.FillDescriptor(p); err != nil {
		return err
	}
	return nil
}

// Show returns the list of users and associated privileges for this descriptor.
func (p *PrivilegeDescriptor) Show() (UserPrivilegeList, error) {
	permissions, err := permissionsFromDescriptor(p)
	if err != nil {
		return nil, err
	}

	return permissions.Show(), nil
}

// HasPrivilege returns true if `user` has `privilege` on this descriptor.
func (p *PrivilegeDescriptor) HasPrivilege(user string, privilege parser.PrivilegeType) bool {
	var privUsers []string
	switch privilege {
	case parser.PrivilegeRead:
		privUsers = p.Read
	case parser.PrivilegeWrite:
		privUsers = p.Write
	}
	result := sort.SearchStrings(privUsers, user)
	return result < len(privUsers) && privUsers[result] == user
}
