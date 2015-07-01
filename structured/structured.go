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
	"fmt"
	"strings"
)

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
	// MaxReservedDescID is the maximum reserved descriptor ID.
	MaxReservedDescID = 999
	// RootNamespaceID is the ID of the root namespace.
	RootNamespaceID = 0
)

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
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

	columnNames := map[string]uint32{}
	for i, column := range desc.Columns {
		if column.ID == 0 {
			column.ID = desc.NextColumnID
			desc.NextColumnID++
		}
		columnNames[column.Name] = column.ID
		// Mildly confusing: column is not a pointer so we need to set it back into
		// the columns slice.
		desc.Columns[i] = column
	}

	for i, index := range desc.Indexes {
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
		// Mildly confusing: index is not a pointer so we need to set it back into
		// the index slice.
		desc.Indexes[i] = index
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

	columnNames := map[string]uint32{}
	columnIDs := map[uint32]string{}
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

	if len(desc.Indexes) == 0 {
		return fmt.Errorf("table must contain at least 1 index")
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[uint32]string{}
	for _, index := range desc.Indexes {
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
func (desc *TableDescriptor) FindColumnByID(id uint32) (*ColumnDescriptor, error) {
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
	case ColumnType_BIT, ColumnType_INT, ColumnType_CHAR, ColumnType_BINARY:
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d)", c.Kind.String(), c.Width)
		}
	case ColumnType_FLOAT, ColumnType_DECIMAL:
		if c.Width > 0 {
			if c.Precision > 0 {
				return fmt.Sprintf("%s(%d,%d)", c.Kind.String(), c.Width, c.Precision)
			}
			return fmt.Sprintf("%s(%d)", c.Kind.String(), c.Width)
		}
	case ColumnType_ENUM, ColumnType_SET:
		return fmt.Sprintf("%s(%s)", c.Kind.String(), strings.Join(c.Vals, ","))
	}
	return c.Kind.String()
}
