// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
)

func TestToOutput(t *testing.T) {
	tests := []struct {
		name     string
		input    apiutil.IndexNamesList
		expected struct {
			databases []string
			tables    []string
			indexes   []string
		}
	}{
		{
			name: "Single database, single table",
			input: apiutil.IndexNamesList{
				{Database: "db1", Table: "table1", Index: "index1"},
			},
			expected: struct {
				databases []string
				tables    []string
				indexes   []string
			}{
				databases: []string{"db1"},
				tables:    []string{"table1"},
				indexes:   []string{"index1"},
			},
		},
		{
			name: "Single database, multiple tables",
			input: apiutil.IndexNamesList{
				{Database: "db1", Table: "table1", Index: "index1"},
				{Database: "db1", Table: "table2", Index: "index2"},
			},
			expected: struct {
				databases []string
				tables    []string
				indexes   []string
			}{
				databases: []string{"db1"},
				tables:    []string{"table1", "table2"},
				indexes:   []string{"table1.index1", "table2.index2"},
			},
		},
		{
			name: "Multiple databases, multiple tables",
			input: apiutil.IndexNamesList{
				{Database: "db1", Table: "table1", Index: "index1"},
				{Database: "db2", Table: "table2", Index: "index2"},
			},
			expected: struct {
				databases []string
				tables    []string
				indexes   []string
			}{
				databases: []string{"db1", "db2"},
				tables:    []string{"db1.table1", "db2.table2"},
				indexes:   []string{"db1.table1.index1", "db2.table2.index2"},
			},
		},
		{
			name: "Duplicate entries",
			input: apiutil.IndexNamesList{
				{Database: "db1", Table: "table1", Index: "index1"},
				{Database: "db1", Table: "table1", Index: "index1"},
			},
			expected: struct {
				databases []string
				tables    []string
				indexes   []string
			}{
				databases: []string{"db1"},
				tables:    []string{"table1"},
				indexes:   []string{"index1", "index1"},
			},
		},
		{
			name: "Identifiers with dot in the name",
			input: apiutil.IndexNamesList{
				{Database: "db.1", Table: "table.1", Index: "index1"},
			},
			expected: struct {
				databases []string
				tables    []string
				indexes   []string
			}{
				databases: []string{"\"db.1\""},
				tables:    []string{"\"table.1\""},
				indexes:   []string{"index1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			databases, tables, indexes := tt.input.ToOutput()
			if !reflect.DeepEqual(databases, tt.expected.databases) {
				t.Errorf("expected databases %v, got %v", tt.expected.databases, databases)
			}
			if !reflect.DeepEqual(tables, tt.expected.tables) {
				t.Errorf("expected tables %v, got %v", tt.expected.tables, tables)
			}
			if !reflect.DeepEqual(indexes, tt.expected.indexes) {
				t.Errorf("expected indexes %v, got %v", tt.expected.indexes, indexes)
			}
		})
	}
}
