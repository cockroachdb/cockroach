// Copyright 2019 The Cockroach Authors.
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

package cat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// Family is an interface to a table column family, exposing only the
// information needed by the query optimizer.
type Family interface {
	// ID is the stable identifier for this family that is guaranteed to be
	// unique within the owning table. See the comment for StableID for more
	// detail.
	ID() StableID

	// Name is the name of the family.
	Name() tree.Name

	// Table returns a reference to the table with which this family is
	// associated.
	Table() Table

	// ColumnCount returns the number of columns in the family.
	ColumnCount() int

	// Column returns the ith FamilyColumn within the family, where
	// i < ColumnCount.
	Column(i int) FamilyColumn
}

// FamilyColumn describes a single column that is part of a family definition.
type FamilyColumn struct {
	// Column is a reference to the column returned by Table.Column, given the
	// column ordinal.
	Column

	// Ordinal is the ordinal position of the family column in the table. It is
	// always >= 0 and < Table.ColumnCount.
	Ordinal int
}
