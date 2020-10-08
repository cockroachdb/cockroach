// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	*Column

	// Ordinal is the ordinal position of the family column in the table. It is
	// always >= 0 and < Table.ColumnCount.
	Ordinal int
}
