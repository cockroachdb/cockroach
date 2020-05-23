// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Example sql tables:
//  CREATE TABLE xy (x INT PRIMARY KEY, y INT)
//  CREATE TABLE uv (u INT PRIMARY KEY, v INT)
//  CREATE TABLE fk (k INT PRIMARY KEY, r INT UNIQUE NOT NULL REFERENCES xy(x))

// A table is being joined with itself on a key column so exactly one match
// is guaranteed per row.
//  SELECT * FROM xy INNER JOIN xy AS a ON xy.x = a.x
var allUnchanged = Multiplicity{
	LeftMultiplicity:  UnchangedMultiplicityVal,
	RightMultiplicity: UnchangedMultiplicityVal,
}

// The foreign key equality guarantees matches for rows from the fk table. Since
// the cross join will duplicate xy rows, fk rows will be duplicated by the
// InnerJoin. Rows from xy are not guaranteed a match, and they will not be
// duplicated because the r column is unique.
//  SELECT * FROM
//  	(SELECT * FROM xy JOIN (Values (1), (2)) ON True) AS f(x)
//  	INNER JOIN fk
//  	ON f.x = r
var innerForeignKey = Multiplicity{
	LeftMultiplicity:  FilteredMultiplicityVal,
	RightMultiplicity: DuplicatedMultiplicityVal,
}

// LeftJoin with an equality between non-key columns.
//  SELECT * FROM xy LEFT JOIN uv ON y = v
var leftNonKey = Multiplicity{
	LeftMultiplicity: DuplicatedMultiplicityVal,
	RightMultiplicity: DuplicatedMultiplicityVal |
		FilteredMultiplicityVal | NullExtendedMultiplicityVal,
}

// LeftJoin case with an equality between the same column. Rows from xy are
// guaranteed a match, while rows from xy1 are not, since xy rows have been
// filtered. Rows from both inputs may be duplicated because the join is on
// a non-key column (which may have duplicates).
//  SELECT * FROM
//  	(SELECT * FROM xy WHERE x < 3)
//  	LEFT JOIN xy AS xy1
//  	On xy.y = xy1.y
var leftSameColFiltered = Multiplicity{
	LeftMultiplicity: DuplicatedMultiplicityVal,
	RightMultiplicity: FilteredMultiplicityVal |
		DuplicatedMultiplicityVal | NullExtendedMultiplicityVal,
}

// FullJoin case with a join on primary keys. Rows can be matched zero or one
// times. Rows that are not matched are added back, null-extended.
//  SELECT * FROM xy FULL JOIN uv ON x = u
var fullPrimaryKey = Multiplicity{
	LeftMultiplicity:  UnchangedMultiplicityVal | NullExtendedMultiplicityVal,
	RightMultiplicity: UnchangedMultiplicityVal | NullExtendedMultiplicityVal,
}

func TestMultiplicity_JoinCanDuplicateLeftRows(t *testing.T) {
	require.Equal(t, false, innerForeignKey.JoinCanDuplicateLeftRows())
	require.Equal(t, true, leftNonKey.JoinCanDuplicateLeftRows())
	require.Equal(t, false, fullPrimaryKey.JoinCanDuplicateLeftRows())
}

func TestMultiplicity_JoinCanDuplicateRightRows(t *testing.T) {
	require.Equal(t, true, innerForeignKey.JoinCanDuplicateRightRows())
	require.Equal(t, true, leftSameColFiltered.JoinCanDuplicateRightRows())
	require.Equal(t, false, fullPrimaryKey.JoinCanDuplicateRightRows())
}

func TestMultiplicity_JoinCanFilterLeftRows(t *testing.T) {
	require.Equal(t, false, allUnchanged.JoinCanFilterLeftRows())
	require.Equal(t, true, innerForeignKey.JoinCanFilterLeftRows())
	require.Equal(t, false, leftNonKey.JoinCanFilterLeftRows())
}

func TestMultiplicity_JoinCanFilterRightRows(t *testing.T) {
	require.Equal(t, false, allUnchanged.JoinCanFilterRightRows())
	require.Equal(t, true, leftNonKey.JoinCanFilterRightRows())
	require.Equal(t, false, fullPrimaryKey.JoinCanFilterRightRows())
}

func TestMultiplicity_JoinCanNullExtendLeftRows(t *testing.T) {
	require.Equal(t, false, allUnchanged.JoinCanNullExtendLeftRows())
	require.Equal(t, false, leftNonKey.JoinCanNullExtendLeftRows())
	require.Equal(t, true, fullPrimaryKey.JoinCanNullExtendLeftRows())
}

func TestMultiplicity_JoinCanNullExtendRightRows(t *testing.T) {
	require.Equal(t, false, allUnchanged.JoinCanNullExtendRightRows())
	require.Equal(t, true, leftSameColFiltered.JoinCanNullExtendRightRows())
	require.Equal(t, true, fullPrimaryKey.JoinCanNullExtendRightRows())
}
