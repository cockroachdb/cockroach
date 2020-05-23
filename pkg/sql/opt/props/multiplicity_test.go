// Copyright 2020 The Cockroach Authors.
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
var allUnchanged = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
}

// The foreign key equality guarantees matches for rows from the fk table. Since
// the cross join will duplicate xy rows, fk rows will be duplicated by the
// InnerJoin. Rows from xy are not guaranteed a match, and they will not be
// duplicated because the r column is unique.
//  SELECT * FROM
//  	(SELECT * FROM xy JOIN (Values (1), (2)) ON True) AS f(x)
//  	INNER JOIN fk
//  	ON f.x = r
var innerForeignKey = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal,
	RightMultiplicity: MultiplicityPreservedVal,
}

// LeftJoin with an equality between non-key columns.
//  SELECT * FROM xy LEFT JOIN uv ON y = v
var leftNonKey = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityIndeterminateVal,
}

// LeftJoin case with an equality between the same column. Rows from xy are
// guaranteed a match, while rows from xy1 are not, since xy rows have been
// filtered. Rows from both inputs may be duplicated because the join is on
// a non-key column (which may have duplicates).
//  SELECT * FROM
//  	(SELECT * FROM xy WHERE x < 3)
//  	LEFT JOIN xy AS xy1
//  	On xy.y = xy1.y
var leftSameColFiltered = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityIndeterminateVal,
}

// FullJoin case with a join on primary keys. Rows can be matched zero or one
// times. Rows that are not matched are added back.
//  SELECT * FROM xy FULL JOIN uv ON x = u
var fullPrimaryKey = JoinMultiplicity{
	LeftMultiplicity:  MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
	RightMultiplicity: MultiplicityNotDuplicatedVal | MultiplicityPreservedVal,
}

func TestJoinMultiplicity_JoinWillNotDuplicateLeftRows(t *testing.T) {
	require.Equal(t, true, innerForeignKey.JoinWillNotDuplicateLeftRows())
	require.Equal(t, false, leftNonKey.JoinWillNotDuplicateLeftRows())
	require.Equal(t, true, fullPrimaryKey.JoinWillNotDuplicateLeftRows())
}

func TestJoinMultiplicity_JoinWillNotDuplicateRightRows(t *testing.T) {
	require.Equal(t, false, innerForeignKey.JoinWillNotDuplicateRightRows())
	require.Equal(t, false, leftSameColFiltered.JoinWillNotDuplicateRightRows())
	require.Equal(t, true, fullPrimaryKey.JoinWillNotDuplicateRightRows())
}

func TestJoinMultiplicity_JoinPreservesLeftRows(t *testing.T) {
	require.Equal(t, true, allUnchanged.JoinPreservesLeftRows())
	require.Equal(t, false, innerForeignKey.JoinPreservesLeftRows())
	require.Equal(t, true, leftNonKey.JoinPreservesLeftRows())
}

func TestJoinMultiplicity_JoinPreservesRightRows(t *testing.T) {
	require.Equal(t, true, allUnchanged.JoinPreservesRightRows())
	require.Equal(t, false, leftNonKey.JoinPreservesRightRows())
	require.Equal(t, true, fullPrimaryKey.JoinPreservesRightRows())
}
