// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttlbase

import (
	"bytes"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
)

// DefaultAOSTDuration is the default duration to use in the AS OF SYSTEM TIME
// clause used in the SELECT query.
const DefaultAOSTDuration = -time.Second * 30

var startKeyCompareOps = map[catenumpb.IndexColumn_Direction]string{
	catenumpb.IndexColumn_ASC:  ">",
	catenumpb.IndexColumn_DESC: "<",
}
var endKeyCompareOps = map[catenumpb.IndexColumn_Direction]string{
	catenumpb.IndexColumn_ASC:  "<",
	catenumpb.IndexColumn_DESC: ">",
}

func BuildSelectQuery(
	relationName string,
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	aostDuration time.Duration,
	ttlExpr catpb.Expression,
	numStartQueryBounds, numEndQueryBounds int,
	limit int64,
	startIncl bool,
) string {
	numPkCols := len(pkColNames)
	if numPkCols == 0 {
		panic("pkColNames is empty")
	}
	if numPkCols != len(pkColDirs) {
		panic("different number of pkColNames and pkColDirs")
	}
	var buf bytes.Buffer
	// SELECT
	buf.WriteString("SELECT ")
	for i := range pkColNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(pkColNames[i])
	}
	// FROM
	buf.WriteString("\nFROM ")
	buf.WriteString(relationName)
	// AS OF SYSTEM TIME
	buf.WriteString("\nAS OF SYSTEM TIME INTERVAL '")
	buf.WriteString(strconv.Itoa(int(aostDuration.Milliseconds()) / 1000))
	buf.WriteString(" seconds'")
	// WHERE
	buf.WriteString("\nWHERE ((")
	buf.WriteString(string(ttlExpr))
	buf.WriteString(") <= $1)")
	writeBounds := func(
		numQueryBounds int,
		placeholderOffset int,
		compareOps map[catenumpb.IndexColumn_Direction]string,
		inclusive bool,
	) {
		if numQueryBounds > 0 {
			buf.WriteString("\nAND (")
			for i := 0; i < numQueryBounds; i++ {
				isLast := i == numQueryBounds-1
				buf.WriteString("\n  (")
				for j := 0; j < i; j++ {
					buf.WriteString(pkColNames[j])
					buf.WriteString(" = $")
					buf.WriteString(strconv.Itoa(j + placeholderOffset))
					buf.WriteString(" AND ")
				}
				buf.WriteString(pkColNames[i])
				buf.WriteString(" ")
				buf.WriteString(compareOps[pkColDirs[i]])
				if isLast && inclusive {
					buf.WriteString("=")
				}
				buf.WriteString(" $")
				buf.WriteString(strconv.Itoa(i + placeholderOffset))
				buf.WriteString(")")
				if !isLast {
					buf.WriteString(" OR")
				}
			}
			buf.WriteString("\n)")
		}
	}
	const endPlaceholderOffset = 2
	writeBounds(
		numStartQueryBounds,
		endPlaceholderOffset+numEndQueryBounds,
		startKeyCompareOps,
		startIncl,
	)
	writeBounds(
		numEndQueryBounds,
		endPlaceholderOffset,
		endKeyCompareOps,
		true, /*inclusive*/
	)

	// ORDER BY
	buf.WriteString("\nORDER BY ")
	for i := range pkColNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(pkColNames[i])
		buf.WriteString(" ")
		buf.WriteString(pkColDirs[i].String())
	}
	// LIMIT
	buf.WriteString("\nLIMIT ")
	buf.WriteString(strconv.Itoa(int(limit)))
	return buf.String()
}

func BuildDeleteQuery(
	relationName string, pkColNames []string, ttlExpr catpb.Expression, numRows int,
) string {
	if len(pkColNames) == 0 {
		panic("pkColNames is empty")
	}
	var buf bytes.Buffer
	// DELETE
	buf.WriteString("DELETE FROM ")
	buf.WriteString(relationName)
	// WHERE
	buf.WriteString("\nWHERE ((")
	buf.WriteString(string(ttlExpr))
	buf.WriteString(") <= $1)")
	if numRows > 0 {
		buf.WriteString("\nAND (")
		for i := range pkColNames {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(pkColNames[i])
		}
		buf.WriteString(") IN (")
		for i := 0; i < numRows; i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("(")
			for j := range pkColNames {
				if j > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString("$")
				buf.WriteString(strconv.Itoa(i*len(pkColNames) + j + 2))
			}
			buf.WriteString(")")
		}
		buf.WriteString(")")
	}
	return buf.String()
}
