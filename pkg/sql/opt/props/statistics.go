// Copyright 2018 The Cockroach Authors.
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

package props

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. Statistics are collected
// for tables and indexes and are exposed to the optimizer via opt.Catalog
// interfaces.
//
// As logical properties are derived bottom-up for each expression, the
// estimated row count is derived bottom-up for each relational expression.
// The column statistics (stored in ColStats and MultiColStats) are derived
// lazily, and only as needed to determine the row count for the current
// expression or a parent expression. For example:
//
//   SELECT y FROM a WHERE x=1
//
// The only column that affects the row count of this query is x, since the
// distribution of values in x is what determines the selectivity of the
// predicate. As a result, column statistics will be derived for column x but
// not for column y.
type Statistics struct {
	// RowCount is the estimated number of rows returned by the expression.
	// Note that - especially when there are no stats available - the scaling of
	// the row counts can be unpredictable; thus, a row count of 0.001 should be
	// considered 1000 times better than a row count of 1, even though if this was
	// a true row count they would be pretty much the same thing.
	RowCount float64

	// ColStats contains statistics that pertain to individual columns
	// in an expression or table. It is keyed by column ID, and it is separated
	// from the MultiColStats to minimize serialization costs and to efficiently
	// iterate through all single-column stats.
	ColStats map[opt.ColumnID]*ColumnStatistic

	// MultiColStats contains statistics that pertain to multi-column subsets
	// of the columns in an expression or table. It is keyed by the column set,
	// which has been serialized to a string to make it a legal map key.
	MultiColStats map[string]*ColumnStatistic

	// Selectivity is a value between 0 and 1 representing the estimated
	// reduction in number of rows for the top-level operator in this
	// expression.
	Selectivity float64
}

func (s *Statistics) String() string {
	var buf bytes.Buffer

	buf.WriteString("[rows=")
	printFloat(&buf, s.RowCount)
	colStats := make(ColumnStatistics, 0, len(s.ColStats)+len(s.MultiColStats))
	for _, colStat := range s.ColStats {
		colStats = append(colStats, *colStat)
	}
	for _, colStat := range s.MultiColStats {
		colStats = append(colStats, *colStat)
	}
	sort.Sort(colStats)
	for _, col := range colStats {
		fmt.Fprintf(&buf, ", distinct%s=", col.Cols.String())
		printFloat(&buf, col.DistinctCount)
	}
	buf.WriteString("]")

	return buf.String()
}

// ColumnStatistic is a collection of statistics that applies to a particular
// set of columns. In theory, a table could have a ColumnStatistic object
// for every possible subset of columns. In practice, it is only worth
// maintaining statistics on a few columns and column sets that are frequently
// used in predicates, group by columns, etc.
type ColumnStatistic struct {
	// Cols is the set of columns whose data are summarized by this
	// ColumnStatistic struct.
	Cols opt.ColSet

	// DistinctCount is the estimated number of distinct values of this
	// set of columns for this expression.
	DistinctCount float64
}

// ColumnStatistics is a slice of ColumnStatistic values.
type ColumnStatistics []ColumnStatistic

// Len returns the number of ColumnStatistic values.
func (c ColumnStatistics) Len() int { return len(c) }

// Less is part of the Sorter interface.
func (c ColumnStatistics) Less(i, j int) bool {
	if c[i].Cols.Len() != c[j].Cols.Len() {
		return c[i].Cols.Len() < c[j].Cols.Len()
	}

	prev := 0
	for {
		nextI, ok := c[i].Cols.Next(prev)
		if !ok {
			return false
		}

		// No need to check if ok since both ColSets are the same length and
		// so far have had the same elements.
		nextJ, _ := c[j].Cols.Next(prev)

		if nextI != nextJ {
			return nextI < nextJ
		}

		prev = nextI
	}
}

// Swap is part of the Sorter interface.
func (c ColumnStatistics) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func printFloat(buf *bytes.Buffer, v float64) {
	if v >= 100 && v <= 1e+15 || math.Abs(math.Round(v)-v) < .0001 {
		// Omit fractional digits for integers and for large values.
		fmt.Fprintf(buf, "%.0f", v)
	} else if v >= 1 {
		fmt.Fprintf(buf, "%.2f", v)
	} else {
		fmt.Fprintf(buf, "%.4f", v)
	}
}
