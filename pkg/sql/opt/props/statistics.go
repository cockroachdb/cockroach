// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/olekukonko/tablewriter"
)

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. Statistics are collected
// for tables and indexes and are exposed to the optimizer via cat.Catalog
// interfaces.
//
// As logical properties are derived bottom-up for each expression, the
// estimated row count is derived bottom-up for each relational expression.
// The column statistics (stored in ColStats and MultiColStats) are derived
// lazily, and only as needed to determine the row count for the current
// expression or a parent expression. For example:
//
//	SELECT y FROM a WHERE x=1
//
// The only column that affects the row count of this query is x, since the
// distribution of values in x is what determines the selectivity of the
// predicate. As a result, column statistics will be derived for column x but
// not for column y.
//
// See memo/statistics_builder.go for more information about how statistics are
// calculated.
type Statistics struct {
	// Available indicates whether the underlying table statistics for this
	// expression were available. If true, RowCount contains a real estimate.
	// If false, RowCount does not represent reality, and should only be used
	// for relative cost comparison.
	Available bool

	// RowCount is the estimated number of rows returned by the expression.
	// Note that - especially when there are no stats available - the scaling of
	// the row counts can be unpredictable; thus, a row count of 0.001 should be
	// considered 1000 times better than a row count of 1, even though if this was
	// a true row count they would be pretty much the same thing.
	//
	// For expressions with Cardinality.Max = 0, RowCount will be 0. For
	// expressions with Cardinality.Max > 0, RowCount will be >= epsilon.
	RowCount float64

	// minRowCount, if greater than zero, limits the lower bound of RowCount
	// when it is updated by ApplySelectivity. See Init for more details.
	minRowCount float64

	// VirtualCols is the set of virtual computed columns produced by our input
	// that we have statistics on. Any of these could appear in ColStats. This set
	// is maintained separately from OutputCols to allow lookup of statistics on
	// virtual columns for expressions that synthesize virtual columns.
	VirtualCols opt.ColSet

	// ColStats is a collection of statistics that pertain to columns in an
	// expression or table. It is keyed by a set of one or more columns over which
	// the statistic is defined.
	ColStats ColStatsMap

	// Selectivity is a value between 0 and 1 representing the estimated reduction
	// in number of rows for the top-level operator in this expression, relative
	// to some input. The input depends on the expression, e.g. for semi joins the
	// input is the left child, for inner joins the input is the left child times
	// the right child, for constrained scans the input is a full table scan, etc.
	//
	// Selectivity is used to calculate column statistics after the corresponding
	// expression statistics have been derived.
	//
	// For expressions with Cardinality.Max = 0, selectivity will be 0. For
	// expressions with Cardinality.Max > 0, selectivity will be in the range
	// [epsilon, 1.0].
	Selectivity Selectivity

	// AvgColSizes contains the estimated average size of columns that
	// originates from a table. The i-th element of the slice is the average
	// size of the column with ordinal i in its table. AvgSize is only non-nil
	// when the statistics are built from a table.
	AvgColSizes []uint64
}

// Init initializes the data members of Statistics.
//
// minRowCount, if greater than zero, limits the lower bound of RowCount when it
// is updated by ApplySelectivity. If minRowCount is zero, then there is no
// lower bound (however, in practice there is some lower bound due to
// Selectivity being at least epsilon). Note that if minRowCount is non-zero,
// RowCount can still be zero if the cardinality of the expression is zero,
// e.g., for a contradictory filter.
func (s *Statistics) Init(relProps *Relational, minRowCount float64) (zeroCardinality bool) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Reusing fields must be done explicitly.
	*s = Statistics{
		minRowCount: minRowCount,
	}
	if relProps.Cardinality.IsZero() {
		s.RowCount = 0
		s.Selectivity = ZeroSelectivity
		s.Available = true
		return true
	}
	s.Selectivity = OneSelectivity
	return false
}

// RowCountIfAvailable returns the RowCount if the stats were available and a
// negative number otherwise.
func (s *Statistics) RowCountIfAvailable() float64 {
	if s.Available {
		return s.RowCount
	}
	return -1
}

// CopyFrom copies a Statistics object which can then be modified independently.
func (s *Statistics) CopyFrom(other *Statistics) {
	s.Available = other.Available
	s.RowCount = other.RowCount
	s.VirtualCols = other.VirtualCols.Copy()
	s.ColStats.CopyFrom(&other.ColStats)
	s.Selectivity = other.Selectivity
}

// ApplySelectivity applies a given selectivity to the statistics. RowCount and
// Selectivity are updated. Note that DistinctCounts, NullCounts, and
// Histograms are not updated.
// See ColumnStatistic.ApplySelectivity for updating distinct counts, null
// counts, and histograms.
func (s *Statistics) ApplySelectivity(selectivity Selectivity) {
	if selectivity == ZeroSelectivity {
		s.Selectivity = ZeroSelectivity
		s.RowCount = 0
	} else if r := s.RowCount * selectivity.AsFloat(); r < s.minRowCount {
		s.Selectivity.Multiply(MakeSelectivityFromFraction(s.minRowCount, s.RowCount))
		s.RowCount = s.minRowCount
	} else {
		s.Selectivity.Multiply(selectivity)
		s.RowCount = r
	}
}

// UnionWith unions this Statistics object with another Statistics object. It
// updates the RowCount and Selectivity, and represents the result of unioning
// two relational expressions with the given statistics. Note that
// DistinctCounts, NullCounts, and Histograms are not updated.
func (s *Statistics) UnionWith(other *Statistics) {
	s.Available = s.Available && other.Available
	s.RowCount += other.RowCount
	s.Selectivity.UnsafeAdd(other.Selectivity)
	if s.Selectivity.AsFloat() > 1.0 {
		s.Selectivity = OneSelectivity
	}
}

// String returns a string representation of the statistics.
func (s *Statistics) String() string {
	return s.stringImpl(true)
}

// StringWithoutHistograms is like String, but all histograms are omitted from
// the returned string.
func (s *Statistics) StringWithoutHistograms() string {
	return s.stringImpl(false)
}

func (s *Statistics) stringImpl(includeHistograms bool) string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "[rows=%.7g", s.RowCount)
	colStats := make(ColumnStatistics, s.ColStats.Count())
	for i := 0; i < s.ColStats.Count(); i++ {
		colStats[i] = s.ColStats.Get(i)
	}
	sort.Sort(colStats)
	for _, col := range colStats {
		fmt.Fprintf(&buf, ", distinct%s=%.6g", col.Cols.String(), col.DistinctCount)
		fmt.Fprintf(&buf, ", null%s=%.6g", col.Cols.String(), col.NullCount)
	}
	buf.WriteString("]")
	if includeHistograms {
		for _, col := range colStats {
			if col.Histogram != nil {
				label := fmt.Sprintf("histogram%s=", col.Cols.String())
				indent := strings.Repeat(" ", tablewriter.DisplayWidth(label))
				fmt.Fprintf(&buf, "\n%s", label)
				histLines := strings.Split(strings.TrimRight(col.Histogram.String(), "\n"), "\n")
				for i, line := range histLines {
					if i != 0 {
						fmt.Fprintf(&buf, "\n%s", indent)
					}
					fmt.Fprintf(&buf, "%s", strings.TrimRight(line, " "))
				}
			}
		}
	}
	if !s.VirtualCols.Empty() {
		fmt.Fprintf(&buf, "\nvirtcolstats: %v", s.VirtualCols)
	}

	return buf.String()
}

// ColumnStatistic is a collection of statistics that applies to a particular
// set of columns. In theory, a table could have a ColumnStatistic object
// for every possible subset of columns. In practice, it is only worth
// maintaining statistics on a few columns and column sets that are frequently
// used in predicates, group by columns, etc.
//
// ColumnStatistics can be copied by value.
type ColumnStatistic struct {
	// Cols is the set of columns whose data are summarized by this
	// ColumnStatistic struct. The ColSet is never modified in-place.
	Cols opt.ColSet

	// DistinctCount is the estimated number of distinct values of this
	// set of columns for this expression. Includes null values.
	DistinctCount float64

	// NullCount is the estimated number of null values of this set of
	// columns for this expression. For multi-column stats, this null
	// count tracks only the rows in which all columns in the set are null.
	NullCount float64

	// Histogram is only used when the size of Cols is one. It contains
	// the approximate distribution of values for that column, represented
	// by a slice of histogram buckets.
	Histogram *Histogram
}

// ApplySelectivity updates the distinct count, null count, and histogram
// according to a given selectivity.
func (c *ColumnStatistic) ApplySelectivity(selectivity Selectivity, inputRows float64) {
	// Since the null count is a simple count of all null rows, we can
	// just multiply the selectivity with it.
	c.NullCount *= selectivity.AsFloat()

	if c.Histogram != nil {
		c.Histogram = c.Histogram.ApplySelectivity(selectivity)
	}

	if selectivity == OneSelectivity || c.DistinctCount == 0 {
		return
	}
	if selectivity == ZeroSelectivity {
		c.DistinctCount = 0
		return
	}

	n := inputRows
	d := c.DistinctCount

	// If each distinct value appears n/d times, and the probability of a
	// row being filtered out is (1 - selectivity), the probability that all
	// n/d rows are filtered out is (1 - selectivity)^(n/d). So the expected
	// number of values that are filtered out is d*(1 - selectivity)^(n/d).
	//
	// This formula returns d * selectivity when d=n but is closer to d
	// when d << n.
	c.DistinctCount = d - d*math.Pow(1-selectivity.AsFloat(), n/d)
	const epsilon = 1e-10
	if c.DistinctCount < epsilon {
		// Avoid setting the distinct count to 0 (since the row count is
		// non-zero).
		c.DistinctCount = epsilon
	}
}

// CopyFromOther copies all fields of the other ColumnStatistic except Cols,
// including the Histogram, into the receiver.
func (c *ColumnStatistic) CopyFromOther(other *ColumnStatistic, evalCtx *eval.Context) {
	c.DistinctCount = other.DistinctCount
	c.NullCount = other.NullCount
	if other.Histogram != nil && c.Cols.Len() == 1 {
		c.Histogram = &Histogram{}
		c.Histogram.Init(evalCtx, c.Cols.SingleColumn(), other.Histogram.buckets)
	}
}

// ColumnStatistics is a slice of pointers to ColumnStatistic values.
type ColumnStatistics []*ColumnStatistic

// Len returns the number of ColumnStatistic values.
func (c ColumnStatistics) Len() int { return len(c) }

// Less is part of the Sorter interface.
func (c ColumnStatistics) Less(i, j int) bool {
	if c[i].Cols.Len() != c[j].Cols.Len() {
		return c[i].Cols.Len() < c[j].Cols.Len()
	}

	prev := opt.ColumnID(0)
	for {
		nextI, ok := c[i].Cols.Next(prev + 1)
		if !ok {
			return false
		}

		// No need to check if ok since both ColSets are the same length and
		// so far have had the same elements.
		nextJ, _ := c[j].Cols.Next(prev + 1)

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
