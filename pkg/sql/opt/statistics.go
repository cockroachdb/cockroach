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

package opt

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. Statistics are collected
// for tables and indexes and are exposed to the optimizer via opt.Catalog
// interfaces. As logical properties are derived bottom-up for each
// expression, statistics are derived for each relational expression, based
// on the characteristics of the expression's operator type, as well as the
// properties of its operands. For example:
//
//   SELECT * FROM a WHERE a=1
//
// Table and column statistics can be used to estimate the number of rows
// in table a, and then to determine the selectivity of the a=1 filter, in
// order to derive the statistics of the Select expression.
type Statistics struct {
	// RowCount is the estimated number of rows returned by the expression.
	RowCount uint64

	// ColStats contains statistics that pertain to individual columns
	// in an expression or table. It is keyed by column ID, and it is separated
	// from the MultiColStats to minimize serialization costs and to efficiently
	// iterate through all single-column stats.
	ColStats map[ColumnID]*ColumnStatistic

	// MultiColStats contains statistics that pertain to multi-column subsets
	// of the columns in an expression or table. It is keyed by the column set,
	// which has been serialized to a string to make it a legal map key.
	MultiColStats map[string]*ColumnStatistic

	// Selectivity is a value between 0 and 1 representing the estimated
	// reduction in number of rows for the top-level operator in this
	// expression.
	Selectivity float64
}

// ColumnStatistic is a collection of statistics that applies to a particular
// set of columns. In theory, a table could have a ColumnStatistic object
// for every possible subset of columns. In practice, it is only worth
// maintaining statistics on a few columns and column sets that are frequently
// used in predicates, group by columns, etc.
type ColumnStatistic struct {
	// Cols is the set of columns whose data are summarized by this
	// ColumnStatistic struct.
	Cols ColSet

	// DistinctCount is the estimated number of distinct values of this
	// set of columns for this expression.
	DistinctCount uint64
}
