// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// AutoStatsName is copied from stats.AutoStatsName to avoid pulling
// in a dependency on sql/stats.
const AutoStatsName = "__auto__"

// JSONStatistic is copied from stats.JSONStatistic to avoid pulling
// in a dependency on sql/stats.
type JSONStatistic struct {
	Name          string   `json:"name,omitempty"`
	CreatedAt     string   `json:"created_at"`
	Columns       []string `json:"columns"`
	RowCount      uint64   `json:"row_count"`
	DistinctCount uint64   `json:"distinct_count"`
	NullCount     uint64   `json:"null_count"`
}

// MakeStat returns a JSONStatistic given the column names, row count, distinct
// count, and null count.
func MakeStat(columns []string, rowCount, distinctCount, nullCount uint64) JSONStatistic {
	return JSONStatistic{
		Name:          AutoStatsName,
		CreatedAt:     timeutil.Now().Round(time.Microsecond).UTC().Format(timestampOutputFormat),
		Columns:       columns,
		RowCount:      rowCount,
		DistinctCount: distinctCount,
		NullCount:     nullCount,
	}
}

// DistinctCount returns the expected number of distinct values in a column
// with rowCount rows, given that the values are chosen from maxDistinctCount
// possible values using uniform random sampling with replacement.
func DistinctCount(rowCount, maxDistinctCount uint64) uint64 {
	n := float64(maxDistinctCount)
	k := float64(rowCount)
	// The probability that one specific value (out of the n possible values)
	// does not appear in any of the k rows is:
	//
	//         ⎛ n-1 ⎞ k
	//     p = ⎜-----⎟
	//         ⎝  n  ⎠
	//
	// Therefore, the probability that a specific value appears at least once is
	// 1-p. Over all n values, the expected number that appear at least once is
	// n * (1-p). In other words, the expected distinct count is:
	//
	//                             ⎛     ⎛ n-1 ⎞ k ⎞
	//     E[distinct count] = n * ⎜ 1 - ⎜-----⎟   ⎟
	//                             ⎝     ⎝  n  ⎠   ⎠
	//
	// See https://math.stackexchange.com/questions/72223/finding-expected-
	//   number-of-distinct-values-selected-from-a-set-of-integers for more info.
	count := n * (1 - math.Pow((n-1)/n, k))
	return uint64(int64(math.Round(count)))
}
