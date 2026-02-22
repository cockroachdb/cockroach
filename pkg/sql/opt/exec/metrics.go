// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exec

import (
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
)

// ScanCountType is the type of count of scan operations in a query.
type ScanCountType int

const (
	// ScanCount is the count of all scans in a query.
	ScanCount ScanCountType = iota
	// ScanWithStatsCount is the count of scans with statistics in a query.
	ScanWithStatsCount
	// ScanWithStatsForecastCount is the count of scans which used forecasted
	// statistics in a query.
	ScanWithStatsForecastCount
	// NumScanCountTypes is the total number of types of counts of scans.
	NumScanCountTypes
)

// JoinAlgorithm is the type of join algorithm used.
type JoinAlgorithm int8

// The following are all the supported join algorithms.
const (
	HashJoin JoinAlgorithm = iota
	CrossJoin
	IndexJoin
	LookupJoin
	MergeJoin
	InvertedJoin
	ApplyJoin
	ZigZagJoin
	NumJoinAlgorithms
)

const (
	// NumRecordedJoinTypes includes all join types except for
	// descpb.RightSemiJoin and descpb.RightAntiJoin, which are recorded as left
	// joins.
	NumRecordedJoinTypes = 8
)

// QueryMetrics contains various metrics for the query collected at
// execbuild-time.
type QueryMetrics struct {
	// MaxFullScanRows is the maximum number of rows scanned by a full scan, as
	// estimated by the optimizer.
	MaxFullScanRows float64

	// TotalScanRows is the total number of rows read by all scans in the query,
	// as estimated by the optimizer.
	TotalScanRows float64

	// TotalScanRowsWithoutForecasts is the total number of rows read by all scans
	// in the query, as estimated by the optimizer without using forecasts. (If
	// forecasts were not used, this should be the same as TotalScanRows.)
	TotalScanRowsWithoutForecasts float64

	// StatsCollectedAt is the collection timestamp of the oldest statistics
	// used for a table scanned by this query.
	StatsCollectedAt time.Time

	// StatsCollectedAt is the forecast timestamp of the latest statistics
	// used for a table scanned by this query. It may be in the future.
	StatsForecastedAt time.Time

	// JoinTypeCounts records the number of times each type of logical join was
	// used in the query, up to 255.
	JoinTypeCounts [NumRecordedJoinTypes]uint8

	// JoinAlgorithmCounts records the number of times each type of join
	// algorithm was used in the query, up to 255.
	JoinAlgorithmCounts [NumJoinAlgorithms]uint8

	// ScanCounts records the number of times scans were used in the query.
	ScanCounts [NumScanCountTypes]int

	// IndexesUsed list the indexes used in query with the format tableID@indexID.
	IndexesUsed
}

// RecordJoinType increments the counter for the given join type for telemetry
// reporting.
func (qm *QueryMetrics) RecordJoinType(j descpb.JoinType) {
	// Don't bother distinguishing between left and right.
	switch j {
	case descpb.RightOuterJoin:
		j = descpb.LeftOuterJoin
	case descpb.RightSemiJoin:
		j = descpb.LeftSemiJoin
	case descpb.RightAntiJoin:
		j = descpb.LeftAntiJoin
	}
	if qm.JoinTypeCounts[j]+1 > qm.JoinTypeCounts[j] {
		qm.JoinTypeCounts[j]++
	}
}

// RecordJoinAlgorithm increments the counter for the given join algorithm for
// telemetry reporting.
func (qm *QueryMetrics) RecordJoinAlgorithm(j JoinAlgorithm) {
	if qm.JoinAlgorithmCounts[j]+1 > qm.JoinAlgorithmCounts[j] {
		qm.JoinAlgorithmCounts[j]++
	}
}

// IndexesUsed is a list of indexes used in a query.
type IndexesUsed struct {
	indexes []struct {
		tableID cat.StableID
		indexID cat.StableID
	}
}

// Add adds the given index to the list, if it is not already present.
func (iu *IndexesUsed) Add(tableID, indexID cat.StableID) {
	s := struct {
		tableID cat.StableID
		indexID cat.StableID
	}{tableID, indexID}
	if !slices.Contains(iu.indexes, s) {
		iu.indexes = append(iu.indexes, s)
	}
}

// Strings returns a slice of strings with the format tableID@indexID for each
// index in the list.
//
// TODO(mgartner): Use a slice of struct{uint64, uint64} instead of converting
// to strings.
func (iu *IndexesUsed) Strings() []string {
	res := make([]string, len(iu.indexes))
	const base = 10
	for i, u := range iu.indexes {
		res[i] = strconv.FormatUint(uint64(u.tableID), base) + "@" +
			strconv.FormatUint(uint64(u.indexID), base)
	}
	return res
}
