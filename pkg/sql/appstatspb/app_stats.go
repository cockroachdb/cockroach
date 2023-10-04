// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package appstatspb

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// StmtFingerprintID is the type of a Statement's fingerprint ID.
type StmtFingerprintID uint64

// ConstructStatementFingerprintID constructs an ID by hashing query with
// constants redacted, its database and failure status, and if it was part of an
// implicit txn. At the time of writing, these are the axis' we use to bucket
// queries for stats collection (see stmtKey).
func ConstructStatementFingerprintID(
	stmtNoConstants string, failed bool, implicitTxn bool, database string,
) StmtFingerprintID {
	fnv := util.MakeFNV64()
	for _, c := range stmtNoConstants {
		fnv.Add(uint64(c))
	}
	for _, c := range database {
		fnv.Add(uint64(c))
	}
	if failed {
		fnv.Add('F')
	} else {
		fnv.Add('S')
	}
	if implicitTxn {
		fnv.Add('I')
	} else {
		fnv.Add('E')
	}
	return StmtFingerprintID(fnv.Sum())
}

// TransactionFingerprintID is the hashed string constructed using the
// individual statement fingerprint IDs that comprise the transaction.
type TransactionFingerprintID uint64

// InvalidTransactionFingerprintID denotes an invalid transaction fingerprint ID.
const InvalidTransactionFingerprintID = TransactionFingerprintID(0)

// Size returns the size of the TransactionFingerprintID.
func (t TransactionFingerprintID) Size() int64 {
	return 8
}

// GetVariance retrieves the variance of the values.
func (l *NumericStat) GetVariance(count int64) float64 {
	return l.SquaredDiffs / (float64(count) - 1)
}

// Record updates the underlying running counts, incorporating the given value.
// It follows Welford's algorithm (Technometrics, 1962). The running count must
// be stored as it is required to finalize and retrieve the variance.
func (l *NumericStat) Record(count int64, val float64) {
	delta := val - l.Mean
	l.Mean += delta / float64(count)
	l.SquaredDiffs += delta * (val - l.Mean)
}

// Add combines b into this derived statistics.
func (l *NumericStat) Add(b NumericStat, countA, countB int64) {
	*l = AddNumericStats(*l, b, countA, countB)
}

// AlmostEqual compares two NumericStats between a window of size eps.
func (l *NumericStat) AlmostEqual(b NumericStat, eps float64) bool {
	return math.Abs(l.Mean-b.Mean) <= eps &&
		math.Abs(l.SquaredDiffs-b.SquaredDiffs) <= eps
}

// AddNumericStats combines derived statistics.
// Adapted from https://www.johndcook.com/blog/skewness_kurtosis/
func AddNumericStats(a, b NumericStat, countA, countB int64) NumericStat {
	total := float64(countA + countB)
	delta := b.Mean - a.Mean

	return NumericStat{
		Mean: ((a.Mean * float64(countA)) + (b.Mean * float64(countB))) / total,
		SquaredDiffs: (a.SquaredDiffs + b.SquaredDiffs) +
			delta*delta*float64(countA)*float64(countB)/total,
	}
}

// GetScrubbedCopy returns a copy of the given SensitiveInfo with its fields redacted
// or omitted entirely. By default, fields are omitted: if a new field is
// added to the SensitiveInfo proto, it must be added here to make it to the
// reg cluster.
func (si SensitiveInfo) GetScrubbedCopy() SensitiveInfo {
	output := SensitiveInfo{}
	// TODO(knz): This should really use si.LastErrorRedacted, however
	// this does not exist yet.
	// See: https://github.com/cockroachdb/cockroach/issues/53191
	output.LastErr = "<redacted>"
	// Not copying over MostRecentPlanDescription until we have an algorithm to scrub plan nodes.
	return output
}

// Add combines other into this TxnStats.
func (s *TxnStats) Add(other TxnStats) {
	s.TxnTimeSec.Add(other.TxnTimeSec, s.TxnCount, other.TxnCount)
	s.TxnCount += other.TxnCount
	s.ImplicitCount += other.ImplicitCount
	s.CommittedCount += other.CommittedCount
}

// Add combines other into TransactionStatistics.
func (t *TransactionStatistics) Add(other *TransactionStatistics) {
	if other.MaxRetries > t.MaxRetries {
		t.MaxRetries = other.MaxRetries
	}

	t.IdleLat.Add(other.IdleLat, t.Count, other.Count)
	t.CommitLat.Add(other.CommitLat, t.Count, other.Count)
	t.RetryLat.Add(other.RetryLat, t.Count, other.Count)
	t.ServiceLat.Add(other.ServiceLat, t.Count, other.Count)
	t.NumRows.Add(other.NumRows, t.Count, other.Count)
	t.RowsRead.Add(other.RowsRead, t.Count, other.Count)
	t.BytesRead.Add(other.BytesRead, t.Count, other.Count)
	t.RowsWritten.Add(other.RowsWritten, t.Count, other.Count)

	t.ExecStats.Add(other.ExecStats)

	t.Count += other.Count
}

// Add combines other into this StatementStatistics.
func (s *StatementStatistics) Add(other *StatementStatistics) {
	s.FirstAttemptCount += other.FirstAttemptCount
	if other.MaxRetries > s.MaxRetries {
		s.MaxRetries = other.MaxRetries
	}
	s.SQLType = other.SQLType
	s.NumRows.Add(other.NumRows, s.Count, other.Count)
	s.IdleLat.Add(other.IdleLat, s.Count, other.Count)
	s.ParseLat.Add(other.ParseLat, s.Count, other.Count)
	s.PlanLat.Add(other.PlanLat, s.Count, other.Count)
	s.RunLat.Add(other.RunLat, s.Count, other.Count)
	s.ServiceLat.Add(other.ServiceLat, s.Count, other.Count)
	s.OverheadLat.Add(other.OverheadLat, s.Count, other.Count)
	s.BytesRead.Add(other.BytesRead, s.Count, other.Count)
	s.RowsRead.Add(other.RowsRead, s.Count, other.Count)
	s.RowsWritten.Add(other.RowsWritten, s.Count, other.Count)
	s.Nodes = util.CombineUnique(s.Nodes, other.Nodes)
	s.Regions = util.CombineUnique(s.Regions, other.Regions)
	s.PlanGists = util.CombineUnique(s.PlanGists, other.PlanGists)
	s.IndexRecommendations = other.IndexRecommendations
	s.Indexes = util.CombineUnique(s.Indexes, other.Indexes)

	s.ExecStats.Add(other.ExecStats)
	s.LatencyInfo.Add(other.LatencyInfo)

	if other.SensitiveInfo.LastErr != "" {
		s.SensitiveInfo.LastErr = other.SensitiveInfo.LastErr
	}

	if other.LastErrorCode != "" {
		s.LastErrorCode = other.LastErrorCode
	}

	if s.SensitiveInfo.MostRecentPlanTimestamp.Before(other.SensitiveInfo.MostRecentPlanTimestamp) {
		s.SensitiveInfo = other.SensitiveInfo
	}

	if s.LastExecTimestamp.Before(other.LastExecTimestamp) {
		s.LastExecTimestamp = other.LastExecTimestamp
	}

	s.Count += other.Count
}

// AlmostEqual compares two StatementStatistics and their contained NumericStats
// objects within an window of size eps, ExecStats are ignored.
func (s *StatementStatistics) AlmostEqual(other *StatementStatistics, eps float64) bool {
	return s.Count == other.Count &&
		s.FirstAttemptCount == other.FirstAttemptCount &&
		s.MaxRetries == other.MaxRetries &&
		s.NumRows.AlmostEqual(other.NumRows, eps) &&
		s.IdleLat.AlmostEqual(other.IdleLat, eps) &&
		s.ParseLat.AlmostEqual(other.ParseLat, eps) &&
		s.PlanLat.AlmostEqual(other.PlanLat, eps) &&
		s.RunLat.AlmostEqual(other.RunLat, eps) &&
		s.ServiceLat.AlmostEqual(other.ServiceLat, eps) &&
		s.OverheadLat.AlmostEqual(other.OverheadLat, eps) &&
		s.SensitiveInfo.Equal(other.SensitiveInfo) &&
		s.BytesRead.AlmostEqual(other.BytesRead, eps) &&
		s.RowsRead.AlmostEqual(other.RowsRead, eps) &&
		s.RowsWritten.AlmostEqual(other.RowsWritten, eps)
	// s.ExecStats are deliberately ignored since they are subject to sampling
	// probability and are not fully deterministic (e.g. the number of network
	// messages depends on the range cache state).
}

// Add combines other into this ExecStats.
func (s *ExecStats) Add(other ExecStats) {
	// Execution stats collected using a sampling approach.
	execStatCollectionCount := s.Count
	if execStatCollectionCount == 0 && other.Count == 0 {
		// If both are zero, artificially set the receiver's count to one to avoid
		// division by zero in Add.
		execStatCollectionCount = 1
	}
	s.NetworkBytes.Add(other.NetworkBytes, execStatCollectionCount, other.Count)
	s.MaxMemUsage.Add(other.MaxMemUsage, execStatCollectionCount, other.Count)
	s.ContentionTime.Add(other.ContentionTime, execStatCollectionCount, other.Count)
	s.NetworkMessages.Add(other.NetworkMessages, execStatCollectionCount, other.Count)
	s.MaxDiskUsage.Add(other.MaxDiskUsage, execStatCollectionCount, other.Count)
	s.CPUSQLNanos.Add(other.CPUSQLNanos, execStatCollectionCount, other.Count)

	s.MVCCIteratorStats.StepCount.Add(other.MVCCIteratorStats.StepCount, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.StepCountInternal.Add(other.MVCCIteratorStats.StepCountInternal, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.SeekCount.Add(other.MVCCIteratorStats.SeekCount, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.SeekCountInternal.Add(other.MVCCIteratorStats.SeekCountInternal, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.BlockBytes.Add(other.MVCCIteratorStats.BlockBytes, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.BlockBytesInCache.Add(other.MVCCIteratorStats.BlockBytesInCache, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.KeyBytes.Add(other.MVCCIteratorStats.KeyBytes, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.ValueBytes.Add(other.MVCCIteratorStats.ValueBytes, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.PointCount.Add(other.MVCCIteratorStats.PointCount, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.PointsCoveredByRangeTombstones.Add(other.MVCCIteratorStats.PointsCoveredByRangeTombstones, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.RangeKeyCount.Add(other.MVCCIteratorStats.RangeKeyCount, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.RangeKeyContainedPoints.Add(other.MVCCIteratorStats.RangeKeyContainedPoints, execStatCollectionCount, other.Count)
	s.MVCCIteratorStats.RangeKeySkippedPoints.Add(other.MVCCIteratorStats.RangeKeySkippedPoints, execStatCollectionCount, other.Count)

	s.Count += other.Count
}

// Add combines other into this LatencyInfo.
func (s *LatencyInfo) Add(other LatencyInfo) {
	// Use the latest non-zero value.
	if other.P50 != 0 {
		s.P50 = other.P50
		s.P90 = other.P90
		s.P99 = other.P99
	}

	if s.Min == 0 || other.Min < s.Min {
		s.Min = other.Min
	}
	if other.Max > s.Max {
		s.Max = other.Max
	}
	s.checkPercentiles()
}

// checkPercentiles is a patchy solution and not ideal.
// When the execution count for a period is smaller than 500,
// the percentiles sample is including previous aggregation periods,
// making the p99 possible be greater than the max.
// For now, we just do a check and update the percentiles to the max
// possible size.
// TODO(maryliag): use a proper sample size (#99070)
func (s *LatencyInfo) checkPercentiles() {
	if s.P99 > s.Max {
		s.P99 = s.Max
	}
	if s.P90 > s.Max {
		s.P90 = s.Max
	}
	if s.P50 > s.Max {
		s.P50 = s.Max
	}
}

// ToAggregatedStatementStatistics transforms this CollectedStatementStatistics into an
// obspb.AggregatedStatementStatistics proto.
//
// The purpose of having a separate protobuf is to protect the external o11y systems from being impacted
// by changes to CollectedStatementStatistics
//
// TODO(abarganier): What kind of redaction needs to happen here? Should we omit the Stats.SensitiveData?
// TODO(abarganier): This is going to require a lot of allocations... We'll need to optimize eventually.
func (s *CollectedStatementStatistics) ToAggregatedStatementStatistics() *obspb.AggregatedStatementStatistics {
	out := &obspb.AggregatedStatementStatistics{
		ID: uint64(s.ID),
		Key: obspb.StatementStatisticsKey{
			Query:                    s.Key.Query,
			App:                      s.Key.App,
			DistSQL:                  s.Key.DistSQL,
			Failed:                   s.Key.Failed,
			ImplicitTxn:              s.Key.ImplicitTxn,
			Vec:                      s.Key.Vec,
			FullScan:                 s.Key.FullScan,
			Database:                 s.Key.Database,
			PlanHash:                 s.Key.PlanHash,
			QuerySummary:             s.Key.QuerySummary,
			TransactionFingerprintID: uint64(s.Key.TransactionFingerprintID),
		},
		Stats: obspb.StatementStatistics{
			Count:             s.Stats.Count,
			FirstAttemptCount: s.Stats.FirstAttemptCount,
			MaxRetries:        s.Stats.MaxRetries,
			NumRows: obspb.NumericStat{
				Mean:         s.Stats.NumRows.Mean,
				SquaredDiffs: s.Stats.NumRows.SquaredDiffs,
			},
			IdleLat: obspb.NumericStat{
				Mean:         s.Stats.IdleLat.Mean,
				SquaredDiffs: s.Stats.IdleLat.SquaredDiffs,
			},
			ParseLat: obspb.NumericStat{
				Mean:         s.Stats.ParseLat.Mean,
				SquaredDiffs: s.Stats.ParseLat.SquaredDiffs,
			},
			PlanLat: obspb.NumericStat{
				Mean:         s.Stats.PlanLat.Mean,
				SquaredDiffs: s.Stats.PlanLat.SquaredDiffs,
			},
			RunLat: obspb.NumericStat{
				Mean:         s.Stats.RunLat.Mean,
				SquaredDiffs: s.Stats.RunLat.SquaredDiffs,
			},
			ServiceLat: obspb.NumericStat{
				Mean:         s.Stats.ServiceLat.Mean,
				SquaredDiffs: s.Stats.ServiceLat.SquaredDiffs,
			},
			OverheadLat: obspb.NumericStat{
				Mean:         s.Stats.OverheadLat.Mean,
				SquaredDiffs: s.Stats.OverheadLat.SquaredDiffs,
			},
			SensitiveInfo: obspb.SensitiveInfo{
				LastErr:                 s.Stats.SensitiveInfo.LastErr,
				MostRecentPlanTimestamp: s.Stats.SensitiveInfo.MostRecentPlanTimestamp,
			},
			BytesRead: obspb.NumericStat{
				Mean:         s.Stats.BytesRead.Mean,
				SquaredDiffs: s.Stats.BytesRead.SquaredDiffs,
			},
			RowsRead: obspb.NumericStat{
				Mean:         s.Stats.RowsRead.Mean,
				SquaredDiffs: s.Stats.RowsRead.SquaredDiffs,
			},
			RowsWritten: obspb.NumericStat{
				Mean:         s.Stats.RowsWritten.Mean,
				SquaredDiffs: s.Stats.RowsWritten.SquaredDiffs,
			},
			ExecStats: obspb.ExecStats{
				Count: s.Stats.ExecStats.Count,
				NetworkBytes: obspb.NumericStat{
					Mean:         s.Stats.ExecStats.NetworkBytes.Mean,
					SquaredDiffs: s.Stats.ExecStats.NetworkBytes.SquaredDiffs,
				},
				MaxMemUsage: obspb.NumericStat{
					Mean:         s.Stats.ExecStats.MaxMemUsage.Mean,
					SquaredDiffs: s.Stats.ExecStats.MaxMemUsage.SquaredDiffs,
				},
				ContentionTime: obspb.NumericStat{
					Mean:         s.Stats.ExecStats.ContentionTime.Mean,
					SquaredDiffs: s.Stats.ExecStats.ContentionTime.SquaredDiffs,
				},
				NetworkMessages: obspb.NumericStat{
					Mean:         s.Stats.ExecStats.NetworkMessages.Mean,
					SquaredDiffs: s.Stats.ExecStats.NetworkMessages.SquaredDiffs,
				},
				MaxDiskUsage: obspb.NumericStat{
					Mean:         s.Stats.ExecStats.MaxDiskUsage.Mean,
					SquaredDiffs: s.Stats.ExecStats.MaxDiskUsage.SquaredDiffs,
				},
				CPUSQLNanos: obspb.NumericStat{
					Mean:         s.Stats.ExecStats.CPUSQLNanos.Mean,
					SquaredDiffs: s.Stats.ExecStats.CPUSQLNanos.SquaredDiffs,
				},
				MVCCIteratorStats: obspb.MVCCIteratorStats{
					StepCount: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.StepCount.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.StepCount.SquaredDiffs,
					},
					StepCountInternal: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.StepCountInternal.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.StepCountInternal.SquaredDiffs,
					},
					SeekCount: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.SeekCount.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.SeekCount.SquaredDiffs,
					},
					SeekCountInternal: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal.SquaredDiffs,
					},
					BlockBytes: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.BlockBytes.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.BlockBytes.SquaredDiffs,
					},
					BlockBytesInCache: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache.SquaredDiffs,
					},
					KeyBytes: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.KeyBytes.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.KeyBytes.SquaredDiffs,
					},
					ValueBytes: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.ValueBytes.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.ValueBytes.SquaredDiffs,
					},
					PointCount: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.PointCount.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.PointCount.SquaredDiffs,
					},
					PointsCoveredByRangeTombstones: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.SquaredDiffs,
					},
					RangeKeyCount: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount.SquaredDiffs,
					},
					RangeKeyContainedPoints: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.SquaredDiffs,
					},
					RangeKeySkippedPoints: obspb.NumericStat{
						Mean:         s.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.Mean,
						SquaredDiffs: s.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.SquaredDiffs,
					},
				},
			},
			SQLType:              s.Stats.SQLType,
			LastExecTimestamp:    s.Stats.LastExecTimestamp,
			Nodes:                s.Stats.Nodes,
			Regions:              s.Stats.Regions,
			PlanGists:            s.Stats.PlanGists,
			IndexRecommendations: s.Stats.IndexRecommendations,
			Indexes:              s.Stats.Indexes,
			LatencyInfo: obspb.LatencyInfo{
				Min: s.Stats.LatencyInfo.Min,
				Max: s.Stats.LatencyInfo.Max,
				P50: s.Stats.LatencyInfo.P50,
				P90: s.Stats.LatencyInfo.P90,
				P99: s.Stats.LatencyInfo.P99,
			},
			LastErrorCode: s.Stats.LastErrorCode,
		},
		AggregatedTs:        s.AggregatedTs,
		AggregationInterval: s.AggregationInterval,
	}
	out.Stats.SensitiveInfo.MostRecentPlanDescription = *transformTreePlanNodeRecursive(&s.Stats.SensitiveInfo.MostRecentPlanDescription)
	return out
}

func transformTreePlanNodeRecursive(p *ExplainTreePlanNode) *obspb.ExplainTreePlanNode {
	out := &obspb.ExplainTreePlanNode{
		Name: p.Name,
	}
	for _, attr := range p.Attrs {
		out.Attrs = append(
			out.Attrs,
			&obspb.ExplainTreePlanNode_Attr{
				Key:   attr.Key,
				Value: attr.Value,
			},
		)
	}
	for _, child := range p.Children {
		out.Children = append(out.Children, transformTreePlanNodeRecursive(child))
	}
	return out
}
