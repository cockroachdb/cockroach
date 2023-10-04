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

// CopyTo copies the values from this  this CollectedStatementStatistics into an the provided other
// obspb.AggregatedStatementStatistics proto.
//
// The purpose of having a separate protobuf is to protect the external o11y systems from being impacted
// by changes to CollectedStatementStatistics
//
// TODO(abarganier): What kind of redaction needs to happen here? Should we omit the Stats.SensitiveData?
func (s *CollectedStatementStatistics) CopyTo(other *obspb.AggregatedStatementStatistics) {
	other.ID = uint64(s.ID)
	other.Key.Query = s.Key.Query
	other.Key.App = s.Key.App
	other.Key.DistSQL = s.Key.DistSQL
	other.Key.Failed = s.Key.Failed
	other.Key.ImplicitTxn = s.Key.ImplicitTxn
	other.Key.Vec = s.Key.Vec
	other.Key.FullScan = s.Key.FullScan
	other.Key.Database = s.Key.Database
	other.Key.PlanHash = s.Key.PlanHash
	other.Key.QuerySummary = s.Key.QuerySummary
	other.Key.TransactionFingerprintID = uint64(s.Key.TransactionFingerprintID)
	other.Stats.Count = s.Stats.Count
	other.Stats.FirstAttemptCount = s.Stats.FirstAttemptCount
	other.Stats.MaxRetries = s.Stats.MaxRetries
	other.Stats.NumRows.Mean = s.Stats.NumRows.Mean
	other.Stats.NumRows.SquaredDiffs = s.Stats.NumRows.SquaredDiffs
	other.Stats.IdleLat.Mean = s.Stats.IdleLat.Mean
	other.Stats.IdleLat.SquaredDiffs = s.Stats.IdleLat.SquaredDiffs
	other.Stats.ParseLat.Mean = s.Stats.ParseLat.Mean
	other.Stats.ParseLat.SquaredDiffs = s.Stats.ParseLat.SquaredDiffs
	other.Stats.PlanLat.Mean = s.Stats.PlanLat.Mean
	other.Stats.PlanLat.SquaredDiffs = s.Stats.PlanLat.SquaredDiffs
	other.Stats.RunLat.Mean = s.Stats.RunLat.Mean
	other.Stats.RunLat.SquaredDiffs = s.Stats.RunLat.SquaredDiffs
	other.Stats.ServiceLat.Mean = s.Stats.ServiceLat.Mean
	other.Stats.ServiceLat.SquaredDiffs = s.Stats.ServiceLat.SquaredDiffs
	other.Stats.OverheadLat.Mean = s.Stats.OverheadLat.Mean
	other.Stats.OverheadLat.SquaredDiffs = s.Stats.OverheadLat.SquaredDiffs
	other.Stats.BytesRead.Mean = s.Stats.BytesRead.Mean
	other.Stats.BytesRead.SquaredDiffs = s.Stats.BytesRead.SquaredDiffs
	other.Stats.RowsRead.Mean = s.Stats.RowsRead.Mean
	other.Stats.RowsRead.SquaredDiffs = s.Stats.RowsRead.SquaredDiffs
	other.Stats.RowsWritten.Mean = s.Stats.RowsWritten.Mean
	other.Stats.RowsWritten.SquaredDiffs = s.Stats.RowsWritten.SquaredDiffs
	other.Stats.ExecStats.Count = s.Stats.ExecStats.Count
	other.Stats.ExecStats.NetworkBytes.Mean = s.Stats.ExecStats.NetworkBytes.Mean
	other.Stats.ExecStats.NetworkBytes.SquaredDiffs = s.Stats.ExecStats.NetworkBytes.SquaredDiffs
	other.Stats.ExecStats.MaxMemUsage.Mean = s.Stats.ExecStats.MaxMemUsage.Mean
	other.Stats.ExecStats.MaxMemUsage.SquaredDiffs = s.Stats.ExecStats.MaxMemUsage.SquaredDiffs
	other.Stats.ExecStats.ContentionTime.Mean = s.Stats.ExecStats.ContentionTime.Mean
	other.Stats.ExecStats.ContentionTime.SquaredDiffs = s.Stats.ExecStats.ContentionTime.SquaredDiffs
	other.Stats.ExecStats.NetworkMessages.Mean = s.Stats.ExecStats.NetworkMessages.Mean
	other.Stats.ExecStats.NetworkMessages.SquaredDiffs = s.Stats.ExecStats.NetworkMessages.SquaredDiffs
	other.Stats.ExecStats.MaxDiskUsage.Mean = s.Stats.ExecStats.MaxDiskUsage.Mean
	other.Stats.ExecStats.MaxDiskUsage.SquaredDiffs = s.Stats.ExecStats.MaxDiskUsage.SquaredDiffs
	other.Stats.ExecStats.CPUSQLNanos.Mean = s.Stats.ExecStats.CPUSQLNanos.Mean
	other.Stats.ExecStats.CPUSQLNanos.SquaredDiffs = s.Stats.ExecStats.CPUSQLNanos.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.StepCount.Mean = s.Stats.ExecStats.MVCCIteratorStats.StepCount.Mean
	other.Stats.ExecStats.MVCCIteratorStats.StepCount.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.StepCount.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.StepCountInternal.Mean = s.Stats.ExecStats.MVCCIteratorStats.StepCountInternal.Mean
	other.Stats.ExecStats.MVCCIteratorStats.StepCountInternal.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.StepCountInternal.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.SeekCount.Mean = s.Stats.ExecStats.MVCCIteratorStats.SeekCount.Mean
	other.Stats.ExecStats.MVCCIteratorStats.SeekCount.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.SeekCount.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal.Mean = s.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal.Mean
	other.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.BlockBytes.Mean = s.Stats.ExecStats.MVCCIteratorStats.BlockBytes.Mean
	other.Stats.ExecStats.MVCCIteratorStats.BlockBytes.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.BlockBytes.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache.Mean = s.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache.Mean
	other.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.KeyBytes.Mean = s.Stats.ExecStats.MVCCIteratorStats.KeyBytes.Mean
	other.Stats.ExecStats.MVCCIteratorStats.KeyBytes.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.KeyBytes.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.ValueBytes.Mean = s.Stats.ExecStats.MVCCIteratorStats.ValueBytes.Mean
	other.Stats.ExecStats.MVCCIteratorStats.ValueBytes.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.ValueBytes.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.PointCount.Mean = s.Stats.ExecStats.MVCCIteratorStats.PointCount.Mean
	other.Stats.ExecStats.MVCCIteratorStats.PointCount.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.PointCount.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.Mean = s.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.Mean
	other.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount.Mean = s.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount.Mean
	other.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.Mean = s.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.Mean
	other.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints.SquaredDiffs
	other.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.Mean = s.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.Mean
	other.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.SquaredDiffs = s.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints.SquaredDiffs
	other.Stats.SQLType = s.Stats.SQLType
	other.Stats.LastExecTimestamp = s.Stats.LastExecTimestamp
	other.Stats.Nodes = s.Stats.Nodes
	other.Stats.Regions = s.Stats.Regions
	other.Stats.PlanGists = s.Stats.PlanGists
	other.Stats.IndexRecommendations = s.Stats.IndexRecommendations
	other.Stats.Indexes = s.Stats.Indexes
	other.Stats.LatencyInfo.Min = s.Stats.LatencyInfo.Min
	other.Stats.LatencyInfo.Max = s.Stats.LatencyInfo.Max
	other.Stats.LatencyInfo.P50 = s.Stats.LatencyInfo.P50
	other.Stats.LatencyInfo.P90 = s.Stats.LatencyInfo.P90
	other.Stats.LatencyInfo.P99 = s.Stats.LatencyInfo.P99
	other.Stats.LastErrorCode = s.Stats.LastErrorCode
	other.AggregatedTs = s.AggregatedTs
	other.AggregationInterval = s.AggregationInterval
	other.Stats.SensitiveInfo.LastErr = s.Stats.SensitiveInfo.LastErr
	other.Stats.SensitiveInfo.MostRecentPlanTimestamp = s.Stats.SensitiveInfo.MostRecentPlanTimestamp
	s.Stats.SensitiveInfo.MostRecentPlanDescription.CopyToRecursive(&other.Stats.SensitiveInfo.MostRecentPlanDescription)
}

// CopyToRecursive copies all data from this ExplainPlanTreeNode into other. This is
// done so recursively for all Children.
func (p *ExplainTreePlanNode) CopyToRecursive(other *obspb.ExplainTreePlanNode) {
	other.Name = p.Name
	// Ensure Attr slices are equivalent in length
	if len(p.Attrs) < len(other.Attrs) {
		other.Attrs = other.Attrs[0:len(p.Attrs)]
	}
	if len(p.Attrs) > len(other.Attrs) {
		for i := len(other.Attrs); i < len(p.Attrs); i++ {
			other.Attrs = append(other.Attrs, new(obspb.ExplainTreePlanNode_Attr))
		}
	}
	for i, attr := range p.Attrs {
		// No guarantee that each element in other is defined, so instantiate if necessary.
		if other.Attrs[i] == nil {
			other.Attrs[i] = new(obspb.ExplainTreePlanNode_Attr)
		}
		other.Attrs[i].Key = attr.Key
		other.Attrs[i].Value = attr.Value
	}

	// Now ensure Children slices are equivalent in length.
	if len(p.Children) < len(other.Children) {
		other.Children = other.Children[0:len(p.Children)]
	}
	if len(p.Children) > len(other.Children) {
		for i := len(other.Children); i < len(p.Children); i++ {
			other.Children = append(other.Children, new(obspb.ExplainTreePlanNode))
		}
	}
	for i, child := range p.Children {
		// Also ensure that each element is defined, and instantiate if necessary.
		if other.Children[i] == nil {
			other.Children[i] = new(obspb.ExplainTreePlanNode)
		}
		child.CopyToRecursive(other.Children[i])
	}
}
