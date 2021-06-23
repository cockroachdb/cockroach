// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/util"
)

// StmtFingerprintID is the type of a Statement's fingerprint ID.
type StmtFingerprintID uint64

// ConstructStatementFingerprintID constructs an ID by hashing an anonymized query, its database
// and failure status, and if it was part of an implicit txn. At the time of writing,
// these are the axis' we use to bucket queries for stats collection
// (see stmtKey).
func ConstructStatementFingerprintID(
	anonymizedStmt string, failed bool, implicitTxn bool, database string,
) StmtFingerprintID {
	fnv := util.MakeFNV64()
	for _, c := range anonymizedStmt {
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

	t.CommitLat.Add(other.CommitLat, t.Count, other.Count)
	t.RetryLat.Add(other.RetryLat, t.Count, other.Count)
	t.ServiceLat.Add(other.ServiceLat, t.Count, other.Count)
	t.NumRows.Add(other.NumRows, t.Count, other.Count)
	t.RowsRead.Add(other.RowsRead, t.Count, other.Count)
	t.BytesRead.Add(other.BytesRead, t.Count, other.Count)

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
	s.ParseLat.Add(other.ParseLat, s.Count, other.Count)
	s.PlanLat.Add(other.PlanLat, s.Count, other.Count)
	s.RunLat.Add(other.RunLat, s.Count, other.Count)
	s.ServiceLat.Add(other.ServiceLat, s.Count, other.Count)
	s.OverheadLat.Add(other.OverheadLat, s.Count, other.Count)
	s.BytesRead.Add(other.BytesRead, s.Count, other.Count)
	s.RowsRead.Add(other.RowsRead, s.Count, other.Count)
	s.Nodes = util.CombineUniqueInt64(s.Nodes, other.Nodes)

	s.ExecStats.Add(other.ExecStats)

	if other.SensitiveInfo.LastErr != "" {
		s.SensitiveInfo.LastErr = other.SensitiveInfo.LastErr
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
		s.ParseLat.AlmostEqual(other.ParseLat, eps) &&
		s.PlanLat.AlmostEqual(other.PlanLat, eps) &&
		s.RunLat.AlmostEqual(other.RunLat, eps) &&
		s.ServiceLat.AlmostEqual(other.ServiceLat, eps) &&
		s.OverheadLat.AlmostEqual(other.OverheadLat, eps) &&
		s.SensitiveInfo.Equal(other.SensitiveInfo) &&
		s.BytesRead.AlmostEqual(other.BytesRead, eps) &&
		s.RowsRead.AlmostEqual(other.RowsRead, eps)
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

	s.Count += other.Count
}
