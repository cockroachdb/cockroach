// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstatstypes

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// QueryLevelStats returns all the query level stats that correspond to the
// given traces and flow metadata.
// NOTE: When adding fields to this struct, be sure to update Accumulate.
type QueryLevelStats struct {
	DistSQLNetworkBytesSent            int64
	MaxMemUsage                        int64
	MaxDiskUsage                       int64
	KVBytesRead                        int64
	KVPairsRead                        int64
	KVRowsRead                         int64
	KVBatchRequestsIssued              int64
	KVTime                             time.Duration
	MvccSteps                          int64
	MvccStepsInternal                  int64
	MvccSeeks                          int64
	MvccSeeksInternal                  int64
	MvccBlockBytes                     int64
	MvccBlockBytesInCache              int64
	MvccKeyBytes                       int64
	MvccValueBytes                     int64
	MvccPointCount                     int64
	MvccPointsCoveredByRangeTombstones int64
	MvccRangeKeyCount                  int64
	MvccRangeKeyContainedPoints        int64
	MvccRangeKeySkippedPoints          int64
	DistSQLNetworkMessages             int64
	ContentionTime                     time.Duration
	LockWaitTime                       time.Duration
	LatchWaitTime                      time.Duration
	ContentionEvents                   []kvpb.ContentionEvent
	RUEstimate                         float64
	CPUTime                            time.Duration
	// SQLInstanceIDs is an ordered list of SQL instances that were involved in
	// query processing.
	SQLInstanceIDs []int32
	// KVNodeIDs is an ordered list of KV Node IDs that were used for KV reads
	// while processing the query.
	KVNodeIDs []int32
	// Regions is an ordered list of regions in which both SQL and KV nodes
	// involved in query processing reside.
	Regions []string
	// UsedFollowerRead indicates whether at least some reads were served by the
	// follower replicas.
	UsedFollowerRead bool
	ClientTime       time.Duration
}

// Accumulate accumulates other's stats into the receiver.
func (s *QueryLevelStats) Accumulate(other QueryLevelStats) {
	s.DistSQLNetworkBytesSent += other.DistSQLNetworkBytesSent
	if other.MaxMemUsage > s.MaxMemUsage {
		s.MaxMemUsage = other.MaxMemUsage
	}
	if other.MaxDiskUsage > s.MaxDiskUsage {
		s.MaxDiskUsage = other.MaxDiskUsage
	}
	s.KVBytesRead += other.KVBytesRead
	s.KVPairsRead += other.KVPairsRead
	s.KVRowsRead += other.KVRowsRead
	s.KVBatchRequestsIssued += other.KVBatchRequestsIssued
	s.KVTime += other.KVTime
	s.MvccSteps += other.MvccSteps
	s.MvccStepsInternal += other.MvccStepsInternal
	s.MvccSeeks += other.MvccSeeks
	s.MvccSeeksInternal += other.MvccSeeksInternal
	s.MvccBlockBytes += other.MvccBlockBytes
	s.MvccBlockBytesInCache += other.MvccBlockBytesInCache
	s.MvccKeyBytes += other.MvccKeyBytes
	s.MvccValueBytes += other.MvccValueBytes
	s.MvccPointCount += other.MvccPointCount
	s.MvccPointsCoveredByRangeTombstones += other.MvccPointsCoveredByRangeTombstones
	s.MvccRangeKeyCount += other.MvccRangeKeyCount
	s.MvccRangeKeyContainedPoints += other.MvccRangeKeyContainedPoints
	s.MvccRangeKeySkippedPoints += other.MvccRangeKeySkippedPoints
	s.DistSQLNetworkMessages += other.DistSQLNetworkMessages
	s.ContentionTime += other.ContentionTime
	s.LockWaitTime += other.LockWaitTime
	s.LatchWaitTime += other.LatchWaitTime
	s.ContentionEvents = append(s.ContentionEvents, other.ContentionEvents...)
	s.RUEstimate += other.RUEstimate
	s.CPUTime += other.CPUTime
	s.SQLInstanceIDs = util.CombineUnique(s.SQLInstanceIDs, other.SQLInstanceIDs)
	s.KVNodeIDs = util.CombineUnique(s.KVNodeIDs, other.KVNodeIDs)
	s.Regions = util.CombineUnique(s.Regions, other.Regions)
	s.UsedFollowerRead = s.UsedFollowerRead || other.UsedFollowerRead
	s.ClientTime += other.ClientTime
}
