// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

// TableID is same as descpb.ID. We redefine it here to avoid importing descpb.
type TableID uint32

// IndexID is same as descpb.IndexID. We redefine it here to avoid importing
// descpb.
type IndexID uint32

// Add add the fields from other IndexUsageStatistics.
func (m *IndexUsageStatistics) Add(other *IndexUsageStatistics) {
	m.FullScanCount += other.FullScanCount
	m.NonFullScanCount += other.NonFullScanCount
	m.LookupJoinCount += other.LookupJoinCount
	m.ZigzagJoinCount += other.ZigzagJoinCount
	m.InvertedJoinCount += other.InvertedJoinCount

	if m.LastScan.Before(other.LastScan) {
		m.LastScan = other.LastScan
	}

	m.TotalScanRows += other.TotalScanRows
	m.UpdateCounts += other.UpdateCounts
	m.TotalUpdatedRows += other.TotalUpdatedRows

	if m.LastUpdate.Before(other.LastUpdate) {
		m.LastUpdate = other.LastUpdate
	}

	if m.LastJoin.Before(other.LastJoin) {
		m.LastJoin = other.LastJoin
	}
}
