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

// Add adds the fields from other IndexUsageStatistics.
func (m *IndexUsageStatistics) Add(other *IndexUsageStatistics) {
	m.TotalRowsRead += other.TotalRowsRead
	m.TotalRowsWritten += other.TotalRowsWritten

	m.TotalReadCount += other.TotalReadCount
	m.TotalWriteCount += other.TotalWriteCount

	if m.LastWrite.Before(other.LastWrite) {
		m.LastWrite = other.LastWrite
	}

	if m.LastRead.Before(other.LastRead) {
		m.LastRead = other.LastRead
	}
}
