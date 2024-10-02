// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
