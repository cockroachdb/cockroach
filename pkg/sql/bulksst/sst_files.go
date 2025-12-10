// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

// Append appends the contents of other to this SSTFiles structure.
// It combines the SST slices, row samples, and sums the total sizes.
func (s *SSTFiles) Append(other *SSTFiles) {
	if other == nil {
		return
	}
	s.SST = append(s.SST, other.SST...)
	s.RowSamples = append(s.RowSamples, other.RowSamples...)
	s.TotalSize += other.TotalSize
}
