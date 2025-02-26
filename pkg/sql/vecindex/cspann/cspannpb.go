// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"fmt"
	"math"
	"slices"
)

// Clone returns a deep copy of the stats. Changes to the original or clone do
// not affect the other.
func (s *IndexStats) Clone() IndexStats {
	return IndexStats{
		NumPartitions:       s.NumPartitions,
		VectorsPerPartition: s.VectorsPerPartition,
		CVStats:             slices.Clone(s.CVStats),
	}
}

// String returns a human-readable representation of the index stats.
func (s *IndexStats) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%d levels, %d partitions, %.2f vectors/partition.\n",
		len(s.CVStats)+1, s.NumPartitions, s.VectorsPerPartition))
	buf.WriteString("CV stats:\n")
	for i, cvstats := range s.CVStats {
		stdev := math.Sqrt(cvstats.Variance)
		buf.WriteString(fmt.Sprintf("  level %d - mean: %.4f, stdev: %.4f\n",
			i+2, cvstats.Mean, stdev))
	}
	return buf.String()
}

// Equal returns true if this key has the same partition key and primary key as
// the given key.
func (k ChildKey) Equal(other ChildKey) bool {
	return k.PartitionKey == other.PartitionKey && bytes.Equal(k.KeyBytes, other.KeyBytes)
}

// Compare returns an integer comparing two child keys. The result is zero if
// the two are equal, -1 if this key is less than the other, and +1 if this key
// is greater than the other.
func (k ChildKey) Compare(other ChildKey) int {
	if k.KeyBytes != nil {
		return bytes.Compare(k.KeyBytes, other.KeyBytes)
	}
	if k.PartitionKey < other.PartitionKey {
		return -1
	} else if k.PartitionKey > other.PartitionKey {
		return 1
	}
	return 0
}
