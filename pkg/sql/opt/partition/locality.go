// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package partition

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// PrefixIsLocal contains a PARTITION BY LIST Prefix, a boolean indicating
// whether the Prefix is from a local partition, and the name of the partition.
type PrefixIsLocal struct {
	Prefix  tree.Datums
	IsLocal bool

	partitionName string
}

// PrefixSorter sorts prefixes (which are wrapped in PrefixIsLocal structs) so
// that longer prefixes are ordered first and within each group of equal-length
// prefixes so that they are ordered by value.
type PrefixSorter struct {
	EvalCtx *tree.EvalContext
	Entry   []PrefixIsLocal

	// A slice of indices of the last element of each equal-length group of
	// entries in the Entry array above. Used by Slice(i int).
	idx []int

	// The set of local partitions
	LocalPartitions util.FastIntSet

	// A slice of local partition prefixes which can be used to construct
	// covering spans for better detection of local spans.
	LocalPrefixes []tree.Datums
}

var _ sort.Interface = &PrefixSorter{}

// Len is part of sort.Interface.
func (ps PrefixSorter) Len() int {
	return len(ps.Entry)
}

// Less is part of sort.Interface.
func (ps PrefixSorter) Less(i, j int) bool {
	if len(ps.Entry[i].Prefix) != len(ps.Entry[j].Prefix) {
		return len(ps.Entry[i].Prefix) > len(ps.Entry[j].Prefix)
	}
	// A zero length prefix is never less than any prefix.
	if len(ps.Entry[i].Prefix) == 0 {
		return false
	}
	compareResult := ps.Entry[i].Prefix.Compare(ps.EvalCtx, ps.Entry[j].Prefix)
	return compareResult == -1
}

// Swap is part of sort.Interface.
func (ps PrefixSorter) Swap(i, j int) {
	ps.Entry[i], ps.Entry[j] = ps.Entry[j], ps.Entry[i]
}

// Slice returns the ith slice of PrefixIsLocal entries, all having the same
// partition prefix length, along with the start index of that slice in the
// main PrefixSorter Entry slice. Slices are sorted by prefix length with those
// of the longest prefix length occurring at i==0.
func (ps PrefixSorter) Slice(i int) (prefixSplice []PrefixIsLocal, sliceStartIndex int, ok bool) {
	if i < 0 || i >= len(ps.idx) {
		return nil, -1, false
	}
	inclusiveStartIndex := 0
	if i > 0 {
		// The start of the slice is the end of the previous slice plus one.
		inclusiveStartIndex = ps.idx[i-1] + 1
	}
	nonInclusiveEndIndex := ps.idx[i] + 1
	if (nonInclusiveEndIndex < inclusiveStartIndex) || (nonInclusiveEndIndex > len(ps.Entry)) {
		panic(errors.AssertionFailedf("Partition prefix slice not found. inclusiveStartIndex = %d, nonInclusiveEndIndex = %d",
			inclusiveStartIndex, nonInclusiveEndIndex))
	}
	return ps.Entry[inclusiveStartIndex:nonInclusiveEndIndex], inclusiveStartIndex, true
}

// GetSortedPrefixes collects all the prefixes from all the different partitions
// in the index (remembering which ones came from local partitions), and sorts
// them so that longer prefixes come before shorter prefixes and within each
// group of equal-length prefixes they are ordered by value.
func GetSortedPrefixes(
	index cat.Index, localPartitions util.FastIntSet, evalCtx *tree.EvalContext,
) *PrefixSorter {
	if index == nil || index.PartitionCount() < 2 {
		return nil
	}
	allPrefixes := make([]PrefixIsLocal, 0, index.PartitionCount())
	localPrefixes := make([]tree.Datums, 0, localPartitions.Len())

	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)
		isLocal := localPartitions.Contains(i)
		partitionPrefixes := part.PartitionByListPrefixes()
		if len(partitionPrefixes) == 0 {
			// This can happen when the partition value is DEFAULT.
			allPrefixes = append(allPrefixes, PrefixIsLocal{
				Prefix:        nil,
				IsLocal:       isLocal,
				partitionName: part.Name(),
			})
		}
		for j := range partitionPrefixes {
			allPrefixes = append(allPrefixes, PrefixIsLocal{
				Prefix:        partitionPrefixes[j],
				IsLocal:       isLocal,
				partitionName: part.Name(),
			})
			if isLocal && len(partitionPrefixes[j]) > 0 {
				localPrefixes = append(localPrefixes, partitionPrefixes[j])
			}
		}
	}
	ps := PrefixSorter{evalCtx, allPrefixes, []int{}, localPartitions, localPrefixes}
	sort.Sort(ps)
	lastPrefixLength := len(ps.Entry[0].Prefix)
	// Mark the index of each prefix group of a different length.
	// We must search each group separately, so the boundaries need tagging.
	for i := 1; i < len(allPrefixes); i++ {
		if len(ps.Entry[i].Prefix) != lastPrefixLength {
			ps.idx = append(ps.idx, i-1)
			lastPrefixLength = len(ps.Entry[i].Prefix)
		}
	}
	ps.LocalPartitions = localPartitions

	// The end of the last slice is always the last element.
	ps.idx = append(ps.idx, len(allPrefixes)-1)
	return &ps
}
