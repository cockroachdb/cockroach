// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecop

// ExternalSorterMinPartitions determines the minimum number of file descriptors
// that the external sorter needs to have in order to make progress (when
// merging, we have to merge at least two partitions into a new third one).
const ExternalSorterMinPartitions = 3

// SortMergeNonSortMinFDsOpen determines the minimum number of file descriptors
// that are needed in the sort-merge fallback strategy of the external hash
// joiner outside of the needs of the external sorters.
const SortMergeNonSortMinFDsOpen = 2

// ExternalHJMinPartitions determines the minimum number of file descriptors
// that the external hash joiner needs to have in order to make progress.
//
// We need at least two buckets per side to make progress. However, the
// minimum number of partitions necessary are the partitions in use during a
// fallback to sort and merge join. We'll be using the minimum necessary per
// input + 2 (1 for each spilling queue that the merge joiner uses). For
// clarity this is what happens:
//   - The 2 partitions that need to be sorted + merged will use an FD each: 2
//     FDs. Meanwhile, each sorter will use up to ExternalSorterMinPartitions to
//     sort and partition this input. At this stage 2 + 2 *
//     ExternalSorterMinPartitions FDs are used.
//   - Once the inputs (the hash joiner partitions) are finished, both FDs will
//     be released. The merge joiner will now be in use, which uses two
//     spillingQueues with 1 FD each for a total of 2. Since each sorter will
//     use ExternalSorterMinPartitions, the FDs used at this stage are 2 +
//     (2 * ExternalSorterMinPartitions) as well. Note that as soon as the
//     sorter emits its first batch, it must be the case that the input to it
//     has returned a zero batch, and thus the FD has been closed.
const ExternalHJMinPartitions = SortMergeNonSortMinFDsOpen + (ExternalSorterMinPartitions * 2)
