// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecop

// ExternalSorterMinPartitions determines the minimum number of file descriptors
// that the external sorter needs to have in order to make progress (when
// merging, we have to merge at least two partitions into a new third one).
const ExternalSorterMinPartitions = 3
