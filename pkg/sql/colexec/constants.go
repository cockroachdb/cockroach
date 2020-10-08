// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

// DefaultVectorizeRowCountThreshold denotes the default row count threshold.
// When it is met, the vectorized execution engine will be used if possible.
// The current number 1000 was chosen upon comparing `SELECT count(*) FROM t`
// query running through the row and the vectorized execution engines on a
// single node with tables having different number of columns.
// Note: if you are updating this field, please make sure to update
// vectorize_threshold logic test accordingly.
const DefaultVectorizeRowCountThreshold = 1000

// VecMaxOpenFDsLimit specifies the maximum number of open file descriptors
// that the vectorized engine can have (globally) for use of the temporary
// storage.
const VecMaxOpenFDsLimit = 256

const defaultMemoryLimit = 64 << 20 /* 64 MiB */
