// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

// DefaultVectorizeRowCountThreshold denotes the default row count threshold.
// When it is exceeded, the vectorized execution engine will be used if
// possible.
// Note: if you are updating this field, please make sure to update
// vectorize_threshold logic test.
// TODO(yuzefovich): run the benchmarks to figure out this number.
const DefaultVectorizeRowCountThreshold = 10000
