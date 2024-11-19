// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

// VecMaxOpenFDsLimit specifies the maximum number of open file descriptors
// that the vectorized engine can have (globally) for use of the temporary
// storage.
const VecMaxOpenFDsLimit = 256
