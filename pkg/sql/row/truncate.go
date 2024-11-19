// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

// TableTruncateChunkSize is the maximum number of keys deleted per chunk
// during a table or index truncation.
const TableTruncateChunkSize = 600
