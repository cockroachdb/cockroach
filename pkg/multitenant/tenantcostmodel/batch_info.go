// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

// BatchInfo records the number and size of read and write batches that are
// submitted to the KV layer. Each batch consists of one or more read or write
// operations. The total size of the batch is the total of the size of all its
// operations.
type BatchInfo struct {
	// ReadCount is the number of reads that were batched together. This is 0 if
	// it is a write-only batch.
	ReadCount int64
	// ReadBytes is the total size of all batched reads in the response, in
	// bytes, or 0 if it is a write-only batch.
	ReadBytes int64
	// WriteCount is the number of writes that were batched together. This is 0
	// if it is a read-only batch.
	WriteCount int64
	// WriteBytes is the total size of all batched writes in the request, in
	// bytes, or 0 if it is a read-only batch.
	WriteBytes int64
}
