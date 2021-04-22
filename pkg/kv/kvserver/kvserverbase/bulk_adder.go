// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverbase

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// BulkAdderOptions is used to configure the behavior of a BulkAdder.
type BulkAdderOptions struct {
	// Name is used in logging messages to identify this adder or the process on
	// behalf of which it is adding data.
	Name string

	// SSTSize is the size at which an SST will be flushed and a new one started.
	// SSTs are also split during a buffer flush to avoid spanning range bounds so
	// they may be smaller than this limit.
	SSTSize func() int64

	// SplitAndScatterAfter is the number of bytes which if added without hitting
	// an existing split will cause the adder to split and scatter the next span.
	// A function returning -1 is interpreted as indicating not to split.
	SplitAndScatterAfter func() int64

	// MinBufferSize is the initial size of the BulkAdder buffer. It indicates the
	// amount of memory we require to be able to buffer data before flushing for
	// SST creation.
	MinBufferSize int64

	// BufferSize is the maximum size we can grow the BulkAdder buffer to.
	MaxBufferSize func() int64

	// StepBufferSize is the increment in which we will attempt to grow the
	// BulkAdder buffer if the memory monitor permits.
	StepBufferSize int64

	// SkipDuplicates configures handling of duplicate keys within a local sorted
	// batch. When true if the same key/value pair is added more than once
	// subsequent additions will be ignored instead of producing an error. If an
	// attempt to add the same key has a different value, it is always an error.
	// Once a batch is flushed – explicitly or automatically – local duplicate
	// detection does not apply.
	SkipDuplicates bool

	// DisallowShadowing controls whether shadowing of existing keys is permitted
	// when the SSTables produced by this adder are ingested.
	DisallowShadowing bool

	// BatchTimestamp is the timestamp to use on AddSSTable requests (which can be
	// different from the timestamp used to construct the adder which is what is
	// actually applied to each key).
	BatchTimestamp hlc.Timestamp
}

// DisableExplicitSplits can be returned by a SplitAndScatterAfter function to
// indicate that the SSTBatcher should not issue explicit splits.
const DisableExplicitSplits = -1

// BulkAdderFactory describes a factory function for BulkAdders.
type BulkAdderFactory func(
	ctx context.Context, db *kv.DB, timestamp hlc.Timestamp, opts BulkAdderOptions,
) (BulkAdder, error)

// BulkAdder describes a bulk-adding helper that can be used to add lots of KVs.
type BulkAdder interface {
	// Add adds a KV pair to the adder's buffer, potentially flushing if needed.
	Add(ctx context.Context, key roachpb.Key, value []byte) error
	// Flush explicitly flushes anything remaining in the adder's buffer.
	Flush(ctx context.Context) error
	// IsEmpty returns whether or not this BulkAdder has data buffered.
	IsEmpty() bool
	// CurrentBufferFill returns how full the configured buffer is.
	CurrentBufferFill() float32
	// GetSummary returns a summary of rows/bytes/etc written by this batcher.
	GetSummary() roachpb.BulkOpSummary
	// Close closes the underlying buffers/writers.
	Close(ctx context.Context)
	// SetOnFlush sets a callback function called after flushing the buffer.
	SetOnFlush(func())
}

// DuplicateKeyError represents a failed attempt to ingest the same key twice
// using a BulkAdder within the same batch.
type DuplicateKeyError struct {
	Key   roachpb.Key
	Value []byte
}

func (d *DuplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key: %s", d.Key)
}
