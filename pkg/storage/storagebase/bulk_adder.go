// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storagebase

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// BulkAdderFactory describes a factory function for BulkAdders.
type BulkAdderFactory func(
	ctx context.Context, db *client.DB, bufferBytes, flushBytes int64, timestamp hlc.Timestamp,
) (BulkAdder, error)

// BulkAdder describes a bulk-adding helper that can be used to add lots of KVs.
type BulkAdder interface {
	// Add adds a KV pair to the adder's buffer, potentially flushing if needed.
	Add(ctx context.Context, key roachpb.Key, value []byte) error
	// Flush explicitly flushes anything remaining in the adder's buffer.
	Flush(ctx context.Context) error
	// CurrentBufferFill returns how full the configured buffer is.
	CurrentBufferFill() float32
	// GetSummary returns a summary of rows/bytes/etc written by this batcher.
	GetSummary() roachpb.BulkOpSummary
	// Close closes the underlying buffers/writers.
	Close(ctx context.Context)
	// SkipLocalDuplicates configures handling of duplicate keys within a local
	// sorted batch. Once a batch is flushed – explicitly or automatically – local
	// duplicate detection does not apply.
	SkipLocalDuplicates(bool)
}

// DuplicateKeyError represents a failed attempt to ingest the same key twice
// using a BulkAdder within the same batch.
type DuplicateKeyError struct {
	Key   roachpb.Key
	Value []byte
}

func (d DuplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key: %s", d.Key)
}
