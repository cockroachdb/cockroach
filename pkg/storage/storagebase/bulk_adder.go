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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// BulkAdderFactory describes a factory function for BulkAdders.
type BulkAdderFactory func(
	ctx context.Context, db *client.DB, flushBytes int64, timestamp hlc.Timestamp,
) (BulkAdder, error)

// BulkAdder describes a bulk-adding helper that can be used to add lots of KVs.
type BulkAdder interface {
	// Add adds a KV pair to the adder's buffer, potentially flushing if needed.
	Add(ctx context.Context, key roachpb.Key, value []byte) error
	// Flush explicitly flushes anything remaining in the adder's buffer.
	Flush(ctx context.Context) error
	// GetSummary returns a summary of rows/bytes/etc written by this batcher.
	GetSummary() roachpb.BulkOpSummary
	// Close closes the underlying buffers/writers.
	Close()
	// Reset resets the bulk-adder, returning it to its initial state.
	Reset() error
}

// CheckIngestedStats sends consistency checks for the ranges in the ingested
// span. This will verify and fixup any stats that were estimated during ingest.
func CheckIngestedStats(ctx context.Context, db *client.DB, ingested roachpb.Span) error {
	if ingested.Key == nil {
		return nil
	}
	if ingested.Key.Equal(ingested.EndKey) {
		ingested.EndKey = ingested.EndKey.Next()
	}
	// TODO(dt): This will compute stats twice when there is a delta once to find
	// the delta and then again to fix it. We could potentially just skip to the
	// fixing but Recompute is a bit harder to use from a client on an entire
	// table since it is a point request, while distsender will fan this one out.
	req := &roachpb.CheckConsistencyRequest{RequestHeader: roachpb.RequestHeaderFromSpan(ingested)}
	_, pErr := client.SendWrapped(ctx, db.NonTransactionalSender(), req)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
}
