// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func splitAndScatterSpans(ctx context.Context, db *kv.DB, spans []roachpb.Span) error {
	// TODO(jeffswenson): when distributing work, we should re-scatter ranges if
	// there are a small number of nodes with remaining work.
	for _, span := range spans {
		expirationTime := db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
		if err := db.AdminSplit(ctx, span.Key, expirationTime); err != nil {
			return err
		}
	}
	for _, span := range spans {
		req := &kvpb.AdminScatterRequest{
			// Randomly distributed ranges to make ingest more balanced.
			RequestHeader:   kvpb.RequestHeaderFromSpan(span),
			RandomizeLeases: true,
			MaxSize:         1,
		}
		if _, err := kv.SendWrapped(ctx, db.NonTransactionalSender(), req); err != nil {
			// Tolerate scatter errors since they are not critical to
			// correctness.
			log.Errorf(ctx, "failed to scatter span [%s,%s): %+v",
				span.Key, span.EndKey, err)
		}
	}
	return nil
}
