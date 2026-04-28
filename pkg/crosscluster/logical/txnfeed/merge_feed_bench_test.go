// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func BenchmarkMergeFeed(b *testing.B) {
	b.ReportAllocs()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	subs, coveringSpan, _ := generateMergeFeedInputs(b, rng, b.N, mergeFeedInputOptions{
		numSubs:    5,
		maxTxnSize: 256,
		density:    1 / (float64(b.N) / 5 / 10), // on average, 10 checkpoints per subscription
	})
	feed := NewMergeFeed(subs, coveringSpan, 128, hlc.MaxTimestamp)

	b.ResetTimer()

	group := ctxgroup.WithContext(context.Background())
	group.GoCtx(func(ctx context.Context) error {
		return feed.Subscribe(ctx)
	})
	group.GoCtx(func(ctx context.Context) error {
		for range feed.Events() {
		}
		return nil
	})
	_ = group.Wait()
}
