// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package gossipccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestDisableMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	testCases := []struct {
		tableIDs []uint32
	}{
		{tableIDs: nil},
		{tableIDs: []uint32{0}},
		{tableIDs: []uint32{1, 2, 9, 10}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			g := gossip.NewTest(1, nil /* rpcContext */, nil, /* grpcServer */
				stopper, metric.NewRegistry())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			DisableMerges(ctx, g, c.tableIDs)
			for _, id := range c.tableIDs {
				key := gossip.MakeTableDisableMergesKey(id)
				if _, err := g.GetInfo(key); err != nil {
					t.Fatalf("expected to find %s, but got %v", key, err)
				}
			}
		})
	}
}
