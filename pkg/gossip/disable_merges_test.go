// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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
			g := NewTest(1, nil /* rpcContext */, nil, /* grpcServer */
				stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			g.DisableMerges(ctx, c.tableIDs)
			for _, id := range c.tableIDs {
				key := MakeTableDisableMergesKey(id)
				if _, err := g.GetInfo(key); err != nil {
					t.Fatalf("expected to find %s, but got %v", key, err)
				}
			}
		})
	}
}
