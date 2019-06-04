// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package gossip

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
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
				stopper, metric.NewRegistry(), config.DefaultZoneConfigRef())
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
