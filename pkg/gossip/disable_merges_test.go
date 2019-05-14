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
