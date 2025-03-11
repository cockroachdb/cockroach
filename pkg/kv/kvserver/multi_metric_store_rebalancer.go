// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mma"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type multiMetricStoreRebalancer struct {
	// allocator mma.Allocator
	allocator mma.Allocator
	store     *Store
	st        *cluster.Settings
}

func (m *multiMetricStoreRebalancer) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "multi-metric-store-rebalancer", func(ctx context.Context) {
		var timer timeutil.Timer
		defer timer.Stop()

		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some stats to accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(jitteredInterval(mma.RebalanceInterval))
			}
			if LoadBasedRebalancingMode.Get(&m.st.SV) != LBRebalancingMultiMetric {
				continue
			}
			// TODO(kvoli): Call ComputeChanges, implement the change enactment and
			// reject/success handling. e.g.,
			//
			// changes := m.allocator.ComputeChanges(mma.ChangeOptions{
			// 	LocalStoreID: m.store.StoreID(),
			// })
			// for _, change := range changes {
			// 	if change.IsTransferLease() {
			// 	} else if change.IsChangeReplicas() {
			// 	} else {
			// 		panic("unexpected change type")
			// 	}
			// }
		}
	})
}
