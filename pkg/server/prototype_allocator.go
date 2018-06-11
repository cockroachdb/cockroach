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

package server

import (
	"container/heap"
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var testQPSThreshold = settings.RegisterNonNegativeFloatSetting(
	"server.test_qps_threshold",
	"the maximum fraction a store's qps can differ from the average before store-level rebalancing kicks in",
	0.25,
)

func (s *Server) RunStoreLevelAllocator(ctx context.Context) {
	if s.NodeID() != 1 {
		return
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		// Wait out the first tick before doing anything since the store is still
		// starting up and we might as well wait for some qps/wps stats to
		// accumulate.
		select {
		case <-s.stopper.ShouldQuiesce():
			return
		case <-ticker.C:
		}

		log.Infof(ctx, "starting prototype allocator loop")

		resp, err := s.status.RaftDebug(ctx, &serverpb.RaftDebugRequest{})
		if err != nil {
			log.Errorf(ctx, "failed to retrieve raft debug info: %s", err)
			continue
		}

		qpsPerStore, hottestRangesByStore := processResponse(resp)
		if len(qpsPerStore) == 0 {
			log.Infof(ctx, "received no stores to process: %+v", resp)
			continue
		}

		log.Infof(ctx, "qpsPerStore: %v", qpsPerStore)

		var avgQPS float64
		for _, qps := range qpsPerStore {
			avgQPS += qps
		}
		avgQPS /= float64(len(qpsPerStore))
		upperBound := math.Max(avgQPS*1.15, avgQPS+100)
		log.Infof(ctx, "avgQPS: %f, upperBound: %f", avgQPS, upperBound)

		// TODO: Also consider trying to move work to under-utilized stores even
		// if there aren't any outliers at the top end.
	topLevelLoop:
		for {
			// Try to lessen the load on the hottest store.
			hottestStore, hottestQPS := findHottestStore(qpsPerStore)
			log.Infof(ctx, "hottestStore: s%d, hottestQPS: %f", hottestStore, hottestQPS)
			if hottestQPS <= upperBound {
				break topLevelLoop
			}

			hottestRanges := hottestRangesByStore[hottestStore]
			var rangeIDs []roachpb.RangeID
			for i := range hottestRanges {
				rangeIDs = append(rangeIDs, hottestRanges[i].RangeID)
			}
			log.Infof(ctx, "hottest rangeIDs: %v", rangeIDs)

			// First check if there are any leases we can reasonably move.
			for i, r := range hottestRanges {
				qps := qps(r)
				log.Infof(ctx, "considering r%d, qps=%f", r.RangeID, qps)
				for j := range r.Nodes {
					storeID := r.Nodes[j].Range.SourceStoreID
					// Transfer the lease if we can move it to a store that will still be
					// under the average per-store QPS.
					if qpsPerStore[storeID]+qps < avgQPS {
						// Attempt to transfer the lease, and make sure we don't do
						// anything else to the range this go-round.
						hottestRangesByStore[hottestStore] = append(hottestRangesByStore[hottestStore][:i], hottestRangesByStore[hottestStore][i+1:]...)
						log.Infof(ctx, "transferring lease for r%d (qps=%f) to s%d (qps=%f)", r.RangeID, qps, storeID, qpsPerStore[storeID])
						if err := s.db.AdminTransferLease(ctx, r.Nodes[j].Range.State.ReplicaState.Desc.StartKey, storeID); err != nil {
							log.Errorf(ctx, "error transferring lease for r%d to s%d: %s", r.RangeID, storeID, err)
							continue topLevelLoop
						}
						qpsPerStore[storeID] += qps
						qpsPerStore[hottestStore] -= qps
						continue topLevelLoop
					}
				}
			}

			log.Infof(ctx, "failed to find a store to transfer a lease to")
			break topLevelLoop

			/*
				// If that didn't work out, then resort to rebalancing replicas.
				log.Infof(ctx, "failed to find a store to transfer a lease to; beginning to consider replica rebalances")

				hottestRanges := hottestRangesByStore[hottestStore]
				var rangeIDs []roachpb.RangeID
				for i := range hottestRanges {
					rangeIDs = append(rangeIDs, hottestRanges[i].RangeID)
				}
				log.Infof(ctx, "hottest remaining rangeIDs: %v", rangeIDs)

				for i, r := range hottestRanges {
					qps := qps(r)
					log.Infof(ctx, "considering r%d, qps=%f", r.RangeID, qps)

					for j := range r.Nodes {
					}
				}

				// TODO
				//storage.TestingRelocateRange(ctx, s.db, rangeDesc, targets)
			*/
		}
	}
}

func findHottestStore(qpsPerStore map[roachpb.StoreID]float64) (roachpb.StoreID, float64) {
	var storeID roachpb.StoreID
	var qps float64
	for s, q := range qpsPerStore {
		if q > qps {
			storeID = s
			qps = q
		}
	}
	return storeID, qps
}

func findColdestStore(qpsPerStore map[roachpb.StoreID]float64) (roachpb.StoreID, float64) {
	var storeID roachpb.StoreID
	qps := math.MaxFloat64
	for s, q := range qpsPerStore {
		if q < qps {
			storeID = s
			qps = q
		}
	}
	return storeID, qps
}

func processResponse(
	resp *serverpb.RaftDebugResponse,
) (map[roachpb.StoreID]float64, map[roachpb.StoreID][]*serverpb.RaftRangeStatus) {
	qpsPerStore := make(map[roachpb.StoreID]float64)
	hottestRangeQueues := make(map[roachpb.StoreID]*PriorityQueue)
	for _, r := range resp.Ranges {
		r := r
		lease, qps := leaseAndQPS(&r)
		qpsPerStore[lease] += qps
		pq := hottestRangeQueues[lease]
		if pq == nil {
			pq = &PriorityQueue{}
			heap.Init(pq)
			hottestRangeQueues[lease] = pq
		}
		heap.Push(pq, &r)
		if pq.Len() > 32 {
			heap.Pop(pq)
		}
	}

	hottestRanges := make(map[roachpb.StoreID][]*serverpb.RaftRangeStatus)
	for storeID, pq := range hottestRangeQueues {
		length := pq.Len()
		hottestRanges[storeID] = make([]*serverpb.RaftRangeStatus, length)
		rangeQPS := make([]float64, length)
		for i := 1; i <= length; i++ {
			hottestRanges[storeID][length-i] = heap.Pop(pq).(*serverpb.RaftRangeStatus)
			rangeQPS[length-i] = qps(hottestRanges[storeID][length-i])
		}
		log.Infof(context.TODO(), "hottest ranges for s%d: %v", storeID, rangeQPS)
	}

	return qpsPerStore, hottestRanges
}

func qps(r *serverpb.RaftRangeStatus) float64 {
	_, qps := leaseAndQPS(r)
	return qps
}

func leaseAndQPS(r *serverpb.RaftRangeStatus) (roachpb.StoreID, float64) {
	for i := range r.Nodes {
		if r.Nodes[i].Range.State.ReplicaState.Lease.Replica.StoreID == r.Nodes[i].Range.SourceStoreID {
			return r.Nodes[i].Range.SourceStoreID, r.Nodes[i].Range.Stats.QueriesPerSecond
		}
	}
	return 0, 0
}

type PriorityQueue []*serverpb.RaftRangeStatus

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return qps(pq[i]) < qps(pq[j])
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*serverpb.RaftRangeStatus)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
