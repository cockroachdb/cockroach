// Copyright 2022 The Cockroach Authors.
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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type exchangeInfo struct {
	created time.Time
	desc    roachpb.StoreDescriptor
}

type fixedDelayExchange struct {
	pending  []exchangeInfo
	settings *config.SimulationSettings
}

func (u *fixedDelayExchange) put(tick time.Time, descs ...roachpb.StoreDescriptor) {
	for _, desc := range descs {
		u.pending = append(u.pending, exchangeInfo{created: tick, desc: desc})
	}
}

func (u *fixedDelayExchange) updates(tick time.Time) []*storepool.StoreDetail {
	sort.Slice(u.pending, func(i, j int) bool { return u.pending[i].created.Before(u.pending[j].created) })
	ready := []*storepool.StoreDetail{}
	i := 0
	for ; i < len(u.pending) && !tick.Before(u.pending[i].created.Add(u.settings.StateExchangeDelay)); i++ {
		ready = append(ready, makeStoreDetail(&u.pending[i].desc, u.pending[i].created))
	}
	u.pending = u.pending[i:]
	return ready
}

// makeStoreDetail wraps a store descriptor into a storepool StoreDetail at the
// given tick.
func makeStoreDetail(desc *roachpb.StoreDescriptor, tick time.Time) *storepool.StoreDetail {
	detail := storepool.StoreDetail{}
	detail.Desc = desc
	detail.LastAvailable = tick
	detail.LastUpdatedTime = tick
	detail.Desc.Node = desc.Node
	return &detail
}
