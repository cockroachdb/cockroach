// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed/rangefeedpb"

// InspectAllRangefeeds is an interface that allows for per-store Rangefeed
// state to be combined into a per-node view. It powers the inspectz Rangefeeds
// functionality.
type InspectAllRangefeeds interface {
	Inspect() ([]rangefeedpb.RangefeedInfoPerStore, error)
}

// StoresForRangefeeds is a wrapper around Stores that implements
// InspectAllRangefeeds.
type StoresForRangefeeds Stores

var _ InspectAllRangefeeds = (*StoresForRangefeeds)(nil)

// MakeStoresForRangefeeds casts Stores into StoresForRangefeeds.
func MakeStoresForRangefeeds(stores *Stores) *StoresForRangefeeds {
	return (*StoresForRangefeeds)(stores)
}

// Inspect implements the InspectAllRangefeeds interface.
// It iterates over all stores and aggregates their RangefeedInfo.
func (sfr *StoresForRangefeeds) Inspect() ([]rangefeedpb.RangefeedInfoPerStore, error) {
	stores := (*Stores)(sfr)
	var rangefeeds []rangefeedpb.RangefeedInfoPerStore
	err := stores.VisitStores(
		func(s *Store) error {
			rangefeeds = append(rangefeeds, s.VisitRangefeeds())
			return nil
		},
	)
	return rangefeeds, err
}
