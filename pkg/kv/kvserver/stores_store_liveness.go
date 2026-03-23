// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"

// InspectAllStoreLiveness is an interface that allows for per-store Store
// Liveness state to be combined into a per-node view. It powers the inspectz
// Store Liveness functionality.
type InspectAllStoreLiveness interface {
	InspectAllSupportFrom() ([]slpb.InspectSupportFromStatesPerStore, error)
	InspectAllSupportFor() ([]slpb.InspectSupportForStatesPerStore, error)
}

// StoresForStoreLiveness is a wrapper around Stores that implements
// InspectAllStoreLiveness.
type StoresForStoreLiveness Stores

var _ InspectAllStoreLiveness = (*StoresForStoreLiveness)(nil)

// MakeStoresForStoreLiveness casts Stores into StoresForStoreLiveness.
func MakeStoresForStoreLiveness(stores *Stores) *StoresForStoreLiveness {
	return (*StoresForStoreLiveness)(stores)
}

// InspectAllSupportFrom implements the InspectAllStoreLiveness interface. It
// iterates over all stores and aggregates their SupportFrom states.
func (sfsl *StoresForStoreLiveness) InspectAllSupportFrom() (
	[]slpb.InspectSupportFromStatesPerStore,
	error,
) {
	stores := (*Stores)(sfsl)
	var sfsps []slpb.InspectSupportFromStatesPerStore
	err := stores.VisitStores(
		func(s *Store) error {
			sfsps = append(sfsps, s.storeLiveness.InspectSupportFrom())
			return nil
		},
	)
	return sfsps, err
}

// InspectAllSupportFor implements the InspectAllStoreLiveness interface. It
// iterates over all stores and aggregates their SupportFor states.
func (sfsl *StoresForStoreLiveness) InspectAllSupportFor() (
	[]slpb.InspectSupportForStatesPerStore,
	error,
) {
	stores := (*Stores)(sfsl)
	var sfsps []slpb.InspectSupportForStatesPerStore
	err := stores.VisitStores(
		func(s *Store) error {
			sfsps = append(sfsps, s.storeLiveness.InspectSupportFor())
			return nil
		},
	)
	return sfsps, err
}
