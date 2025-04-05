// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed/rangefeedpb"
)

type InspectAllRangefeeds interface {
	InspectAllRangefeeds() ([]rangefeedpb.InspectStoreRangefeedsResponse, error)
}

var _ InspectAllRangefeeds = (*Stores)(nil)

func (ss *Stores) InspectAllRangefeeds() ([]rangefeedpb.InspectStoreRangefeedsResponse, error) {
	var sspf []rangefeedpb.InspectStoreRangefeedsResponse
	err := ss.VisitStores(
		func(s *Store) error {
			sspf = append(sspf, s.VisitRangefeeds()...)
			return nil
		},
	)
	return sspf, err
}
