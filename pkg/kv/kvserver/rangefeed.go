package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed/rangefeedpb"
)

type InspectAllRangefeeds interface {
	InspectAllRangefeeds() (*rangefeedpb.InspectStoreRangefeedsResponse, error)
}

var _ InspectAllRangefeeds = (*Stores)(nil)

func (ss *Stores) InspectAllRangefeeds() (*rangefeedpb.InspectStoreRangefeedsResponse, error) {
	resp := rangefeedpb.InspectStoreRangefeedsResponse{}
	err := ss.VisitStores(
		func(s *Store) error {
			// Every store should return an rangefeedInfoPerStore.
			resp.RangefeedInfoPerStore = append(resp.RangefeedInfoPerStore, s.VisitRangefeeds())
			return nil
		},
	)
	return &resp, err
}

// replica (processor per replica)
// processor (per replica) -> deliver events to rangefeed
