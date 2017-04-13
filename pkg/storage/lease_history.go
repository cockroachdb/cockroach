// Copyright 2017 The Cockroach Authors.
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

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// leaseHistoryMaxEntries is the maximum number of lease history entries to
// maintain for both expiration and epoch based leases.
const defaultLeaseHistoryMaxEntries = 100

var leaseHistoryMaxEntries = envutil.EnvOrDefaultInt("COCKROACH_LEASE_HISTORY_MAX_ENTRIES", defaultLeaseHistoryMaxEntries)

type leaseHistoryEntry struct {
	rangeID roachpb.RangeID
	lease   roachpb.Lease
}

type leaseHistory struct {
	syncutil.Mutex
	expirationIndex int
	expiration      []leaseHistoryEntry // A circular buffer with expirationIndex.
	epochIndex      int
	epoch           []leaseHistoryEntry // A circular buffer with epochIndex.
}

func newLeaseHistory() *leaseHistory {
	lh := &leaseHistory{
		expiration: make([]leaseHistoryEntry, leaseHistoryMaxEntries),
		epoch:      make([]leaseHistoryEntry, leaseHistoryMaxEntries),
	}
	return lh
}

func (lh *leaseHistory) add(rangeID roachpb.RangeID, lease roachpb.Lease) {
	lh.Lock()
	defer lh.Unlock()

	if lease.Epoch != nil {
		lh.epoch[lh.epochIndex] = leaseHistoryEntry{
			lease:   lease,
			rangeID: rangeID,
		}
		lh.epochIndex++
		if lh.epochIndex >= leaseHistoryMaxEntries {
			lh.epochIndex = 0
		}
		return
	}
	lh.expiration[lh.expirationIndex] = leaseHistoryEntry{
		lease:   lease,
		rangeID: rangeID,
	}
	lh.expirationIndex++
	if lh.expirationIndex >= leaseHistoryMaxEntries {
		lh.expirationIndex = 0
	}
}

func (lh *leaseHistory) get(rangeID roachpb.RangeID) []roachpb.Lease {
	var results []roachpb.Lease
	lh.Lock()
	defer lh.Unlock()
	for i := lh.epochIndex; i < lh.epochIndex+leaseHistoryMaxEntries; i++ {
		j := i
		if i >= leaseHistoryMaxEntries {
			j -= leaseHistoryMaxEntries
		}
		if lh.epoch[j].rangeID == rangeID {
			results = append(results, lh.epoch[j].lease)
		}
	}
	if len(results) > 0 {
		// There are never any mixed lease types.
		return results
	}
	for i := lh.expirationIndex; i < lh.expirationIndex+leaseHistoryMaxEntries; i++ {
		j := i
		if i >= leaseHistoryMaxEntries {
			j -= leaseHistoryMaxEntries
		}
		if lh.expiration[j].rangeID == rangeID {
			results = append(results, lh.expiration[j].lease)
		}
	}
	return results
}
