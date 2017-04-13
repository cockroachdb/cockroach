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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// leaseHistoryMaxEntries is the maximum number of lease history entries to
// maintain.
const leaseHistoryMaxEntries = 100

type leaseHistoryEntry struct {
	rangeID roachpb.RangeID
	lease   roachpb.Lease
}

type leaseHistory struct {
	syncutil.Mutex
	index   int
	history []leaseHistoryEntry // A circular buffer starting at index.
}

func newLeaseHistory() *leaseHistory {
	lh := &leaseHistory{
		history: make([]leaseHistoryEntry, leaseHistoryMaxEntries),
	}
	return lh
}

func (lh *leaseHistory) Add(rangeID roachpb.RangeID, lease roachpb.Lease) {
	lh.Lock()
	defer lh.Unlock()

	lh.history[lh.index] = leaseHistoryEntry{
		lease:   lease,
		rangeID: rangeID,
	}
	lh.index++
	if lh.index >= leaseHistoryMaxEntries {
		lh.index = 0
	}
}

func (lh *leaseHistory) Get(rangeID roachpb.RangeID) []roachpb.Lease {
	var results []roachpb.Lease
	lh.Lock()
	defer lh.Unlock()
	for i := lh.index; i < lh.index+leaseHistoryMaxEntries; i++ {
		j := i
		if i >= leaseHistoryMaxEntries {
			j -= leaseHistoryMaxEntries
		}
		if lh.history[j].rangeID == rangeID {
			results = append(results, lh.history[j].lease)
		}
	}
	return results
}
