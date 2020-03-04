// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// leaseHistoryMaxEntries controls if replica lease histories are enabled and
// how much memory they take up when enabled.
var leaseHistoryMaxEntries = envutil.EnvOrDefaultInt("COCKROACH_LEASE_HISTORY", 5)

type leaseHistory struct {
	syncutil.Mutex
	index   int
	history []roachpb.Lease // A circular buffer with index.
}

func newLeaseHistory() *leaseHistory {
	lh := &leaseHistory{
		history: make([]roachpb.Lease, 0, leaseHistoryMaxEntries),
	}
	return lh
}

func (lh *leaseHistory) add(lease roachpb.Lease) {
	lh.Lock()
	defer lh.Unlock()

	// Not through the first pass through the buffer.
	if lh.index == len(lh.history) {
		lh.history = append(lh.history, lease)
	} else {
		lh.history[lh.index] = lease
	}
	lh.index++
	if lh.index >= leaseHistoryMaxEntries {
		lh.index = 0
	}
}

func (lh *leaseHistory) get() []roachpb.Lease {
	lh.Lock()
	defer lh.Unlock()
	if len(lh.history) == 0 {
		return nil
	}
	if len(lh.history) < leaseHistoryMaxEntries || lh.index == 0 {
		result := make([]roachpb.Lease, len(lh.history))
		copy(result, lh.history)
		return lh.history
	}
	first := lh.history[lh.index:]
	second := lh.history[:lh.index]
	result := make([]roachpb.Lease, len(first)+len(second))
	copy(result, first)
	copy(result[len(first):], second)
	return result
}
