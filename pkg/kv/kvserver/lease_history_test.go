// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLeaseHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	history := newLeaseHistory(leaseHistoryMaxEntries)

	for i := 0; i < leaseHistoryMaxEntries; i++ {
		leases := history.get()
		if e, a := i, len(leases); e != a {
			t.Errorf("%d: expected history len to be %d , actual %d:\n%+v", i, e, a, leases)
		}
		if i > 0 {
			if e, a := int64(0), leases[0].Epoch; e != a {
				t.Errorf("%d: expected oldest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
			}
			if e, a := int64(i-1), leases[len(leases)-1].Epoch; e != a {
				t.Errorf("%d: expected newest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
			}
			require.NotSame(t, &history.history[0], &leases[0], "expected slice copy")
		} else {
			require.Nil(t, leases)
		}

		history.add(roachpb.Lease{
			Epoch: int64(i),
		})
	}

	// Now overflow the circular buffer.
	for i := 0; i < leaseHistoryMaxEntries; i++ {
		leases := history.get()
		if e, a := leaseHistoryMaxEntries, len(leases); e != a {
			t.Errorf("%d: expected history len to be %d , actual %d:\n%+v", i, e, a, leases)
		}
		if e, a := int64(i), leases[0].Epoch; e != a {
			t.Errorf("%d: expected oldest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
		}
		if e, a := int64(i+leaseHistoryMaxEntries-1), leases[leaseHistoryMaxEntries-1].Epoch; e != a {
			t.Errorf("%d: expected newest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
		}
		require.NotSame(t, &history.history[0], &leases[0], "expected slice copy")

		history.add(roachpb.Lease{
			Epoch: int64(i + leaseHistoryMaxEntries),
		})
	}
}
