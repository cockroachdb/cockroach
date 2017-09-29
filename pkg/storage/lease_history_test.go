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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestLeaseHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	history := newLeaseHistory()

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

		history.add(roachpb.Lease{
			Epoch: int64(i + leaseHistoryMaxEntries),
		})
	}
}
