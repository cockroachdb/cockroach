// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author:

package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestAcquireLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	rng1, err := store.GetRange(1)
	if err != nil {
		t.Fatal(err)
	}
	rng1.WaitForElection()
	var lease *proto.Lease
	if err := util.IsTrueWithin(func() bool {
		lease = rng1.getLease()
		return lease != nil
	}, time.Second); err != nil {
		t.Fatal("timeout obtaining lease for first range")
	}
	if lease.Duration != int64(defaultLeaderLeaseDuration) {
		t.Errorf("unexpected lease duration %d, wanted %d",
			lease.Duration, defaultLeaderLeaseDuration)
	}
	// TODO when lease is properly set.
	expExpiration := int64(defaultLeaderLeaseDuration)
	if lease.Expiration != expExpiration {
		t.Errorf("unexpected lease expiry %d, wanted %d",
			lease.Expiration, expExpiration)
	}
}
