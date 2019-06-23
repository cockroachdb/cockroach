// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func newInfo(val float64) Info {
	now := timeutil.Now()

	v := roachpb.Value{Timestamp: hlc.Timestamp{WallTime: now.UnixNano()}}
	v.SetFloat(val)

	return Info{
		Value:     v,
		OrigStamp: now.UnixNano(),
		TTLStamp:  now.Add(time.Millisecond).UnixNano(),
	}
}

func TestExpired(t *testing.T) {
	defer leaktest.AfterTest(t)()

	i := newInfo(float64(1))
	if i.expired(i.Value.Timestamp.WallTime) {
		t.Error("premature expiration")
	}
	if !i.expired(i.TTLStamp) {
		t.Error("info should have expired")
	}
}

func TestIsFresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	i := newInfo(float64(1))
	if !i.isFresh(i.OrigStamp - 1) {
		t.Error("info should be fresh:", i)
	}
	if i.isFresh(i.OrigStamp) {
		t.Error("info should not be fresh:", i)
	}
	if i.isFresh(i.OrigStamp + 1) {
		t.Error("info should not be fresh:", i)
	}
	// Using timestamp 0 will always yield fresh data.
	if !i.isFresh(0) {
		t.Error("info should be fresh from node0:", i)
	}
}
