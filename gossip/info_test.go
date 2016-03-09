// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

func newInfo(val float64) Info {
	now := timeutil.Now()

	v := roachpb.Value{Timestamp: roachpb.Timestamp{WallTime: now.UnixNano()}}
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
