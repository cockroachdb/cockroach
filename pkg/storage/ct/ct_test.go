// Copyright 2018 The Cockroach Authors.
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

package ct_test

import (
	"errors"
	"testing"

	"context"

	"time"

	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/ctconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/ct/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"google.golang.org/grpc"
)

type mockDialer struct{}

func (mc *mockDialer) Dial(context.Context, roachpb.NodeID) (ctpb.Client, error) {
	return nil, errors.New("unimplemented")
}
func (mc *mockDialer) Ready(roachpb.NodeID) bool {
	return false
}

func TestContainer(t *testing.T) {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	rawClock := hlc.NewClock(hlc.NewManualClock(1E9).UnixNano, 500*time.Millisecond)
	epoch, live := int64(1), int32(1) // atomically

	clock := func() (liveNow hlc.Timestamp, liveEpoch int64, _ error) {
		if atomic.LoadInt32(&live) != 1 {
			return hlc.Timestamp{}, 0, errors.New("not live")
		}
		return rawClock.Now(), atomic.LoadInt64(&epoch), nil
	}

	var refreshed struct {
		syncutil.Mutex
		sl []roachpb.RangeID
	}
	refresh := func(requested ...roachpb.RangeID) {
		refreshed.Lock()
		refreshed.sl = append(refreshed.sl, requested...)
		refreshed.Unlock()
	}

	dialer := &mockDialer{}

	cfg := ctconfig.Config{
		Settings:   cluster.MakeTestingClusterSettings(),
		Stopper:    stopper,
		Clock:      clock,
		Refresh:    refresh,
		Dialer:     dialer,
		GRPCServer: grpc.NewServer(),
	}
	c := ctconfig.NewContainer(cfg)
	c.Start()
}
