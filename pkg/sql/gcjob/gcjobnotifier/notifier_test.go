// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjobnotifier

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type testingProvider struct {
	syncutil.Mutex
	cfg *config.SystemConfig
	ch  chan struct{}
}

func (t *testingProvider) GetSystemConfig() *config.SystemConfig {
	t.Lock()
	defer t.Unlock()
	return t.cfg
}

func (t *testingProvider) setSystemConfig(cfg *config.SystemConfig) {
	t.Lock()
	defer t.Unlock()
	t.cfg = cfg
}

func (t *testingProvider) RegisterSystemConfigChannel() <-chan struct{} {
	return t.ch
}

var _ config.SystemConfigProvider = (*testingProvider)(nil)

func TestNotifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	t.Run("start with stopped stopper leads to nil being returned", func(t *testing.T) {
		stopper := stop.NewStopper()
		stopper.Stop(ctx)
		n := New(settings, &testingProvider{}, keys.SystemSQLCodec, stopper)
		n.Start(ctx)
		ch, _ := n.AddNotifyee(ctx)
		require.Nil(t, ch)
	})
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	t.Run("panic on double start", func(t *testing.T) {
		n := New(settings, &testingProvider{ch: make(chan struct{})}, keys.SystemSQLCodec, stopper)
		n.Start(ctx)
		require.Panics(t, func() {
			n.Start(ctx)
		})
	})
	t.Run("panic on AddNotifyee before start", func(t *testing.T) {
		n := New(settings, &testingProvider{ch: make(chan struct{})}, keys.SystemSQLCodec, stopper)
		require.Panics(t, func() {
			n.AddNotifyee(ctx)
		})
	})
	t.Run("notifies on changed delta and cleanup", func(t *testing.T) {
		cfg := config.NewSystemConfig(zonepb.DefaultSystemZoneConfigRef())
		cfg.Values = []roachpb.KeyValue{
			mkZoneConfigKV(1, 1, "1"),
		}
		ch := make(chan struct{}, 1)
		p := &testingProvider{
			cfg: mkSystemConfig(mkZoneConfigKV(1, 1, "1")),
			ch:  ch,
		}
		n := New(settings, p, keys.SystemSQLCodec, stopper)
		n.Start(ctx)
		n1Ch, cleanup1 := n.AddNotifyee(ctx)

		t.Run("don't receive on new notifyee", func(t *testing.T) {
			expectNoSend(t, n1Ch)
		})
		t.Run("don't receive with no change", func(t *testing.T) {
			ch <- struct{}{}
			expectNoSend(t, n1Ch)
		})
		n2Ch, _ := n.AddNotifyee(ctx)
		t.Run("receive from all notifyees when data does change", func(t *testing.T) {
			p.setSystemConfig(mkSystemConfig(mkZoneConfigKV(1, 2, "2")))
			ch <- struct{}{}
			expectSend(t, n1Ch)
			expectSend(t, n2Ch)
		})
		t.Run("don't receive after cleanup", func(t *testing.T) {
			cleanup1()
			p.setSystemConfig(mkSystemConfig(mkZoneConfigKV(1, 3, "3")))
			ch <- struct{}{}
			expectSend(t, n2Ch)
			expectNoSend(t, n1Ch)
		})
	})
}

const (
	// used for timeouts of things which should be fast
	longTime = time.Second
	// used for sanity check of channel sends which shouldn't happen
	shortTime = 10 * time.Millisecond
)

func expectNoSend(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("did not expect to receive")
	case <-time.After(shortTime):
	}
}

func expectSend(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(longTime):
		t.Fatal("expected to receive")
	}
}

func mkZoneConfigKV(id config.SystemTenantObjectID, ts int64, value string) roachpb.KeyValue {
	kv := roachpb.KeyValue{
		Key: config.MakeZoneKey(id),
		Value: roachpb.Value{
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
	}
	kv.Value.SetString(value)
	return kv
}

func mkSystemConfig(kvs ...roachpb.KeyValue) *config.SystemConfig {
	cfg := config.NewSystemConfig(zonepb.DefaultSystemZoneConfigRef())
	cfg.Values = kvs
	return cfg
}
