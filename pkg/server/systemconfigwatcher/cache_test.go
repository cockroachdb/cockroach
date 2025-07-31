// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package systemconfigwatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestNewWithAdditionalProvider(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '20ms'`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '20ms'`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '20ms'`)
	fakeTenant := roachpb.MustMakeTenantID(10)
	codec := keys.MakeSQLCodec(fakeTenant)
	fp := &fakeProvider{
		ch: make(chan struct{}, 1),
	}
	fp.setSystemConfig(config.NewSystemConfig(zonepb.DefaultZoneConfigRef()))
	cache := systemconfigwatcher.NewWithAdditionalProvider(
		codec, s.Clock(), s.RangeFeedFactory().(*rangefeed.Factory),
		zonepb.DefaultZoneConfigRef(), fp,
	)
	mkKV := func(key, value string) roachpb.KeyValue {
		return roachpb.KeyValue{
			Key:   []byte(key),
			Value: func() (v roachpb.Value) { v.SetString(value); return v }(),
		}
	}
	setAdditional := func(kvs ...roachpb.KeyValue) {
		additional := config.SystemConfig{}
		additional.Values = kvs
		fp.setSystemConfig(&additional)
	}
	kvA := mkKV("a", "value")
	setAdditional(kvA)
	fp.ch <- struct{}{}

	ch, _ := cache.RegisterSystemConfigChannel()
	require.NoError(t, cache.Start(ctx, s.Stopper()))
	getValues := func() []roachpb.KeyValue {
		return cache.GetSystemConfig().SystemConfigEntries.Values
	}

	<-ch // we'll get notified upon initial scan, which should be empty
	require.Equal(t, []roachpb.KeyValue{kvA}, getValues())

	// Update the kv-pair and make sure it propagates
	kvB := mkKV("b", "value")
	setAdditional(kvB)
	fp.ch <- struct{}{}
	<-ch
	require.Equal(t, []roachpb.KeyValue{kvB}, getValues())

	mkTenantKey := func(key string) roachpb.Key {
		return append(codec.TablePrefix(keys.ZonesTableID), key...)
	}
	// Write a value and make sure that it shows up.
	tenantA := mkTenantKey("a")
	require.NoError(t, kvDB.Put(ctx, tenantA, "value"))
	<-ch
	require.Len(t, getValues(), 2)
	require.Equal(t, kvB, getValues()[0])
	require.Equal(t, tenantA, getValues()[1].Key)

	// Update the additional value.
	kvC := mkKV("c", "value")
	setAdditional(kvA, kvC)
	fp.ch <- struct{}{}
	<-ch
	require.Len(t, getValues(), 3)
	require.Equal(t, kvA, getValues()[0])
	require.Equal(t, kvC, getValues()[1])
	require.Equal(t, tenantA, getValues()[2].Key)
}

type fakeProvider struct {
	ch chan struct{}
	mu struct {
		syncutil.Mutex
		cfg *config.SystemConfig
	}
}

func (f *fakeProvider) GetSystemConfig() *config.SystemConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.cfg
}

func (f *fakeProvider) setSystemConfig(cfg *config.SystemConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.cfg = cfg
}

func (f *fakeProvider) RegisterSystemConfigChannel() (_ <-chan struct{}, unregister func()) {
	return f.ch, func() {}
}

var _ config.SystemConfigProvider = (*fakeProvider)(nil)
