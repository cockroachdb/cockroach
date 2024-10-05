// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestEmptyInstanceFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var feed instanceCache = &emptyInstanceCache{}
	require.Empty(t, feed.listInstances())

	_, ok := feed.getInstance(base.SQLInstanceID(0))
	require.False(t, ok)
}

func TestSingletonFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	instance := instancerow{
		instanceID: base.SQLInstanceID(10),
		sqlAddr:    "something",
	}
	var feed instanceCache = &singletonInstanceFeed{instance: instance}

	got, ok := feed.getInstance(10)
	require.True(t, ok)
	require.Equal(t, instance, got)

	got, ok = feed.getInstance(11)
	require.False(t, ok)
	require.NotEqual(t, instance, got)

	require.Equal(t, feed.listInstances(), []instancerow{instance})
}

func TestRangeFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	host := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly},
	)
	defer host.Stopper().Stop(ctx)

	tenant, tenantSQL := serverutils.StartTenant(t, host, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	})
	tDB := sqlutils.MakeSQLRunner(tenantSQL)

	newStorage := func(t *testing.T, codec keys.SQLCodec) *Storage {
		tDB.Exec(t, `CREATE DATABASE "`+t.Name()+`"`)
		tDB.Exec(t, GetTableSQLForDatabase(t.Name()))
		tableDesc := desctestutils.TestingGetTableDescriptor(tenant.DB(), tenant.Codec(), t.Name(), "public", "sql_instances")
		slStorage := slstorage.NewFakeStorage()
		return NewTestingStorage(tenant.DB(), codec, tableDesc, slStorage,
			tenant.ClusterSettings(), tenant.Clock(), tenant.RangeFeedFactory().(*rangefeed.Factory), tenant.SettingsWatcher().(*settingswatcher.SettingsWatcher))
	}

	t.Run("success", func(t *testing.T) {
		storage := newStorage(t, tenant.Codec())

		require.NoError(t, storage.generateAvailableInstanceRows(ctx, [][]byte{enum.One}, tenant.Clock().Now().Add(int64(time.Minute), 0)))

		feed, err := storage.newInstanceCache(ctx)
		require.NoError(t, err)
		require.NotNil(t, feed)
		defer feed.Close()

		// Check the entries in the feed to make sure it is constructed after
		// the complete scan.
		instances := feed.listInstances()
		require.Len(t, instances, int(PreallocatedCount.Get(&tenant.ClusterSettings().SV)))
	})

	t.Run("auth_error", func(t *testing.T) {
		storage := newStorage(t, keys.SystemSQLCodec)
		_, err := storage.newInstanceCache(ctx)
		require.True(t, grpcutil.IsAuthError(err), "expected %+v to be an auth error", err)
	})

	t.Run("context_cancelled", func(t *testing.T) {
		storage := newStorage(t, tenant.Codec())

		ctx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := storage.newInstanceCache(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, ctx.Err())
	})
}
