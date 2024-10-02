// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantsettingswatcher_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `INSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES
	  (0, 'foo', 'foo-all', 's'),
	  (1, 'foo', 'foo-t1', 's'),
	  (1, 'bar', 'bar-t1', 's'),
	  (2, 'baz', 'baz-t2', 's')`)

	w := tenantsettingswatcher.New(
		ts.Clock(),
		ts.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		ts.AppStopper(),
		ts.ClusterSettings(),
	)
	err := w.Start(ctx, ts.SystemTableIDResolver().(catalog.SystemTableIDResolver))
	require.NoError(t, err)
	// WaitForStart should return immediately.
	err = w.WaitForStart(ctx)
	require.NoError(t, err)

	expect := func(overrides []kvpb.TenantSetting, expected string) {
		t.Helper()
		var vals []string
		for _, s := range overrides {
			if s.InternalKey == clusterversion.KeyVersionSetting {
				continue
			}
			vals = append(vals, fmt.Sprintf("%s=%s", s.InternalKey, s.Value.Value))
		}
		if actual := strings.Join(vals, " "); actual != expected {
			t.Errorf("expected: %s; got: %s", expected, actual)
		}
	}
	t1 := roachpb.MustMakeTenantID(1)
	t2 := roachpb.MustMakeTenantID(2)
	t3 := roachpb.MustMakeTenantID(3)
	all, allCh := w.GetAllTenantOverrides(ctx)
	expect(all, "foo=foo-all")

	t1Overrides, t1Ch := w.GetTenantOverrides(ctx, t1)
	expect(t1Overrides, "bar=bar-t1 foo=foo-t1")

	t2Overrides, _ := w.GetTenantOverrides(ctx, t2)
	expect(t2Overrides, "baz=baz-t2")

	t3Overrides, t3Ch := w.GetTenantOverrides(ctx, t3)
	expect(t3Overrides, "")

	expectClose := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(15 * time.Second):
			t.Fatalf("channel did not close")
		}
	}
	// Add an all-tenant override.
	r.Exec(t, "INSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES (0, 'bar', 'bar-all', 's')")
	expectClose(allCh)
	all, allCh = w.GetAllTenantOverrides(ctx)
	expect(all, "bar=bar-all foo=foo-all")

	// Modify a tenant override.
	r.Exec(t, "UPSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES (1, 'bar', 'bar-t1-updated', 's')")
	expectClose(t1Ch)
	t1Overrides, _ = w.GetTenantOverrides(ctx, t1)
	expect(t1Overrides, "bar=bar-t1-updated foo=foo-t1")

	// Remove an all-tenant override.
	r.Exec(t, "DELETE FROM system.tenant_settings WHERE tenant_id = 0 AND name = 'foo'")
	expectClose(allCh)
	all, _ = w.GetAllTenantOverrides(ctx)
	expect(all, "bar=bar-all")

	// Add an override for a tenant that has no overrides.
	r.Exec(t, "INSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES (3, 'qux', 'qux-t3', 's')")
	expectClose(t3Ch)
	t3Overrides, _ = w.GetTenantOverrides(ctx, t3)
	expect(t3Overrides, "qux=qux-t3")
}
