// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantsettingswatcher_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.Exec(t, `INSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES
	  (0, 'foo', 'foo-all', 's'),
	  (1, 'foo', 'foo-t1', 's'),
	  (1, 'bar', 'bar-t1', 's'),
	  (2, 'baz', 'baz-t2', 's')`)

	s0 := tc.Server(0)
	w := tenantsettingswatcher.New(
		s0.Clock(),
		s0.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		s0.Stopper(),
		s0.ClusterSettings(),
	)
	err := w.Start(ctx, s0.SystemTableIDResolver().(catalog.SystemTableIDResolver))
	require.NoError(t, err)
	// WaitForStart should return immediately.
	err = w.WaitForStart(ctx)
	require.NoError(t, err)

	expect := func(overrides []kvpb.TenantSetting, expected string) {
		t.Helper()
		var vals []string
		for _, s := range overrides {
			vals = append(vals, fmt.Sprintf("%s=%s", s.Name, s.Value.Value))
		}
		if actual := strings.Join(vals, " "); actual != expected {
			t.Errorf("expected: %s; got: %s", expected, actual)
		}
	}
	t1 := roachpb.MustMakeTenantID(1)
	t2 := roachpb.MustMakeTenantID(2)
	t3 := roachpb.MustMakeTenantID(3)
	all, allCh := w.GetAllTenantOverrides()
	expect(all, "foo=foo-all")

	t1Overrides, t1Ch := w.GetTenantOverrides(t1)
	expect(t1Overrides, "bar=bar-t1 foo=foo-t1")

	t2Overrides, _ := w.GetTenantOverrides(t2)
	expect(t2Overrides, "baz=baz-t2")

	t3Overrides, t3Ch := w.GetTenantOverrides(t3)
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
	all, allCh = w.GetAllTenantOverrides()
	expect(all, "bar=bar-all foo=foo-all")

	// Modify a tenant override.
	r.Exec(t, "UPSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES (1, 'bar', 'bar-t1-updated', 's')")
	expectClose(t1Ch)
	t1Overrides, _ = w.GetTenantOverrides(t1)
	expect(t1Overrides, "bar=bar-t1-updated foo=foo-t1")

	// Remove an all-tenant override.
	r.Exec(t, "DELETE FROM system.tenant_settings WHERE tenant_id = 0 AND name = 'foo'")
	expectClose(allCh)
	all, _ = w.GetAllTenantOverrides()
	expect(all, "bar=bar-all")

	// Add an override for a tenant that has no overrides.
	r.Exec(t, "INSERT INTO system.tenant_settings (tenant_id, name, value, value_type) VALUES (3, 'qux', 'qux-t3', 's')")
	expectClose(t3Ch)
	t3Overrides, _ = w.GetTenantOverrides(t3)
	expect(t3Overrides, "qux=qux-t3")
}
