// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgkvaccessor_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func upsert(id int64, name string, weight int64, maxCPU bool) *rgpb.ResourceGroupUpsert {
	return &rgpb.ResourceGroupUpsert{
		Id:   id,
		Name: name,
		Config: admissionpb.ResourceGroupConfig{
			CPUWeight: weight,
			MaxCPU:    maxCPU,
		},
	}
}

func del(id int64) *rgpb.ResourceGroupDelete {
	return &rgpb.ResourceGroupDelete{Id: id}
}

func TestWriterApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	w := rgkvaccessor.NewWriter(execCfg.InternalDB)
	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t))

	tenantA := roachpb.MustMakeTenantID(2)
	tenantB := roachpb.MustMakeTenantID(3)

	configFor := func(t *testing.T, tenantID roachpb.TenantID, id int64) admissionpb.ResourceGroupConfig {
		t.Helper()
		var cfgBytes []byte
		sqlDB.QueryRow(t,
			`SELECT config FROM system.tenant_resource_groups WHERE tenant_id = $1 AND id = $2`,
			tenantID.ToUint64(), id).Scan(&cfgBytes)
		var cfg admissionpb.ResourceGroupConfig
		require.NoError(t, protoutil.Unmarshal(cfgBytes, &cfg))
		return cfg
	}

	rowCount := func(t *testing.T, tenantID roachpb.TenantID) int {
		t.Helper()
		var n int
		sqlDB.QueryRow(t,
			`SELECT count(*) FROM system.tenant_resource_groups WHERE tenant_id = $1`,
			tenantID.ToUint64()).Scan(&n)
		return n
	}

	t.Run("empty apply is a no-op", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantA, nil, nil))
		require.Equal(t, 0, rowCount(t, tenantA))
	})

	t.Run("upsert inserts a new row", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantA,
			[]*rgpb.ResourceGroupUpsert{upsert(17, "high", 100, true)},
			nil,
		))
		cfg := configFor(t, tenantA, 17)
		require.Equal(t, int64(100), cfg.CPUWeight)
		require.Equal(t, true, cfg.MaxCPU)
	})

	t.Run("upsert overwrites at same (tenant_id, id)", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantA,
			[]*rgpb.ResourceGroupUpsert{upsert(17, "high-prime", 200, false)},
			nil,
		))
		cfg := configFor(t, tenantA, 17)
		require.Equal(t, int64(200), cfg.CPUWeight)
		require.Equal(t, false, cfg.MaxCPU)
	})

	t.Run("different tenants do not collide on the same id", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantB,
			[]*rgpb.ResourceGroupUpsert{upsert(17, "different-tenant", 50, false)},
			nil,
		))
		require.Equal(t, int64(200), configFor(t, tenantA, 17).CPUWeight)
		require.Equal(t, int64(50), configFor(t, tenantB, 17).CPUWeight)
	})

	t.Run("delete removes a row", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantA, nil,
			[]*rgpb.ResourceGroupDelete{del(17)},
		))
		require.Equal(t, 0, rowCount(t, tenantA))
	})

	t.Run("delete of a missing row is a no-op", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantA, nil,
			[]*rgpb.ResourceGroupDelete{del(999)},
		))
	})

	t.Run("mixed batch applies atomically", func(t *testing.T) {
		require.NoError(t, w.Apply(ctx, tenantA,
			[]*rgpb.ResourceGroupUpsert{
				upsert(20, "a", 10, false),
				upsert(21, "b", 20, false),
			},
			nil,
		))
		require.Equal(t, 2, rowCount(t, tenantA))
		require.NoError(t, w.Apply(ctx, tenantA,
			[]*rgpb.ResourceGroupUpsert{upsert(20, "a-prime", 11, true)},
			[]*rgpb.ResourceGroupDelete{del(21)},
		))
		require.Equal(t, 1, rowCount(t, tenantA))
		cfg := configFor(t, tenantA, 20)
		require.Equal(t, int64(11), cfg.CPUWeight)
		require.Equal(t, true, cfg.MaxCPU)
	})
}
