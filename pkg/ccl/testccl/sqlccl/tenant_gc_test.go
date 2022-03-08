// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestGCTenantRemovesSpanConfigs ensures that GC-ing a tenant removes all
// span/system span configs installed by it.
func TestGCTenantRemovesSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// Disable the system tenant's reconciliation process so that we can
				// make assertions on the total number of span configurations in the
				// system.
				ManagerDisableJobCreation: true,
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	gcClosure := func(tenID uint64, progress *jobspb.SchemaChangeGCProgress) error {
		return gcjob.TestingGCTenant(ctx, &execCfg, tenID, progress)
	}

	tenantID := roachpb.MakeTenantID(10)

	tt, err := ts.StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
		TestingKnobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// Disable the tenant's span config reconciliation process, we'll
				// instead manually add system span configs via the KVAccessor.
				ManagerDisableJobCreation: true,
			},
		},
	})
	require.NoError(t, err)

	tenantKVAccessor := tt.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	// Write a system span config, set by the tenant, targeting its entire
	// keyspace.
	systemTarget, err := spanconfig.MakeTenantKeyspaceTarget(tenantID, tenantID)
	require.NoError(t, err)
	err = tenantKVAccessor.UpdateSpanConfigRecords(ctx, nil /* toDelete */, []spanconfig.Record{
		{
			Target: spanconfig.MakeTargetFromSystemTarget(systemTarget),
			Config: roachpb.SpanConfig{}, // Doesn't matter
		},
	})
	require.NoError(t, err)

	// Ensure there are 2 configs for the tenant -- one that spans its entire
	// keyspace, installed on creation, and of course the system span config we
	// inserted above.
	tenPrefix := keys.MakeTenantPrefix(tenantID)
	records, err := tenantKVAccessor.GetSpanConfigRecords(ctx, spanconfig.Targets{
		spanconfig.MakeTargetFromSpan(roachpb.Span{Key: tenPrefix, EndKey: tenPrefix.PrefixEnd()}),
		spanconfig.MakeTargetFromSystemTarget(systemTarget),
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(records))

	// Get the entire span config state, from the system tenant's perspective,
	// which we'll use to compare against once the tenant is GC-ed.
	records, err = scKVAccessor.GetSpanConfigRecords(
		ctx, spanconfig.TestingEntireSpanConfigurationStateTargets(),
	)
	require.NoError(t, err)
	beforeDelete := len(records)

	// Mark the tenant as dropped by updating its record.
	require.NoError(t, sql.TestingUpdateTenantRecord(
		ctx, &execCfg, nil, /* txn */
		&descpb.TenantInfo{ID: tenantID.ToUint64(), State: descpb.TenantInfo_DROP},
	))

	// Run GC on the tenant.
	progress := &jobspb.SchemaChangeGCProgress{
		Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
			Status: jobspb.SchemaChangeGCProgress_DELETING,
		},
	}
	require.NoError(t, gcClosure(tenantID.ToUint64(), progress))
	require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.Tenant.Status)

	// Ensure the tenant's span configs and system span configs have been deleted.
	records, err = scKVAccessor.GetSpanConfigRecords(
		ctx, spanconfig.TestingEntireSpanConfigurationStateTargets(),
	)
	require.NoError(t, err)
	require.Equal(t, len(records), beforeDelete-2)
}
