// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGCTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	defer srv.Stopper().Stop(ctx)

	gcClosure := func(tenID uint64, progress *jobspb.SchemaChangeGCProgress) error {
		return gcjob.GcTenant(ctx, &execCfg, tenID, progress)
	}

	const (
		activeTenID      = 10
		dropTenID        = 11
		nonexistentTenID = 12
	)
	require.NoError(t, sql.CreateTenantRecord(
		ctx, &execCfg, nil, /* txn */
		&descpb.TenantInfo{ID: activeTenID}),
	)
	require.NoError(t, sql.CreateTenantRecord(
		ctx, &execCfg, nil, /* txn */
		&descpb.TenantInfo{ID: dropTenID, State: descpb.TenantInfo_DROP}),
	)

	t.Run("unexpected progress state", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_GC,
			},
		}
		require.EqualError(
			t,
			gcClosure(10, progress),
			"Tenant id 10 is expired and should not be in state WAITING_FOR_GC",
		)
		require.Equal(t, jobspb.SchemaChangeGCProgress_WAITING_FOR_GC, progress.Tenant.Status)
	})

	t.Run("non-existent tenant deleting progress", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETING,
			},
		}
		require.NoError(t, gcClosure(nonexistentTenID, progress))
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.Tenant.Status)
	})

	t.Run("existent tenant deleted progress", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETED,
			},
		}
		require.EqualError(
			t,
			gcClosure(dropTenID, progress),
			"GC state for tenant id:11 state:DROP is DELETED yet the tenant row still exists",
		)
	})

	t.Run("active tenant GC", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETING,
			},
		}
		require.EqualError(
			t,
			gcClosure(activeTenID, progress),
			"gc tenant 10: clear tenant: tenant 10 is not in state DROP", activeTenID,
		)
	})

	t.Run("drop tenant GC", func(t *testing.T) {
		progress := &jobspb.SchemaChangeGCProgress{
			Tenant: &jobspb.SchemaChangeGCProgress_TenantProgress{
				Status: jobspb.SchemaChangeGCProgress_DELETING,
			},
		}

		descKey := catalogkeys.MakeDescMetadataKey(
			keys.MakeSQLCodec(roachpb.MakeTenantID(dropTenID)), keys.NamespaceTableID,
		)
		require.NoError(t, kvDB.Put(ctx, descKey, "foo"))

		r, err := kvDB.Get(ctx, descKey)
		require.NoError(t, err)
		val, err := r.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, []byte("foo"), val)

		require.NoError(t, gcClosure(dropTenID, progress))
		require.Equal(t, jobspb.SchemaChangeGCProgress_DELETED, progress.Tenant.Status)
		_, err = sql.GetTenantRecord(ctx, &execCfg, nil /* txn */, dropTenID)
		require.EqualError(t, err, `tenant "11" does not exist`)
		require.NoError(t, gcClosure(dropTenID, progress))

		r, err = kvDB.Get(ctx, descKey)
		require.NoError(t, err)
		require.True(t, nil == r.Value)
	})
}
