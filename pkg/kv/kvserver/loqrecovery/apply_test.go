// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestApplyVerifiesVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testTime, _ := time.Parse(time.RFC3339, "2022-02-24T01:40:00Z")
	clock := timeutil.NewManualTime(testTime)
	ps := NewPlanStore("", vfs.NewMem())
	cid := uuid.MakeV4()
	engines := []storage.Engine{
		createEngineOrFatal(ctx, t, cid, 1),
		createEngineOrFatal(ctx, t, cid, 2),
	}
	defer engines[1].Close()
	defer engines[0].Close()

	assertPlanError := func(t *testing.T, plan loqrecoverypb.ReplicaUpdatePlan, errMsg string) {
		require.NoError(t, ps.SavePlan(plan), "failed to stage plan")
		err := MaybeApplyPendingRecoveryPlan(ctx, ps, engines, clock)
		require.NoError(t, err, "fatal error applying recovery plan")
		report, ok, err := readNodeRecoveryStatusInfo(ctx, engines[0])
		require.NoError(t, err, "failed to read application outcome")
		require.True(t, ok, "didn't find application outcome in engine")
		require.NotEmpty(t, report.AppliedPlanID, "plan was not processed")
		require.Contains(t, report.Error, errMsg, "version error not registered")
	}

	t.Run("apply plan version is higher than cluster", func(t *testing.T) {
		aboveCurrent := clusterversion.Latest.Version()
		aboveCurrent.Major += 1
		plan := loqrecoverypb.ReplicaUpdatePlan{
			PlanID:    uuid.MakeV4(),
			ClusterID: cid.String(),
			Version:   aboveCurrent,
		}
		assertPlanError(t, plan, "doesn't match cluster active version")
	})

	t.Run("apply plan version lower than current", func(t *testing.T) {
		belowMin := clusterversion.MinSupported.Version()
		belowMin.Minor -= 1
		plan := loqrecoverypb.ReplicaUpdatePlan{
			PlanID:    uuid.MakeV4(),
			ClusterID: cid.String(),
			Version:   belowMin,
		}
		assertPlanError(t, plan, "doesn't match cluster active version")
	})
}

func createEngineOrFatal(ctx context.Context, t *testing.T, uuid uuid.UUID, id int) storage.Engine {
	eng, err := storage.Open(ctx,
		storage.InMemory(),
		cluster.MakeClusterSettings(),
		storage.CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatalf("failed to crate in mem store: %v", err)
	}
	sIdent := roachpb.StoreIdent{
		ClusterID: uuid,
		NodeID:    roachpb.NodeID(id),
		StoreID:   roachpb.StoreID(id),
	}
	if err = storage.MVCCPutProto(
		context.Background(), eng, keys.StoreIdentKey(), hlc.Timestamp{}, &sIdent, storage.MVCCWriteOptions{},
	); err != nil {
		t.Fatalf("failed to populate test store ident: %v", err)
	}
	return eng
}
