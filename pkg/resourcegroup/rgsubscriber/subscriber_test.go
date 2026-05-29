// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgsubscriber_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgsubscriber"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSubscriberObservesWrites verifies that rows written by the
// host-side Writer become visible through the Subscriber.Reader
// interface (Get and Snapshot) and that LastUpdated tracks the latest
// rangefeed update.
func TestSubscriberObservesWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	w := rgkvaccessor.NewWriter(execCfg.InternalDB)

	sub := rgsubscriber.New(execCfg.Settings, execCfg.Clock, execCfg.RangeFeedFactory, s.Stopper())
	require.NoError(t, sub.Start(ctx, execCfg.SystemTableIDResolver))

	tenantA := roachpb.MustMakeTenantID(2)
	tenantB := roachpb.MustMakeTenantID(3)

	require.NoError(t, w.Apply(ctx, tenantA,
		[]*rgpb.ResourceGroupUpsert{{
			Id:     17,
			Name:   "a",
			Config: admissionpb.ResourceGroupConfig{CPUWeight: 100, MaxCPU: true},
		}},
		nil,
	))
	testutils.SucceedsSoon(t, func() error {
		cfg, ok := sub.Get(tenantA, 17)
		if !ok {
			return errors.New("tenantA/17 not yet visible via Get")
		}
		if cfg.CPUWeight != 100 || !cfg.MaxCPU {
			return errors.Newf("unexpected config: %+v", cfg)
		}
		snap := sub.Snapshot()
		if _, ok := snap[tenantA]; !ok {
			return errors.New("tenantA missing from Snapshot")
		}
		if snap[tenantA][17] == nil {
			return errors.New("tenantA/17 missing from Snapshot")
		}
		return nil
	})
	updated, ok := sub.LastUpdated()
	require.True(t, ok)
	require.False(t, updated.IsZero())

	_, ok = sub.Get(tenantB, 17)
	require.False(t, ok)

	// Update tenantA's row.
	require.NoError(t, w.Apply(ctx, tenantA,
		[]*rgpb.ResourceGroupUpsert{{
			Id:     17,
			Name:   "a-prime",
			Config: admissionpb.ResourceGroupConfig{CPUWeight: 200},
		}},
		nil,
	))
	testutils.SucceedsSoon(t, func() error {
		cfg, ok := sub.Get(tenantA, 17)
		if !ok || cfg.CPUWeight != 200 {
			return errors.New("update not yet visible")
		}
		return nil
	})

	require.NoError(t, w.Apply(ctx, tenantB,
		[]*rgpb.ResourceGroupUpsert{{
			Id:     20,
			Name:   "b",
			Config: admissionpb.ResourceGroupConfig{CPUWeight: 50},
		}},
		nil,
	))
	testutils.SucceedsSoon(t, func() error {
		if _, ok := sub.Get(tenantB, 20); !ok {
			return errors.New("tenantB/20 not yet visible")
		}
		if _, ok := sub.Get(tenantA, 17); !ok {
			return errors.New("tenantA/17 disappeared after tenantB write")
		}
		snap := sub.Snapshot()
		if len(snap) != 2 {
			return errors.Newf("expected 2 tenants in snapshot, got %d", len(snap))
		}
		return nil
	})

	require.NoError(t, w.Apply(ctx, tenantA, nil,
		[]*rgpb.ResourceGroupDelete{{Id: 17}},
	))
	testutils.SucceedsSoon(t, func() error {
		if _, ok := sub.Get(tenantA, 17); ok {
			return errors.New("tenantA/17 still visible after delete")
		}
		if _, ok := sub.Snapshot()[tenantA]; ok {
			return errors.New("tenantA still in Snapshot after deleting its only row")
		}
		return nil
	})
}
