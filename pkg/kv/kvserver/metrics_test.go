// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantsStorageMetricsRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	m := newTenantsStorageMetrics()
	var refs []*tenantMetricsRef
	const tenants = 7
	for i := 0; i < tenants; i++ {
		id := roachpb.MustMakeTenantID(roachpb.MinTenantID.InternalValue + uint64(i))
		ref := m.acquireTenant(id)
		tm := m.getTenant(context.Background(), ref)
		tm.SysBytes.Update(1023)
		tm.KeyCount.Inc(123)
		refs = append(refs, ref)
	}
	for i, ref := range refs {
		require.Equal(t, int64(1023*(tenants-i)), m.SysBytes.Value(), i)
		require.Equal(t, int64(123*(tenants-i)), m.KeyCount.Value(), i)
		m.releaseTenant(context.Background(), ref)
	}
	require.Zero(t, m.SysBytes.Value())
	require.Zero(t, m.KeyCount.Value())
}

// TestTenantsStorageMetricsConcurrency exercises the concurrency logic of the
// TenantsStorageMetrics and ensures that none of the assertions are hit.
// The test doesn't meaningfully exercise the logic which is tested elsewhere.
func TestTenantsStorageMetricsConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		tenants  = 3
		N        = 200
		maxSleep = time.Microsecond
		rounds   = 10
	)
	randDuration := func() time.Duration {
		return time.Duration(rand.Intn(int(maxSleep)))
	}

	var tenantIDs []roachpb.TenantID
	for id := uint64(1); id <= tenants; id++ {
		tenantIDs = append(tenantIDs, roachpb.MustMakeTenantID(id))
	}
	ctx := context.Background()
	sm := newTenantsStorageMetrics()
	// Launch N goroutines and have them all acquire a random tenant, then sleep
	// a random tiny amount, increment the metrics, then release. We want to
	// ensure that the refCount is never in an illegal state.
	run := func() {
		for i := 0; i < rounds; i++ {
			tid := tenantIDs[rand.Intn(tenants)]

			time.Sleep(randDuration())
			ref := sm.acquireTenant(tid)

			time.Sleep(randDuration())
			sm.incMVCCGauges(ctx, ref, enginepb.MVCCStats{})

			time.Sleep(randDuration())
			sm.releaseTenant(ctx, ref)
		}
	}
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() { defer wg.Done(); run() }()
	}
	wg.Wait()
}
