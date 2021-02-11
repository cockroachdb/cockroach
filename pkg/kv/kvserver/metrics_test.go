// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
)

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
		tenantIDs = append(tenantIDs, roachpb.MakeTenantID(id))
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
			sm.acquireTenant(tid)

			time.Sleep(randDuration())
			sm.incMVCCGauges(ctx, tid, enginepb.MVCCStats{})

			time.Sleep(randDuration())
			sm.releaseTenant(ctx, tid)
		}
	}
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() { defer wg.Done(); run() }()
	}
	wg.Wait()
}
