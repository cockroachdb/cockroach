// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leasemanager_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

const (
	clientID1 = "1"
	clientID2 = "2"
)

var (
	leaseKey = roachpb.Key("/SystemVersion/lease")
)

func TestAcquireAndRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	lm := leasemanager.New(db, clock, leasemanager.Options{ClientID: clientID1})

	l, err := lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); !testutils.IsError(err, "unexpected value") {
		t.Fatal(err)
	}

	l, err = lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
}

func TestReacquireLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	lm := leasemanager.New(db, clock, leasemanager.Options{ClientID: clientID1})

	if _, err := lm.AcquireLease(ctx, leaseKey); err != nil {
		t.Fatal(err)
	}

	// We allow re-acquiring the same lease as long as the client ID is
	// the same to allow a client to reacquire its own leases rather than
	// having to wait them out if it crashes and restarts.
	l, err := lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
}

func TestExtendLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	lm := leasemanager.New(db, clock, leasemanager.Options{ClientID: clientID1})

	l, err := lm.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}

	manual.Increment(int64(time.Second))
	timeRemainingBefore := lm.TimeRemaining(l)
	if err := lm.ExtendLease(ctx, l); err != nil {
		t.Fatal(err)
	}
	timeRemainingAfter := lm.TimeRemaining(l)
	if !(timeRemainingAfter > timeRemainingBefore) {
		t.Errorf("expected time remaining after renewal (%s) to be greater than before renewal (%s)",
			timeRemainingAfter, timeRemainingBefore)
	}

	manual.Increment(int64(leasemanager.DefaultLeaseDuration) + 1)
	if tr := lm.TimeRemaining(l); tr >= 0 {
		t.Errorf("expected negative time remaining on lease, got %s", tr)
	}
	if err := lm.ExtendLease(ctx, l); !testutils.IsError(err, "can't extend lease that expired") {
		t.Fatalf("didn't get expected error when renewing lease %+v: %v", l, err)
	}

	if err := lm.ReleaseLease(ctx, l); err != nil {
		t.Fatal(err)
	}
}

func TestLeasesMultipleClients(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	manual1 := hlc.NewManualClock(123)
	clock1 := hlc.NewClock(manual1.UnixNano, time.Nanosecond)
	manual2 := hlc.NewManualClock(123)
	clock2 := hlc.NewClock(manual2.UnixNano, time.Nanosecond)
	lm1 := leasemanager.New(db, clock1, leasemanager.Options{ClientID: clientID1})
	lm2 := leasemanager.New(db, clock2, leasemanager.Options{ClientID: clientID2})

	l1, err := lm1.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}
	_, err = lm2.AcquireLease(ctx, leaseKey)
	if !testutils.IsError(err, "is not available until") {
		t.Fatalf("didn't get expected error trying to acquire already held lease: %v", err)
	}
	if !errors.HasType(err, (*leasemanager.LeaseNotAvailableError)(nil)) {
		t.Fatalf("expected LeaseNotAvailableError, got %v", err)
	}

	// Ensure a lease can be "stolen" after it's expired.
	manual2.Increment(int64(leasemanager.DefaultLeaseDuration) + 1)
	l2, err := lm2.AcquireLease(ctx, leaseKey)
	if err != nil {
		t.Fatal(err)
	}

	// lm1's clock indicates that its lease should still be valid, but it doesn't
	// own it anymore.
	manual1.Increment(int64(leasemanager.DefaultLeaseDuration) / 2)
	if err := lm1.ExtendLease(ctx, l1); !testutils.IsError(err, "out of sync with DB state") {
		t.Fatalf("didn't get expected error trying to extend expired lease: %v", err)
	}
	if err := lm1.ReleaseLease(ctx, l1); !testutils.IsError(err, "unexpected value") {
		t.Fatalf("didn't get expected error trying to release stolen lease: %v", err)
	}

	if err := lm2.ReleaseLease(ctx, l2); err != nil {
		t.Fatal(err)
	}
}
