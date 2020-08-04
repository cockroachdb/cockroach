// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slinstance_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSQLInstance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func(old int) {
		slinstance.DefaultMaxRetriesToExtend = old
	}(slinstance.DefaultMaxRetriesToExtend)
	slinstance.DefaultMaxRetriesToExtend = 1

	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	mClock := hlc.NewManualClock(hlc.UnixNano())
	clock := hlc.NewClock(mClock.UnixNano, time.Nanosecond)
	settings := cluster.MakeClusterSettings()
	slinstance.DefaultTTL.Override(&settings.SV, 2*time.Microsecond)
	slinstance.DefaultHeartBeat.Override(&settings.SV, time.Microsecond)

	fakeStorage := slstorage.NewFakeStorage()
	sqlInstance := slinstance.NewSqlInstance(stopper, clock, fakeStorage, settings)
	sqlInstance.Start(ctx)

	s1, err := sqlInstance.Session(ctx)
	require.NoError(t, err)
	a, err := fakeStorage.IsAlive(ctx, nil, s1.ID())
	require.NoError(t, err)
	require.True(t, a)

	s2, err := sqlInstance.Session(ctx)
	require.NoError(t, err)
	require.Equal(t, s1.ID(), s2.ID())

	_ = fakeStorage.Delete(ctx, s2)
	t.Logf("Deleted session %s", s2.ID())
	a, err = fakeStorage.IsAlive(ctx, nil, s2.ID())
	require.NoError(t, err)
	require.False(t, a)

	opts := retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
	}
	var s3 sqlliveness.Session
	if err := retry.WithMaxAttempts(ctx, opts, 10, func() error {
		s3, err = sqlInstance.Session(ctx)
		if s3.ID().String() == s2.ID().String() {
			return errors.Errorf("Session %s did not expire", s2.ID())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a, err = fakeStorage.IsAlive(ctx, nil, s3.ID())
	require.NoError(t, err)
	require.True(t, a)
	require.NotEqual(t, s2.ID(), s3.ID())
}
