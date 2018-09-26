// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTableHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ts := func(wt int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wt} }

	validateFn := func(_ context.Context, desc *sqlbase.TableDescriptor) error {
		if desc.Name != `` {
			return errors.New(desc.Name)
		}
		return nil
	}
	requireChannelEmpty := func(t *testing.T, ch chan error) {
		t.Helper()
		select {
		case err := <-ch:
			t.Fatalf(`expected empty channel got %v`, err)
		default:
		}
	}

	m := makeTableHistory(validateFn, ts(0))

	require.Equal(t, ts(0), m.HighWater())

	// advance
	require.NoError(t, m.IngestDescriptors(ctx, ts(0), ts(1), nil))
	require.Equal(t, ts(1), m.HighWater())
	require.NoError(t, m.IngestDescriptors(ctx, ts(1), ts(2), nil))
	require.Equal(t, ts(2), m.HighWater())

	// no-ops
	require.NoError(t, m.IngestDescriptors(ctx, ts(0), ts(1), nil))
	require.Equal(t, ts(2), m.HighWater())
	require.NoError(t, m.IngestDescriptors(ctx, ts(1), ts(2), nil))
	require.Equal(t, ts(2), m.HighWater())

	// overlap
	require.NoError(t, m.IngestDescriptors(ctx, ts(1), ts(3), nil))
	require.Equal(t, ts(3), m.HighWater())

	// gap
	require.EqualError(t, m.IngestDescriptors(ctx, ts(4), ts(5), nil),
		`gap between 0.000000003,0 and 0.000000004,0`)
	require.Equal(t, ts(3), m.HighWater())

	// validates
	require.NoError(t, m.IngestDescriptors(ctx, ts(3), ts(4), []*sqlbase.TableDescriptor{
		{ID: 0},
	}))
	require.Equal(t, ts(4), m.HighWater())

	// high-water already high enough. fast-path
	require.NoError(t, m.WaitForTS(ctx, ts(3)))
	require.NoError(t, m.WaitForTS(ctx, ts(4)))

	// high-water not there yet. blocks
	errCh6 := make(chan error, 1)
	errCh7 := make(chan error, 1)
	go func() { errCh7 <- m.WaitForTS(ctx, ts(7)) }()
	go func() { errCh6 <- m.WaitForTS(ctx, ts(6)) }()
	requireChannelEmpty(t, errCh6)
	requireChannelEmpty(t, errCh7)

	// high-water advances, but not enough
	require.NoError(t, m.IngestDescriptors(ctx, ts(4), ts(5), nil))
	requireChannelEmpty(t, errCh6)
	requireChannelEmpty(t, errCh7)

	// high-water advances, unblocks only errCh6
	require.NoError(t, m.IngestDescriptors(ctx, ts(5), ts(6), nil))
	require.NoError(t, <-errCh6)
	requireChannelEmpty(t, errCh7)

	// high-water advances again, unblocks errCh7
	require.NoError(t, m.IngestDescriptors(ctx, ts(6), ts(7), nil))
	require.NoError(t, <-errCh7)

	// validate ctx cancellation
	errCh8 := make(chan error, 1)
	ctxTS8, cancelTS8 := context.WithCancel(ctx)
	go func() { errCh8 <- m.WaitForTS(ctxTS8, ts(8)) }()
	requireChannelEmpty(t, errCh8)
	cancelTS8()
	require.EqualError(t, <-errCh8, `context canceled`)

	// does not validate, high-water does not change
	require.EqualError(t, m.IngestDescriptors(ctx, ts(7), ts(10), []*sqlbase.TableDescriptor{
		{ID: 0, Name: `whoops!`},
	}), `whoops!`)
	require.Equal(t, ts(7), m.HighWater())

	// ts 10 has errored, so validate can return its error without blocking
	require.EqualError(t, m.WaitForTS(ctx, ts(10)), `whoops!`)

	// ts 8 and 9 are still unknown
	errCh8 = make(chan error, 1)
	errCh9 := make(chan error, 1)
	go func() { errCh8 <- m.WaitForTS(ctx, ts(8)) }()
	go func() { errCh9 <- m.WaitForTS(ctx, ts(9)) }()
	requireChannelEmpty(t, errCh8)
	requireChannelEmpty(t, errCh9)

	// turns out ts 10 is not a tight bound. ts 9 also has an error
	require.EqualError(t, m.IngestDescriptors(ctx, ts(7), ts(9), []*sqlbase.TableDescriptor{
		{ID: 0, Name: `oh no!`},
	}), `oh no!`)
	require.Equal(t, ts(7), m.HighWater())
	require.EqualError(t, <-errCh9, `oh no!`)

	// ts 8 is still unknown
	requireChannelEmpty(t, errCh8)

	// always return the earlist error seen (so waiting for ts 10 immediately
	// returns the 9 error now, it returned the ts 10 error above)
	require.EqualError(t, m.WaitForTS(ctx, ts(9)), `oh no!`)

	// something earlier than ts 10 can still be okay
	require.NoError(t, m.IngestDescriptors(ctx, ts(7), ts(8), nil))
	require.Equal(t, ts(8), m.HighWater())
	require.NoError(t, <-errCh8)
}
