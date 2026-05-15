// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWriteTickManifestValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()

	te := hlc.Timestamp{
		WallTime: time.Date(2026, 5, 10, 12, 0, 10, 0, time.UTC).UnixNano(),
	}
	ts := hlc.Timestamp{WallTime: te.WallTime - 10*int64(time.Second)}

	t.Run("empty TickEnd", func(t *testing.T) {
		err := revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: ts,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "TickEnd must be set")
	})

	t.Run("empty TickStart", func(t *testing.T) {
		err := revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickEnd: te,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "TickStart must be set")
	})

	t.Run("TickStart == TickEnd", func(t *testing.T) {
		err := revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: te,
			TickEnd:   te,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "TickStart")
	})

	t.Run("TickStart > TickEnd", func(t *testing.T) {
		err := revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: te,
			TickEnd:   ts,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "TickStart")
	})

	t.Run("valid manifest succeeds", func(t *testing.T) {
		err := revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
			TickStart: ts,
			TickEnd:   te,
		})
		require.NoError(t, err)
	})
}

func TestTickWriterRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()

	te := hlc.Timestamp{
		WallTime: time.Date(2026, 5, 10, 12, 0, 10, 0, time.UTC).UnixNano(),
	}
	ts := hlc.Timestamp{WallTime: te.WallTime - 10*int64(time.Second)}

	tw, err := revlog.NewTickWriter(ctx, es, te, 1, 0)
	require.NoError(t, err)

	require.NoError(t, tw.Add(
		roachpb.Key("key1"),
		hlc.Timestamp{WallTime: te.WallTime - 5*int64(time.Second)},
		[]byte("val1"), nil,
	))
	require.NoError(t, tw.Add(
		roachpb.Key("key2"),
		hlc.Timestamp{WallTime: te.WallTime - 3*int64(time.Second)},
		[]byte("val2"), []byte("prev2"),
	))

	f, stats, err := tw.Close()
	require.NoError(t, err)
	require.Equal(t, int64(1), f.FileID)
	require.Equal(t, int32(0), f.FlushOrder)
	require.Equal(t, int64(2), stats.KeyCount)
	require.Greater(t, stats.LogicalBytes, int64(0))
	require.Greater(t, stats.SstBytes, int64(0))

	// Write manifest and read back.
	manifest := revlogpb.Manifest{
		TickStart: ts,
		TickEnd:   te,
		Files:     []revlogpb.File{f},
		Stats:     stats,
	}
	require.NoError(t, revlog.WriteTickManifest(ctx, es, manifest))

	lr := revlog.NewLogReader(es)
	var ticks []revlog.Tick
	for tk, tickErr := range lr.Ticks(ctx, ts, te) {
		require.NoError(t, tickErr)
		ticks = append(ticks, tk)
	}
	require.Len(t, ticks, 1)

	var events []revlog.Event
	for ev, evErr := range lr.GetTickReader(ctx, ticks[0], nil).Events(ctx) {
		require.NoError(t, evErr)
		events = append(events, ev)
	}
	require.Len(t, events, 2)
	require.Equal(t, roachpb.Key("key1"), events[0].Key)
	require.Equal(t, roachpb.Key("key2"), events[1].Key)
}
