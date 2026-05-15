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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHasLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := newTestStorage(t)
	defer es.Close()

	t.Run("empty", func(t *testing.T) {
		has, err := revlog.HasLog(ctx, es)
		require.NoError(t, err)
		require.False(t, has, "empty storage should not have a log")
	})

	t.Run("with-tick", func(t *testing.T) {
		te := hlc.Timestamp{
			WallTime: time.Date(2026, 5, 10, 12, 0, 10, 0, time.UTC).UnixNano(),
		}
		manifest := revlogpb.Manifest{
			TickStart: hlc.Timestamp{WallTime: te.WallTime - 10*int64(time.Second)},
			TickEnd:   te,
		}
		require.NoError(t, revlog.WriteTickManifest(ctx, es, manifest))

		has, err := revlog.HasLog(ctx, es)
		require.NoError(t, err)
		require.True(t, has, "storage with a tick should have a log")
	})
}
