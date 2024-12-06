// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// addSeconds is used to adjust the cache's perception of time without having
// the CI actually do the waiting.
func addSeconds(curr func() time.Time, seconds int) func() time.Time {
	return func() time.Time {
		return curr().Local().Add(time.Duration(seconds) * time.Second)
	}
}

func TestSessionCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cache *ClosedSessionCache

	datadriven.Walk(t, datapathutils.TestDataPath(t, "closed_session_cache"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			ctx := context.Background()
			switch d.Cmd {
			case "init":
				var capacity, timeToLive int
				d.ScanArgs(t, "capacity", &capacity)
				d.ScanArgs(t, "timeToLive", &timeToLive)

				st := &cluster.Settings{}
				monitor := mon.NewUnlimitedMonitor(ctx, mon.Options{
					Name:     mon.MakeMonitorName("test"),
					Settings: st,
				})
				cache = NewClosedSessionCache(st, monitor, time.Now)

				closedSessionCacheCapacity.Override(ctx, &st.SV, int64(capacity))
				closedSessionCacheTimeToLive.Override(ctx, &st.SV, int64(timeToLive))

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "addSession":
				var idStr string
				d.ScanArgs(t, "id", &idStr)
				id, err := uint128.FromString(idStr)
				require.NoError(t, err)

				session := serverpb.Session{}
				sessionID := clusterunique.ID{Uint128: id}
				err = cache.add(ctx, sessionID, session)
				require.NoError(t, err)

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "addSessionBatch":
				var startIDStr string
				var sessions int
				var seconds int
				d.ScanArgs(t, "startId", &startIDStr)
				d.ScanArgs(t, "sessions", &sessions)
				d.ScanArgs(t, "seconds", &seconds)
				id, err := uint128.FromString(startIDStr)
				require.NoError(t, err)

				for i := 0; i < sessions; i++ {
					cache.timeSrc = addSeconds(cache.timeSrc, seconds)
					session := serverpb.Session{}
					sessionID := clusterunique.ID{Uint128: id}
					err := cache.add(ctx, sessionID, session)
					require.NoError(t, err)
					id = id.Add(1)
				}

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "wait":
				var seconds int
				d.ScanArgs(t, "seconds", &seconds)
				cache.timeSrc = addSeconds(cache.timeSrc, seconds)
				return "ok"
			case "clear":
				cache.clear()

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "show":
				var result []string

				sessions := cache.getSessions()
				for _, s := range sessions {
					result = append(result, fmt.Sprintf("id: %s age: %s session: {}", s.id, s.getAgeString(cache.timeSrc)))
				}
				if len(result) == 0 {
					return "empty"
				}
				return strings.Join(result, "\n")
			}
			return ""
		})
	})

}
