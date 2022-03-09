// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestSessionCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cache *SessionCache

	datadriven.Walk(t, testutils.TestDataPath(t, "session_cache"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var capacity, timeToLive int
				d.ScanArgs(t, "capacity", &capacity)
				d.ScanArgs(t, "timeToLive", &timeToLive)

				ctx := context.Background()
				st := &cluster.Settings{}
				cache = NewSessionCache(st)

				SessionCacheCapacity.Override(ctx, &st.SV, int64(capacity))
				SessionCacheTimeToLive.Override(ctx, &st.SV, int64(timeToLive))

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "addSession":
				var idStr string
				d.ScanArgs(t, "id", &idStr)
				id, err := uint128.FromString(idStr)
				require.NoError(t, err)

				session := &connExecutor{}
				sessionID := ClusterWideID{id}
				cache.Add(sessionID, session)

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
					time.Sleep(time.Duration(seconds) * time.Second)
					session := &connExecutor{}
					sessionID := ClusterWideID{id}
					cache.Add(sessionID, session)
					id = id.Add(1)
				}

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "wait":
				var secondsStr int
				d.ScanArgs(t, "seconds", &secondsStr)
				time.Sleep(time.Duration(secondsStr) * time.Second)

				return "ok"
			case "show":
				return cache.viewCachedSessions()
			}
			return ""
		})
	})

}
