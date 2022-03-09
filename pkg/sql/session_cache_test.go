package sql

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSessionCacheBasic(t *testing.T) {
	var cache *SessionCache

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var capacity, lifetime int
				d.ScanArgs(t, "capacity", &capacity)
				d.ScanArgs(t, "timeToLive", &lifetime)
				cache = NewSessionCache(capacity, int64(lifetime))

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "addSession":
				var idStr string
				d.ScanArgs(t, "id", &idStr)
				id, err := uint128.FromString(idStr)
				require.NoError(t, err)

				session := &connExecutor{}
				sessionId := ClusterWideID{id}
				cache.add(sessionId, session)

				return fmt.Sprintf("cache_size: %d", cache.size())
			case "addSessionBatch":
				var startIdStr string
				var sessions int
				var seconds int
				d.ScanArgs(t, "startId", &startIdStr)
				d.ScanArgs(t, "sessions", &sessions)
				d.ScanArgs(t, "seconds", &seconds)
				id, err := uint128.FromString(startIdStr)
				require.NoError(t, err)

				for i := 0; i < sessions; i++ {
					time.Sleep(time.Duration(seconds) * time.Second)
					session := &connExecutor{}
					sessionId := ClusterWideID{id}
					cache.add(sessionId, session)
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
