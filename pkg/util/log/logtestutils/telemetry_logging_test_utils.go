package logtestutils

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type StubTime struct {
	syncutil.RWMutex
	t time.Time
}

func (s *StubTime) SetTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

func (s *StubTime) TimeNow() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}

type StubQueryStats struct {
	syncutil.RWMutex
	stats execstats.QueryLevelStats
}

func (s *StubQueryStats) SetQueryLevelStats(stats execstats.QueryLevelStats) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.stats = stats
}

func (s *StubQueryStats) QueryLevelStats() execstats.QueryLevelStats {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.stats
}

type StubTracingStatus struct {
	syncutil.RWMutex
	isTracing bool
}

func (s *StubTracingStatus) SetTracingStatus(t bool) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.isTracing = t
}

func (s *StubTracingStatus) TracingStatus() bool {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.isTracing
}
