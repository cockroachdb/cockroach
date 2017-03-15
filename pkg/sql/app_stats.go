// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//

package sql

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// appStats holds per-application statistics.
type appStats struct {
	syncutil.Mutex

	stmtCount int
}

func (a *appStats) recordStatement() {
	if a == nil {
		return
	}
	a.Lock()
	a.stmtCount++
	a.Unlock()
}

// sqlStats carries per-application statistics for all applications on
// each node. It hangs off Executor.
type sqlStats struct {
	syncutil.Mutex

	// apps is the container for all the per-application statistics
	// objects.
	apps map[string]*appStats
}

// resetApplicationName initializes both Session.ApplicationName and
// the cached pointer to per-application statistics. It is meant to be
// used upon session initialization and upon SET APPLICATION_NAME.
func (s *Session) resetApplicationName(appName string) {
	s.ApplicationName = appName
	if s.sqlStats != nil {
		s.appStats = s.sqlStats.getStatsForApplication(appName)
	}
}

func (s *sqlStats) getStatsForApplication(appName string) *appStats {
	s.Lock()
	defer s.Unlock()
	if a, ok := s.apps[appName]; ok {
		return a
	}
	a := &appStats{}
	s.apps[appName] = a
	return a
}
