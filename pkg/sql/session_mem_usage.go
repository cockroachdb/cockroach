// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// noteworthyMemoryUsageBytes is the minimum size tracked by a
// transaction or session monitor before the monitor starts explicitly
// logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SESSION_MEMORY_USAGE", 1024*1024)

// StartMonitor interfaces between Session and mon.MemoryMonitor
func (s *Session) StartMonitor(pool *mon.BytesMonitor, reserved mon.BoundAccount) {
	// Note: we pass `reserved` to s.mon where it causes `s.mon` to act
	// as a buffer. This is not done for sessionMon nor TxnState.mon:
	// these monitors don't start with any buffer, so they'll need to
	// ask their "parent" for memory as soon as the first
	// allocation. This is acceptable because the session is single
	// threaded, and the point of buffering is just to avoid contention.
	s.mon = mon.MakeMonitor("root",
		mon.MemoryResource,
		s.memMetrics.CurBytesCount,
		s.memMetrics.MaxBytesHist,
		-1, math.MaxInt64, s.execCfg.Settings)
	s.mon.Start(s.context, pool, reserved)
	s.deriveAndStartMonitors()
}

// StartUnlimitedMonitor interfaces between Session and mon.MemoryMonitor
func (s *Session) StartUnlimitedMonitor() {
	s.mon = mon.MakeUnlimitedMonitor(s.context,
		"root",
		mon.MemoryResource,
		s.memMetrics.CurBytesCount,
		s.memMetrics.MaxBytesHist,
		math.MaxInt64,
		s.execCfg.Settings,
	)
	s.deriveAndStartMonitors()
}

func (s *Session) deriveAndStartMonitors() {
	s.sessionMon = mon.MakeMonitor("session",
		mon.MemoryResource,
		s.memMetrics.SessionCurBytesCount,
		s.memMetrics.SessionMaxBytesHist,
		-1, noteworthyMemoryUsageBytes, s.execCfg.Settings)
	s.sessionMon.Start(s.context, &s.mon, mon.BoundAccount{})

	// We merely prepare the txn monitor here. It is fully started in
	// resetForNewSQLTxn().
	s.TxnState.mon = mon.MakeMonitor("txn",
		mon.MemoryResource,
		s.memMetrics.TxnCurBytesCount,
		s.memMetrics.TxnMaxBytesHist,
		-1, noteworthyMemoryUsageBytes, s.execCfg.Settings)
}

func (s *Session) makeBoundAccount() mon.BoundAccount {
	return s.sessionMon.MakeBoundAccount()
}
