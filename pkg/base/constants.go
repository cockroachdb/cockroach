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

package base

import "time"

const (
	// DefaultMaxClockOffset is the default maximum acceptable clock offset value.
	// On Azure, clock offsets between 250ms and 500ms are common. On AWS and GCE,
	// clock offsets generally stay below 250ms. See comments on Config.MaxOffset
	// for more on this setting.
	DefaultMaxClockOffset = 500 * time.Millisecond

	// DefaultHeartbeatInterval is how often heartbeats are sent from the
	// transaction coordinator to a live transaction. These keep it from
	// being preempted by other transactions writing the same keys. If a
	// transaction fails to be heartbeat within 2x the heartbeat interval,
	// it may be aborted by conflicting txns.
	DefaultHeartbeatInterval = 1 * time.Second

	// SlowRequestThreshold is the amount of time to wait before considering a
	// request to be "slow".
	SlowRequestThreshold = 60 * time.Second

	// ChunkRaftCommandThresholdBytes is the threshold in bytes at which
	// to chunk or otherwise limit commands being sent to Raft.
	ChunkRaftCommandThresholdBytes = 256 * 1000
)
