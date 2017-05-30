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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

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

	// DefaultSendNextTimeout is the duration to wait before trying another
	// replica to send a KV batch.
	//
	// TODO(peter): The SendNextTimeout mechanism is intended to lower tail
	// latencies by performing "speculative retries". In order to do so, the
	// timeout needs to by very low, but it can't be too low or we add
	// unnecessary network traffic and increase the likelihood of ambiguous
	// results. The SendNextTimeout mechanism has been the source of several bugs
	// and it is questionable about whether it is still necessary given that we
	// shut down the connection on heartbeat timeouts.
	//
	//   https://github.com/cockroachdb/cockroach/issues/15687
	//   https://github.com/cockroachdb/cockroach/issues/16119
	//
	// In advance of removing the SendNextTimeout mechanism completely, we're
	// setting the value very high and will be paying attention to tail
	// latencies.
	DefaultSendNextTimeout = 10 * time.Minute

	// DefaultPendingRPCTimeout is the duration to wait for outstanding RPCs
	// after receiving an error.
	DefaultPendingRPCTimeout = time.Minute

	// SlowRequestThreshold is the amount of time to wait before considering a
	// request to be "slow".
	SlowRequestThreshold = 60 * time.Second
)
