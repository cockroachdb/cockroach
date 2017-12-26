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

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ClosedTimestampInterval specifies the target duration for the
// interval after which consistent reads from follower replicas are
// possible.
//
// See /docs/tech-notes/follower-reads.md for a detailed explanation.
const ClosedTimestampInterval = 5 * time.Second

// storeClosedTS contains a set of quiesced range IDs, a store-wide
// closed timestamp, a sequence number to ensure continuity of
// heartbeats, and a liveness epoch for which this information should
// be considered valid. This information is maintained on behalf of
// remote leader/leaseholders and is updated on heartbeats. If the
// sequence number fails to increase by exactly one on successive
// heartbeats, the quiesced map is reset.
type storeClosedTS struct {
	hlc.Timestamp
	sequence int64
	epoch    int64
	quiesced map[roachpb.RangeID]struct{}
}
