// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// defaultClosedTimestampInterval specifies the target duration for
// the interval after which consistent reads from follower replicas
// are possible.
//
// An interval of zero disables follower reads.
//
// See the Follower Reads RFC for more information.
var defaultClosedTimestampInterval = envutil.EnvOrDefaultDuration("COCKROACH_CLOSED_TIMESTAMP_INTERVAL", 0)

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

// storeClosedTSMap provides closed timestamps for follower reads to
// ranges with epoch leaseholder matching the map's key and
// liveness epoch, and which have committed to the required min
// commit index. It is updated whenever a coalesced update is received
// from another store.
type storeClosedTSMap map[roachpb.StoreIdent]*storeClosedTS

// closedTSOutging aggregates closed timestamp updates to be sent to other stores.
// They are periodically sent out, similar to coalesced heartbeats.
type closedTSOutgoing struct {
	// Follower reads updates collected for the next outgoing update to other
	// stores. Replicas update this whenever they propose or quiesce, which
	// typically applies only to leaseholders (if a non-leaseholder adds an
	// update here, the recipients will discard it).
	infosByRange map[roachpb.RangeID]*ClosedTSInfo
	// Recipients for the infosByRange updates, with corresponding infosByRange
	// duplicated within.
	//
	// TODO(tschottdorf): this really only needs to be a map[StoreIdent]map[RangeID]struct{}.
	// This saves a small amount of memory, spares GC from tracing all of the
	// pointers, and is also less confusing. Perhaps it can even be inferred
	// completely from the infosByRange map at sending time; the peers are in
	// the range descriptor.
	closedTSInfos map[roachpb.StoreIdent]map[roachpb.RangeID]*ClosedTSInfo
	// Keeps track of the sequence number to send to other stores. It is
	// incremented by one with each update; when the recipient detects a gap
	// (which indicates a lost/reordered heartbeat), it must assume that all
	// quiescent ranges on the origin store have unquiesced and updates it
	// storeClosedTSMap accordingly.
	//
	// TODO(tschottdorf): this could just be an int64 (i.e. not
	// tracked per store), but this also serves a more subtle use case:
	// Assume a peer store has only quiesced ranges; it wouldn't receive
	// any update from this node proactively. We need to somehow keep
	// track of the fact that that peer still needs to receive a store
	// closed timestamp regularly. This map is used for that purpose
	// by making sure that we send to every store within in each round
	// of updates, even if all we're sharing is the closed timestamp.
	// This approach comes with the problem that when a store disappears,
	// we'll try sending to it "forever". There needs to be some expiration;
	// we should probably just send to all live stores instead. Left as a
	// separate PR.
	closedSequences map[roachpb.StoreIdent]int64
}
