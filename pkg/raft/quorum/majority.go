// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quorum

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"golang.org/x/exp/maps"
)

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
type MajorityConfig map[pb.PeerID]struct{}

func (c MajorityConfig) String() string {
	var buf strings.Builder
	buf.WriteByte('(')
	for i, id := range c.Slice() {
		if i > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprint(&buf, id)
	}
	buf.WriteByte(')')
	return buf.String()
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
func (c MajorityConfig) Describe(l AckedIndexer) string {
	if len(c) == 0 {
		return "<empty majority quorum>"
	}
	type tup struct {
		id  pb.PeerID
		idx Index
		ok  bool // idx found?
		bar int  // length of bar displayed for this tup
	}

	// Below, populate .bar so that the i-th largest commit index has bar i (we
	// plot this as sort of a progress bar). The actual code is a bit more
	// complicated and also makes sure that equal index => equal bar.

	n := len(c)
	info := make([]tup, 0, n)
	for id := range c {
		idx, ok := l.AckedIndex(id)
		info = append(info, tup{id: id, idx: idx, ok: ok})
	}
	// Sort by (index, ID).
	slices.SortFunc(info, func(a, b tup) int {
		return cmp.Or(cmp.Compare(a.idx, b.idx), cmp.Compare(a.id, b.id))
	})
	// Populate .bar.
	for i := range info {
		if i > 0 && info[i-1].idx < info[i].idx {
			info[i].bar = i
		}
	}
	// Sort by ID.
	slices.SortFunc(info, func(a, b tup) int { return cmp.Compare(a.id, b.id) })

	var buf strings.Builder

	// Print.
	fmt.Fprint(&buf, strings.Repeat(" ", n)+"    idx\n")
	for _, t := range info {
		if !t.ok {
			fmt.Fprint(&buf, "?"+strings.Repeat(" ", n))
		} else {
			fmt.Fprint(&buf, strings.Repeat("x", t.bar)+">"+strings.Repeat(" ", n-t.bar))
		}
		fmt.Fprintf(&buf, " %5d    (id=%d)\n", t.idx, t.id)
	}
	return buf.String()
}

// Slice returns the MajorityConfig as a sorted slice.
func (c MajorityConfig) Slice() []pb.PeerID {
	if len(c) == 0 {
		return nil
	}
	peers := maps.Keys(c)
	slices.Sort(peers)
	return peers
}

// NB: A lot of logic in CommittedIndex, VoteResult, and LeadSupportExpiration
// can be de-duplicated by using generics. This was attempted in
// https://github.com/cockroachdb/cockroach/pull/128054, but eventually
// abandoned because of microbenchmark regressions.

// CommittedIndex computes the committed index from those supplied via the
// provided AckedIndexer (for the active config).
func (c MajorityConfig) CommittedIndex(l AckedIndexer) Index {
	n := len(c)
	if n == 0 {
		// This plays well with joint quorums which, when one half is the zero
		// MajorityConfig, should behave like the other half.
		return math.MaxUint64
	}

	// Use an on-stack slice to collect the committed indexes when n <= 7
	// (otherwise we alloc). The alternative is to stash a slice on
	// MajorityConfig, but this impairs usability (as is, MajorityConfig is just
	// a map, and that's nice). The assumption is that running with a
	// replication factor of >7 is rare, and in cases in which it happens
	// performance is a lesser concern (additionally the performance
	// implications of an allocation here are far from drastic).
	var stk [7]uint64
	var srt []uint64
	if len(stk) >= n {
		srt = stk[:n]
	} else {
		srt = make([]uint64, n)
	}

	{
		// Fill the slice with the indexes observed. Any unused slots will be
		// left as zero; these correspond to voters that may report in, but
		// haven't yet. We fill from the right (since the zeroes will end up on
		// the left after sorting below anyway).
		i := n - 1
		for id := range c {
			if idx, ok := l.AckedIndex(id); ok {
				srt[i] = uint64(idx)
				i--
			}
		}
	}
	slices.Sort(srt)

	// The smallest index into the array for which the value is acked by a
	// quorum. In other words, from the end of the slice, move n/2+1 to the
	// left (accounting for zero-indexing).
	pos := n - (n/2 + 1)
	return Index(srt[pos])
}

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending (i.e. neither a quorum of
// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
// quorum of no has been reached).
func (c MajorityConfig) VoteResult(votes map[pb.PeerID]bool) VoteResult {
	if len(c) == 0 {
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.
		return VoteWon
	}

	var votedCnt int // vote counts for yes.
	var missing int
	for id := range c {
		v, ok := votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			votedCnt++
		}
	}

	q := len(c)/2 + 1
	if votedCnt >= q {
		return VoteWon
	}
	if votedCnt+missing >= q {
		return VotePending
	}
	return VoteLost
}

// LeadSupportExpiration takes a slice of timestamps peers have promised a
// fortified leader support until and returns the timestamp until which the
// leader is guaranteed support until.
func (c MajorityConfig) LeadSupportExpiration(support []hlc.Timestamp) hlc.Timestamp {
	if len(c) == 0 {
		// There are no peers in the config, and therefore no leader, so we return
		// MaxTimestamp as a sentinel value. This also plays well with joint quorums
		// when one half is the zero MajorityConfig. In such cases, the joint config
		// should behave like the other half.
		return hlc.MaxTimestamp
	}

	n := len(c)
	slices.SortFunc(support, func(a hlc.Timestamp, b hlc.Timestamp) int {
		return a.Compare(b)
	})

	// We want the maximum timestamp that's supported by the quorum. The
	// assumption is that if a timestamp is supported by a peer, so are all
	// timestamps less than that timestamp. For this, we can simply consider the
	// quorum formed by picking the highest value elements and pick the minimum
	// from this. In other words, from our sorted (in increasing order) array
	// support, we want to move n/2 + 1 to the left from the end (accounting for
	// zero-indexing).
	pos := n - (n/2 + 1)
	return support[pos]
}
