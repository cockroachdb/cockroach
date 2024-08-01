// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
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
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
type MajorityConfig map[pb.PeerID]struct{}

func (c MajorityConfig) String() string {
	sl := make([]pb.PeerID, 0, len(c))
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	var buf strings.Builder
	buf.WriteByte('(')
	for i := range sl {
		if i > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprint(&buf, sl[i])
	}
	buf.WriteByte(')')
	return buf.String()
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
func (c MajorityConfig) Describe(l ComparableMap[Index]) string {
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
		idx, ok := l.Get(id)
		info = append(info, tup{id: id, idx: idx, ok: ok})
	}

	// Sort by index
	sort.Slice(info, func(i, j int) bool {
		if info[i].idx == info[j].idx {
			return info[i].id < info[j].id
		}
		return info[i].idx < info[j].idx
	})

	// Populate .bar.
	for i := range info {
		if i > 0 && info[i-1].idx < info[i].idx {
			info[i].bar = i
		}
	}

	// Sort by ID.
	sort.Slice(info, func(i, j int) bool {
		return info[i].id < info[j].id
	})

	var buf strings.Builder

	// Print.
	fmt.Fprint(&buf, strings.Repeat(" ", n)+"    idx\n")
	for i := range info {
		bar := info[i].bar
		if !info[i].ok {
			fmt.Fprint(&buf, "?"+strings.Repeat(" ", n))
		} else {
			fmt.Fprint(&buf, strings.Repeat("x", bar)+">"+strings.Repeat(" ", n-bar))
		}
		fmt.Fprintf(&buf, " %5d    (id=%d)\n", info[i].idx, info[i].id)
	}
	return buf.String()
}

// Slice returns the MajorityConfig as a sorted slice.
func (c MajorityConfig) Slice() []pb.PeerID {
	var sl []pb.PeerID
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	return sl
}

// CommittedIndex computes the committed index from those supplied via the
// provided AckedIndexer (for the active config).
func (c MajorityConfig) CommittedIndex(l AckedIndexer) Index {
	n := len(c)
	if n == 0 {
		// This plays well with joint quorums which, when one half is the zero
		// MajorityConfig, should behave like the other half.
		return math.MaxUint64
	}

	// The commit index is the smallest index that's been acked by all replicas in
	// a quorum. For the majority config, we want the largest such index across
	// all quorums. quorumSupportedElement will give us that.
	return computeElement(c, l)
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

	// NB: We could use computeElement here instead. However, this logic is a bit
	// more intuitive.
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

// supportMap allows looking up fortification support provided to the leader by
// a given peer.
type supportMap map[pb.PeerID]hlc.Timestamp

// Get implements the ComparableMap interface.
func (s supportMap) Get(id pb.PeerID) (hlc.Timestamp, bool) {
	idx, ok := s[id]
	return idx, ok
}

// LeadSupportExpiration takes a mapping of timestamps peers have promised a
// fortified leader support until and returns the timestamp until which the
// leader is guaranteed support until.
func (c MajorityConfig) LeadSupportExpiration(supported supportMap) hlc.Timestamp {
	if len(c) == 0 {
		// There are no peers in the config, and therefore no leader, so we return
		// MaxTimestamp as a sentinel value. This also plays well with joint quorums
		// when one half is the zero MajorityConfig. In such cases, the joint config
		// should behave like the other half.
		return hlc.MaxTimestamp
	}

	// As per the definition of QSE above, quorumSupportedElement should give us
	// what we need.
	return computeElement(c, supported)
}

// Comparable is a thin interface that allows computeElement to compare
// elements.
type Comparable[T any] interface {
	Compare(T) int
}

// ComparableMap is a thin interface that allows computeElement to map peer IDs
// in a config to comparable elements.
type ComparableMap[T Comparable[T]] interface {
	Get(id pb.PeerID) (T, bool)
}

// computeElement returns the maximum element that's supported by a majority in
// the provided configuration. We assume that if an element is supported by a
// peer, then all elements with smaller values (as defined by the Comparable
// ordering) are also supported by it.
//
// Elements must be comparable to each other. If an element does not exist for
// a peer then it is treated as the zero value and sorts before all other
// elements.
func computeElement[E Comparable[E], M ComparableMap[E]](c MajorityConfig, elems M) E {
	n := len(c)

	// Use an on-stack slice whenever n <= 7 (otherwise we alloc). The assumption
	// is that running with a replication factor of >7 is rare, and in cases in
	// which it happens, performance is less of a concern (it's not like
	// performance implications of an allocation here are drastic).
	var stk [7]E
	var srt []E
	if len(stk) >= n {
		srt = stk[:n]
	} else {
		srt = make([]E, n)
	}

	{
		// Fill the slice with elements. Any unused slots will be left as zero/empty
		// for our calculation. We fill from the right (since the zeros will end up
		// on the left after sorting anyway).
		i := n - 1
		for id := range c {
			if e, ok := elems.Get(id); ok {
				srt[i] = e
				i--
			}
		}
	}

	slices.SortFunc(srt, func(a, b E) int {
		return a.Compare(b)
	})

	// We want the maximum element supported by the quorum, with the assumption
	// that if an element is supported by a peer, so are all elements with smaller
	// values (as defined by the Comparable ordering). For this, we can simply
	// consider the quorum formed by picking the highest value elements and pick
	// the minimum from this. In other words, from our sorted (in increasing
	// order) array srt, we want to move n/2 + 1 to the left from the end
	// (accounting for zero-indexing).
	pos := n - (n/2 + 1)
	return srt[pos]
}
