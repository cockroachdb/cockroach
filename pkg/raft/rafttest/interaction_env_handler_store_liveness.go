// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttest

import (
	"fmt"
	"strings"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// livenessEntry is an entry in the liveness fabric.
type livenessEntry struct {
	epoch       pb.Epoch
	isSupported bool
}

// initLIvenessEntry is the initial liveness entry placed in the liveness fabric
// for new stores.
var initLivenessEntry = livenessEntry{
	epoch:       1,
	isSupported: true,
}

// livenessFabric is a global view of the store liveness state.
type livenessFabric struct {
	// state is a 2D array, where state[i][j] represents store i's support for
	// store j. Stores are 1-indexed.
	state [][]livenessEntry

	// leadSupportExpired tracks whether a store considers its leadSupportUntil
	// to be expired or not.
	leadSupportExpired []bool
}

// newLivenessFabric initializes and returns a livenessFabric.
func newLivenessFabric() *livenessFabric {
	// Stores are 1-indexed. Pad our state with an empty, unusued throughout,
	// liveness entry. This is useful in addNode below.
	state := make([][]livenessEntry, 1)
	state[0] = make([]livenessEntry, 1)
	state[0][0] = initLivenessEntry
	// Ditto for leadSupportExpired.
	leadSupportExpired := make([]bool, 1)
	leadSupportExpired[0] = false
	return &livenessFabric{
		state:              state,
		leadSupportExpired: leadSupportExpired,
	}
}

// addNode adds another node (store) to the liveness fabric.
func (l *livenessFabric) addNode() {
	// For every store that already exists, add another column for the node we're
	// adding.
	for i := range l.state {
		l.state[i] = append(l.state[i], initLivenessEntry)
	}
	// Add a row for the node we're adding.
	newNodeState := make([]livenessEntry, len(l.state)+1) // 1-indexed stores
	for i := range l.state {
		newNodeState[i] = livenessEntry{
			// NB: The most recent epoch nodes are using to seek support is stored at
			// l.state[i][i].epoch. Start providing support at this epoch.
			epoch:       l.state[i][i].epoch,
			isSupported: true,
		}
	}
	l.state = append(l.state, newNodeState)

	// Finally, initialize the liveness entry for the node we've just added. It'll
	// start off with epoch 1 and support itself.
	l.state[len(l.state)-1][len(l.state)-1] = initLivenessEntry
	l.leadSupportExpired = append(l.leadSupportExpired, false)
}

func (l *livenessFabric) String() string {
	var buf strings.Builder
	for i := range l.state {
		for j := range l.state[i] {
			if i == 0 && j == 0 {
				buf.WriteString(" ")
				continue
			}
			if i == 0 {
				buf.WriteString(fmt.Sprintf(" %d", j))
				continue
			}
			if j == 0 {
				buf.WriteString(fmt.Sprintf("%d", i))
				continue
			}

			buf.WriteString(" ")
			if l.state[i][j].isSupported {
				buf.WriteString(fmt.Sprintf("%d", l.state[i][j].epoch))
			} else {
				buf.WriteString("x")
			}
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

// storeLiveness is a per-peer view of the store liveness state.
type storeLiveness struct {
	livenessFabric *livenessFabric
	nodeID         pb.PeerID
}

var _ raftstoreliveness.StoreLiveness = &storeLiveness{}

func newStoreLiveness(livenessFabric *livenessFabric, nodeID pb.PeerID) *storeLiveness {
	return &storeLiveness{
		nodeID:         nodeID,
		livenessFabric: livenessFabric,
	}
}

// SupportFor implements the StoreLiveness interface.
func (s *storeLiveness) SupportFor(id pb.PeerID) (pb.Epoch, bool) {
	entry := s.livenessFabric.state[s.nodeID][id]
	return entry.epoch, entry.isSupported
}

// SupportFrom implements the StoreLiveness interface.
func (s *storeLiveness) SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp) {
	entry := s.livenessFabric.state[id][s.nodeID]
	if !entry.isSupported {
		return 0, hlc.Timestamp{}
	}
	// TODO(arul): we may need to inject timestamps in here as well.
	return entry.epoch, hlc.MaxTimestamp
}

// SupportFromEnabled implements the StoreLiveness interface.
func (s *storeLiveness) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the StoreLiveness interface.
func (s *storeLiveness) SupportExpired(ts hlc.Timestamp) bool {
	if s.livenessFabric.leadSupportExpired[s.nodeID] {
		return true
	}
	// If not configured explicitly, infer from the supplied timestamp.
	switch ts {
	case hlc.Timestamp{}:
		return true
	case hlc.MaxTimestamp:
		return false
	default:
		panic("unexpected timestamp")
	}
}

// handleBumpEpoch handles the case where the epoch of a store is bumped and the
// store then seeks support from all other stores at this new epoch.
func (env *InteractionEnv) handleBumpEpoch(t *testing.T, d datadriven.TestData) error {
	n := firstAsInt(t, d)
	for i := range env.Fabric.state {
		env.Fabric.state[i][n].epoch++
	}
	_, err := env.Output.WriteString(env.Fabric.String())
	return err
}

// handleWithdrawSupport handles the case where a store withdraws support for
// another store. The store for which support has been withdrawn may seek
// support from the store that withdrew support in the future by bumping its
// epoch.
func (env *InteractionEnv) handleWithdrawSupport(t *testing.T, d datadriven.TestData) error {
	fromStore := nthAsInt(t, d, 0)
	forStore := nthAsInt(t, d, 1)
	// Bump forStore's epoch and mark it as unsupported.
	env.Fabric.state[fromStore][forStore].epoch++
	env.Fabric.state[fromStore][forStore].isSupported = false
	_, err := env.Output.WriteString(env.Fabric.String())
	return err
}

// handleGrantSupport handles the case where a store grants support for another
// store. To enable this, the store for whom support is being granted must seek
// support at a higher epoch.
func (env *InteractionEnv) handleGrantSupport(t *testing.T, d datadriven.TestData) error {
	fromStore := nthAsInt(t, d, 0)
	forStore := nthAsInt(t, d, 1)
	if env.Fabric.state[fromStore][forStore].isSupported {
		return errors.Newf("store %d is already supporting %d", fromStore, forStore)
	}
	// First, copy over the bumped epoch (while ensuring it doesn't regress) onto
	// env.Fabric.state[forStore][forStore].epoch.
	env.Fabric.state[forStore][forStore].epoch = max(
		env.Fabric.state[forStore][forStore].epoch, env.Fabric.state[fromStore][forStore].epoch,
	)
	// Then, provide support from fromStore for forStore at this new epoch.
	env.Fabric.state[fromStore][forStore].epoch = env.Fabric.state[forStore][forStore].epoch
	env.Fabric.state[fromStore][forStore].isSupported = true
	_, err := env.Output.WriteString(env.Fabric.String())
	return err
}

// handleSupportExpired is a testing hook to configure whether a store
// considers its leadSupportUntil expired or not.
func (env *InteractionEnv) handleSupportExpired(t *testing.T, d datadriven.TestData) error {
	idx := firstAsInt(t, d)
	if d.HasArg("reset") {
		env.Fabric.leadSupportExpired[idx] = false
	} else {
		env.Fabric.leadSupportExpired[idx] = true
	}
	return nil
}
