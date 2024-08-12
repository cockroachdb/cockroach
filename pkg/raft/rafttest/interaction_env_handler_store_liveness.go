// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rafttest

import (
	"fmt"
	"strings"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/datadriven"
)

// livenessEntry is an entry in the liveness fabric.
type livenessEntry struct {
	epoch       pb.Epoch
	isSupported bool
}

// livenessFabric is a global view of the store liveness state.
type livenessFabric struct {
	// sate is a 2D array, where state[i][j] represents store i's support for
	// store j. Stores are 1-indexed.
	state [][]livenessEntry
}

// newLivenessFabric initializes and returns a livenessFabric.
func newLivenessFabric() *livenessFabric {
	// Stores are 1-indexed. Pad our state with an empty, unusued throughout,
	// liveness entry.
	state := make([][]livenessEntry, 1)
	state[0] = make([]livenessEntry, 1)
	state[0][0] = livenessEntry{
		epoch:       1,
		isSupported: true,
	}
	return &livenessFabric{
		state: state,
	}
}

// addNode adds another node (store) to the liveness fabric.
func (l *livenessFabric) addNode() {
	// For every store that already exists, add another column for the node we're
	// adding.
	for i := 0; i < len(l.state); i++ {
		l.state[i] = append(l.state[i], livenessEntry{
			epoch:       1,
			isSupported: true,
		})
	}
	// Add a row for the node we're adding.
	state := make([]livenessEntry, len(l.state)+1) // 1-indexed stores
	for i := 0; i < len(l.state); i++ {
		state[i] = livenessEntry{
			epoch:       l.state[i][i].epoch,
			isSupported: true,
		}
	}
	l.state = append(l.state, state)

	// Finally, start the epoch for the node we've just added from 1. It supports
	// itself at this epoch.
	l.state[len(l.state)-1][len(l.state)-1].epoch = 1
	l.state[len(l.state)-1][len(l.state)-1].isSupported = true
}

func (l *livenessFabric) String() string {
	var buf strings.Builder
	for i := 0; i < len(l.state); i++ {
		for j := 0; j < len(l.state[i]); j++ {
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
func (s *storeLiveness) SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp, bool) {
	entry := s.livenessFabric.state[id][s.nodeID]
	// TODO(arul): we may need to inject timestamps in here as well.
	return entry.epoch, hlc.MaxTimestamp, entry.isSupported
}

// SupportFromEnabled implements the StoreLiveness interface.
func (s *storeLiveness) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the StoreLiveness interface.
func (s *storeLiveness) SupportExpired(hlc.Timestamp) bool {
	// TODO(arul): we may need to implement this if we start injecting timestamps.
	return false
}

// handleBumpEpoch handles the case where the epoch of a store is bumped and the
// store then seeks support from all other stores at this new epoch.
func (env *InteractionEnv) handleBumpEpoch(t *testing.T, d datadriven.TestData) error {
	n := firstAsInt(t, d)
	for i := 1; i < len(env.Fabric.state[n]); i++ {
		env.Fabric.state[i][n].epoch++
	}
	_, err := env.Output.WriteString(env.Fabric.String())
	return err
}

// handleBumpSupportFor handles the case where a store (s_local) stops
// supporting another store (s_remote), resulting in s_remote bumping its epoch
// and seeking support at this new epoch. S_local them provides support to
// s_remote at this new epoch.
func (env *InteractionEnv) handleBumpSupportFor(t *testing.T, d datadriven.TestData) error {
	forStore := nthAsInt(t, d, 0)
	byStore := nthAsInt(t, d, 1)
	// First, bump the forStore's epoch.
	env.Fabric.state[forStore][forStore].epoch++
	env.Fabric.state[byStore][forStore] = env.Fabric.state[forStore][forStore]
	env.Fabric.state[byStore][forStore].isSupported = true
	_, err := env.Output.WriteString(env.Fabric.String())
	return err
}

// handleWithdrawSupportFor handles the case where a remote store withdraws
// support from a given store. The epoch is left as is; the remote store may
// seek support from the store in the future by bumping its epoch.
func (env *InteractionEnv) handleWithdrawSupportFor(t *testing.T, d datadriven.TestData) error {
	forStore := nthAsInt(t, d, 0)
	byStore := nthAsInt(t, d, 1)
	env.Fabric.state[byStore][forStore].isSupported = false
	_, err := env.Output.WriteString(env.Fabric.String())
	return err
}
