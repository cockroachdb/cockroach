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

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/datadriven"
)

type livenessEntry struct {
	epoch       raft.StoreLivenessEpoch
	isSupported bool
}

// globalLivenessFabric is a global view of store liveness.
type globalLivenessFabric struct {
	// state is the 2D array, where state[i][j] represents store i's support for
	// store j.
	state [][]livenessEntry
}

func newGlobalLivenessFabric(numNodes int) *globalLivenessFabric {
	// NB: IDs are 1 indexed.
	livenessFabric := make([][]livenessEntry, numNodes+1)
	for i := range livenessFabric {
		livenessFabric[i] = make([]livenessEntry, numNodes+1)
	}
	// Initialize the liveness state to epoch 1.
	for i := 1; i <= numNodes; i++ {
		for j := 1; j <= numNodes; j++ {
			livenessFabric[i][j] = livenessEntry{
				epoch:       1,
				isSupported: true,
			}
		}
	}
	return &globalLivenessFabric{
		state: livenessFabric,
	}
}

func (g *globalLivenessFabric) addNode() {
	for i := 1; i < len(g.state); i++ {
		g.state[i] = append(g.state[i], livenessEntry{
			epoch:       g.state[i][i].epoch,
			isSupported: true,
		})
	}
	arr := make([]livenessEntry, len(g.state)+1)
	for i := 1; i < len(g.state); i++ {
		arr[i] = livenessEntry{
			epoch:       1,
			isSupported: true,
		}
	}
	g.state = append(g.state, arr)
}

func (g *globalLivenessFabric) String() string {
	var buf strings.Builder
	for i := 0; i < len(g.state); i++ {
		for j := 0; j < len(g.state); j++ {
			if i == 0 && j == 0 {
				buf.WriteString(" ")
			} else if i == 0 {
				buf.WriteString(fmt.Sprintf(" %d", j))
			} else if j == 0 {
				buf.WriteString(fmt.Sprintf("%d", i))
			} else {
				buf.WriteString(" ")
				if g.state[i][j].isSupported {
					buf.WriteString(fmt.Sprintf("%d", g.state[i][j].epoch))
				} else {
					buf.WriteString("x")
				}
			}
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

type storeLiveness struct {
	fabric *globalLivenessFabric // read-only
	nodeID uint64
}

var _ raft.StoreLiveness = &storeLiveness{}

// Enabled implements the StoreLiveness interface.
func (s *storeLiveness) Enabled() bool {
	return true
}

// SupportFor implements the StoreLiveness interface.
func (s *storeLiveness) SupportFor(id uint64) (raft.StoreLivenessEpoch, bool) {
	return s.fabric.state[s.nodeID][id].epoch, s.fabric.state[s.nodeID][id].isSupported
}

// SupportFrom implements the StoreLiveness interface.
func (s *storeLiveness) SupportFrom(
	id uint64,
) (raft.StoreLivenessEpoch, raft.StoreLivenessExpiration, bool) {
	return s.fabric.state[id][s.nodeID].epoch,
		raft.StoreLivenessExpiration(hlc.MaxTimestamp),
		s.fabric.state[id][s.nodeID].isSupported
}

// handleBumpEpoch handles the case where the epoch of a store is bumped and the
// store then seeks support from all other stores at this new epoch.
func (env *InteractionEnv) handleBumpEpoch(t *testing.T, d datadriven.TestData) error {
	n := firstAsInt(t, d)
	for i := 1; i < len(env.fabric.state); i++ {
		for j := 1; j < len(env.fabric.state); j++ {
			if j == n {
				env.fabric.state[i][j].epoch++
			}
		}
	}
	_, err := env.Output.WriteString(env.fabric.String())
	return err
}

// handleBumpSupport handles the case where a remote store withdraws support
// from a given store. In doing so, the store's epoch is incremented and the
// remote store starts supporting the new epoch. All other remote stores are
// left unaffected.
func (env *InteractionEnv) handleBumpSupport(t *testing.T, d datadriven.TestData) error {
	forStore := nthAsInt(t, d, 0)
	byStore := nthAsInt(t, d, 1)
	// First off, bump the forStore's epoch.
	env.fabric.state[forStore][forStore].epoch++
	env.fabric.state[byStore][forStore] = env.fabric.state[forStore][forStore]
	env.fabric.state[byStore][forStore].isSupported = true
	_, err := env.Output.WriteString(env.fabric.String())
	return err
}

// handleWithdrawSupport handles the case where a remote store withdraws support
// from a given store. The epoch is left as is; the store may start supporting the
// remote store in the future by bumping its epoch and providing support at that
// higher epoch.
func (env *InteractionEnv) handleWithdrawSupport(t *testing.T, d datadriven.TestData) error {
	forStore := nthAsInt(t, d, 0)
	byStore := nthAsInt(t, d, 1)
	env.fabric.state[byStore][forStore].isSupported = false
	_, err := env.Output.WriteString(env.fabric.String())
	return err
}
