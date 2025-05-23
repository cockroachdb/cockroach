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

package rafttest

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/raft"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

func (env *InteractionEnv) handleAddNodes(t *testing.T, d datadriven.TestData) error {
	n := firstAsInt(t, d)
	var snap pb.Snapshot
	cfg := raftConfigStub()
	// NB: the datadriven tests use the async storage API, but have an option to
	// sync writes immediately in imitation of the previous synchronous API.
	var asyncWrites bool

	for _, arg := range d.CmdArgs[1:] {
		for i := range arg.Vals {
			switch arg.Key {
			case "voters":
				var rawID uint64
				arg.Scan(t, i, &rawID)
				id := pb.PeerID(rawID)
				snap.Metadata.ConfState.Voters = append(snap.Metadata.ConfState.Voters, id)
			case "learners":
				var rawID uint64
				arg.Scan(t, i, &rawID)
				id := pb.PeerID(rawID)
				snap.Metadata.ConfState.Learners = append(snap.Metadata.ConfState.Learners, id)
			case "inflight":
				arg.Scan(t, i, &cfg.MaxInflightMsgs)
			case "index":
				arg.Scan(t, i, &snap.Metadata.Index)
				cfg.Applied = snap.Metadata.Index
			case "content":
				arg.Scan(t, i, &snap.Data)
			case "async-storage-writes":
				arg.Scan(t, i, &asyncWrites)
			case "lazy-replication":
				arg.Scan(t, i, &cfg.LazyReplication)
			case "prevote":
				arg.Scan(t, i, &cfg.PreVote)
			case "checkquorum":
				arg.Scan(t, i, &cfg.CheckQuorum)
			case "max-committed-size-per-ready":
				arg.Scan(t, i, &cfg.MaxCommittedSizePerReady)
			case "disable-conf-change-validation":
				arg.Scan(t, i, &cfg.DisableConfChangeValidation)
			case "crdb-version":
				var key string
				arg.Scan(t, i, &key)
				version, err := roachpb.ParseVersion(key)
				if err != nil {
					return err
				}
				settings := cluster.MakeTestingClusterSettingsWithVersions(version,
					clusterversion.RemoveDevOffset(clusterversion.MinSupported.Version()),
					true /* initializeVersion */)
				cfg.CRDBVersion = settings.Version
			}
		}
	}
	return env.AddNodes(n, cfg, snap, asyncWrites)
}

type snapOverrideStorage struct {
	Storage
	snapshotOverride func() (pb.Snapshot, error)
}

func (s snapOverrideStorage) Snapshot() (pb.Snapshot, error) {
	if s.snapshotOverride != nil {
		return s.snapshotOverride()
	}
	return s.Storage.Snapshot()
}

var _ raft.Storage = snapOverrideStorage{}

// AddNodes adds n new nodes initialized from the given snapshot (which may be
// empty), and using the cfg as template. They will be assigned consecutive IDs.
func (env *InteractionEnv) AddNodes(
	n int, cfg raft.Config, snap pb.Snapshot, asyncWrites bool,
) error {
	bootstrap := !reflect.DeepEqual(snap, pb.Snapshot{})
	for i := 0; i < n; i++ {
		id := pb.PeerID(1 + len(env.Nodes))
		s := snapOverrideStorage{
			Storage: raft.NewMemoryStorage(),
			// When you ask for a snapshot, you get the most recent snapshot.
			//
			// TODO(tbg): this is sort of clunky, but MemoryStorage itself will
			// give you some fixed snapshot and also the snapshot changes
			// whenever you compact the logs and vice versa, so it's all a bit
			// awkward to use.
			snapshotOverride: func() (pb.Snapshot, error) {
				snaps := env.Nodes[int(id-1)].History
				return snaps[len(snaps)-1], nil
			},
		}
		if bootstrap {
			// NB: we could make this work with 1, but MemoryStorage just
			// doesn't play well with that and it's not a loss of generality.
			if snap.Metadata.Index <= 1 {
				return errors.New("index must be specified as > 1 due to bootstrap")
			}
			snap.Metadata.Term = 1
			if err := s.ApplySnapshot(snap); err != nil {
				return err
			}
			ci := s.Compacted()
			// At the time of writing and for *MemoryStorage, applying a
			// snapshot also truncates appropriately, but this would change with
			// other storage engines potentially.
			if exp := snap.Metadata.Index; ci != exp {
				return fmt.Errorf("failed to establish compacted index %d; got %d", exp, ci)
			}
		}
		cfg := cfg // fork the config stub
		cfg.ID, cfg.Storage = id, s

		// If the node creating command hasn't specified the CRDBVersion, use the
		// latest one.
		if cfg.CRDBVersion == nil {
			cfg.CRDBVersion = cluster.MakeTestingClusterSettings().Version
		}

		cfg.StoreLiveness = newStoreLiveness(env.Fabric, id)

		cfg.Metrics = raft.NewMetrics()

		if env.Options.OnConfig != nil {
			env.Options.OnConfig(&cfg)
			if cfg.ID != id {
				// This could be supported but then we need to do more work
				// translating back and forth -- not worth it.
				return errors.New("OnConfig must not change the ID")
			}
		}
		if cfg.Logger != nil {
			return errors.New("OnConfig must not set Logger")
		}
		cfg.Logger = env.Output

		rn, err := raft.NewRawNode(&cfg)
		if err != nil {
			return err
		}

		node := Node{
			RawNode: rn,
			// TODO(tbg): allow a more general Storage, as long as it also allows
			// us to apply snapshots, append entries, and update the HardState.
			Storage:     s,
			asyncWrites: asyncWrites,
			Config:      &cfg,
			History:     []pb.Snapshot{snap},
		}
		env.Nodes = append(env.Nodes, node)
	}

	// The potential store nodes is the max between the number of nodes in the env
	// and the sum of voters and learners. Add the difference between the
	// potential nodes and the current store nodes.
	allPotential := max(len(env.Nodes),
		len(snap.Metadata.ConfState.Voters)+len(snap.Metadata.ConfState.Learners))
	curNodesCount := len(env.Fabric.state) - 1 // 1-indexed stores
	for rem := allPotential - curNodesCount; rem > 0; rem-- {
		env.Fabric.addNode()
	}
	return nil
}
