// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"gopkg.in/yaml.v2"
)

// Range info used for test data to avoid providing unnecessary fields that are
// not used in replica removal.
type testReplicaInfo struct {
	// Replica location.
	NodeID  roachpb.NodeID  `yaml:"NodeID"`
	StoreID roachpb.StoreID `yaml:"StoreID"`

	// Descriptor as seen by replica.
	RangeID    roachpb.RangeID         `yaml:"RangeID"`
	StartKey   string                  `yaml:"StartKey"`
	EndKey     string                  `yaml:"EndKey"`
	Replicas   []replicaDescriptorView `yaml:"Replicas,flow"`
	Generation roachpb.RangeGeneration `yaml:"Generation,omitempty"`

	// Raft state.
	RangeAppliedIndex         uint64 `yaml:"RangeAppliedIndex"`
	RaftCommittedIndex        uint64 `yaml:"RaftCommittedIndex"`
	HasUncommittedDescriptors bool   `yaml:"HasUncommittedDescriptors"`

	// TODO(oleg): Add ability to have descriptor intents in the store for testing purposes
}

type storeView struct {
	NodeID  roachpb.NodeID  `yaml:"NodeID"`
	StoreID roachpb.StoreID `yaml:"StoreID"`

	Descriptors []storeDescriptorView `yaml:"Descriptors"`
}

// storeDescriptorView contains important fields from the range
// descriptor that tests want to assert against when diffing outputs.
// It is used as a meaningful yaml serializable format since using
// default indented json produces incomprehensible diffs.
type storeDescriptorView struct {
	// Descriptor as seen by replica
	RangeID  roachpb.RangeID         `yaml:"RangeID"`
	StartKey string                  `yaml:"StartKey"`
	Replicas []descriptorViewWrapper `yaml:"Replicas"`
}

// descriptorViewWrapper is a wrapper type to customize representation of
// replicas slice elements in test expectations.
type descriptorViewWrapper struct {
	Replica replicaDescriptorView `yaml:"Replica,flow"`
}

// replicaDescriptorView contains a copy of roachpb.ReplicaDescriptor
// for the purpose of yaml representation and to avoid leaking test
// specific code into production.
type replicaDescriptorView struct {
	NodeID      roachpb.NodeID       `yaml:"NodeID"`
	StoreID     roachpb.StoreID      `yaml:"StoreID"`
	ReplicaID   roachpb.ReplicaID    `yaml:"ReplicaID"`
	ReplicaType *roachpb.ReplicaType `yaml:"ReplicaType,omitempty"`
}

// Store with its owning NodeID for easier grouping by owning nodes.
type wrappedStore struct {
	engine storage.Engine
	nodeID roachpb.NodeID
}

type quorumRecoveryEnv struct {
	// Stores with data
	stores map[roachpb.StoreID]wrappedStore

	// Collected info from nodes
	replicas []loqrecoverypb.NodeReplicaInfo

	// plan to update replicas
	plan loqrecoverypb.ReplicaUpdatePlan
}

func (e *quorumRecoveryEnv) Handle(t *testing.T, d datadriven.TestData) string {
	var err error
	var out string
	switch d.Cmd {
	case "replication-data":
		// Populate in-mem engines with data.
		out = e.handleReplicationData(t, d)
	case "collect-replica-info":
		// Collect one or more range info "files" from stores.
		out, err = e.handleCollectReplicas(t, d)
	case "make-plan":
		// Make plan out of multiple collected replica info.
		out, err = e.handleMakePlan(t, d)
	case "dump-store":
		// Dump the content of the store (all descriptors) for verification.
		out = e.handleDumpStore(t, d)
	case "apply-plan":
		// Create a plan from listed replicas.
		out, err = e.handleApplyPlan(t, d)
	default:
		t.Fatalf("%s: unknown command %s", d.Pos, d.Cmd)
	}
	if err != nil {
		// This is a special case of error. Coverage errors provide properly
		// formatted report as a separate function to better separate processing
		// from presentation.
		details := errors.GetAllDetails(err)
		if len(details) > 0 {
			return fmt.Sprintf("ERROR: %s", strings.Join(details, "\n"))
		}
		return err.Error()
	}
	if len(out) > 0 {
		return out
	}
	return "ok"
}

func (e *quorumRecoveryEnv) handleReplicationData(t *testing.T, d datadriven.TestData) string {
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond*100)

	// Close existing stores in case we have multiple use cases within a data file.
	e.cleanupStores()
	e.stores = make(map[roachpb.StoreID]wrappedStore)

	// Load yaml from data into local range info.
	var replicaData []testReplicaInfo
	err := yaml.UnmarshalStrict([]byte(d.Input), &replicaData)
	if err != nil {
		t.Fatalf("failed to unmarshal test replica data: %v", err)
	}
	for _, replica := range replicaData {
		// If node id is not explicitly supplied, use storeID as sensible shortcut.
		if replica.NodeID == roachpb.NodeID(0) {
			replica.NodeID = roachpb.NodeID(replica.StoreID)
		}

		key, desc, replicaState, hardState := buildReplicaDescriptorFromTestData(t, replica)

		eng := e.getOrCreateStore(ctx, t, replica.StoreID, replica.NodeID)
		if err = storage.MVCCPutProto(ctx, eng, nil, key, clock.Now(), nil, /* txn */
			&desc); err != nil {
			t.Fatalf("failed to write range descriptor into store: %v", err)
		}

		sl := stateloader.Make(replica.RangeID)
		if _, err := sl.Save(ctx, eng, replicaState); err != nil {
			t.Fatalf("failed to save raft replica state into store: %v", err)
		}
		if err := sl.SetHardState(ctx, eng, hardState); err != nil {
			t.Fatalf("failed to save raft hard state: %v", err)
		}
	}
	return "ok"
}

func buildReplicaDescriptorFromTestData(
	t *testing.T, replica testReplicaInfo,
) (roachpb.Key, roachpb.RangeDescriptor, kvserverpb.ReplicaState, raftpb.HardState) {
	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond*100)

	startKey := parsePrettyKey(t, replica.StartKey)
	endKey := parsePrettyKey(t, replica.EndKey)
	key := keys.RangeDescriptorKey(startKey)
	var replicas []roachpb.ReplicaDescriptor
	maxReplicaID := replica.Replicas[0].ReplicaID
	for _, r := range replica.Replicas {
		if r.ReplicaID > maxReplicaID {
			maxReplicaID = r.ReplicaID
		}
		replicas = append(replicas, roachpb.ReplicaDescriptor{
			NodeID:    r.NodeID,
			StoreID:   r.StoreID,
			ReplicaID: r.ReplicaID,
			Type:      r.ReplicaType,
		})
	}
	if replica.Generation == 0 {
		replica.Generation = roachpb.RangeGeneration(maxReplicaID)
	}
	desc := roachpb.RangeDescriptor{
		RangeID:                        replica.RangeID,
		StartKey:                       startKey,
		EndKey:                         endKey,
		InternalReplicas:               replicas,
		NextReplicaID:                  maxReplicaID + 1,
		Generation:                     replica.Generation,
		DeprecatedGenerationComparable: nil,
		StickyBit:                      nil,
	}
	lease := roachpb.Lease{
		Start:           clock.Now().Add(5*time.Minute.Nanoseconds(), 0).UnsafeToClockTimestamp(),
		Expiration:      nil,
		Replica:         desc.InternalReplicas[0],
		ProposedTS:      nil,
		Epoch:           0,
		Sequence:        0,
		AcquisitionType: 0,
	}
	replicaState := kvserverpb.ReplicaState{
		RaftAppliedIndex:  replica.RangeAppliedIndex,
		LeaseAppliedIndex: 0,
		Desc:              &desc,
		Lease:             &lease,
		TruncatedState: &roachpb.RaftTruncatedState{
			Index: 1,
			Term:  1,
		},
		GCThreshold:         &hlc.Timestamp{},
		Version:             nil,
		Stats:               &enginepb.MVCCStats{},
		RaftClosedTimestamp: clock.Now().Add(-30*time.Second.Nanoseconds(), 0),
	}
	hardState := raftpb.HardState{
		Term:   0,
		Vote:   0,
		Commit: replica.RaftCommittedIndex,
	}
	return key, desc, replicaState, hardState
}

func parsePrettyKey(t *testing.T, pretty string) roachpb.RKey {
	scanner := keysutil.MakePrettyScanner(nil /* tableParser */)
	key, err := scanner.Scan(pretty)
	if err != nil {
		t.Fatalf("failed to parse key %s: %v", pretty, err)
	}
	return roachpb.RKey(key)
}

func (e *quorumRecoveryEnv) handleMakePlan(t *testing.T, d datadriven.TestData) (string, error) {
	var err error
	stores := e.parseStoresArg(t, d, false /* defaultToAll */)
	e.plan, _, err = PlanReplicas(context.Background(), e.replicas, stores)
	if err != nil {
		return "", err
	}
	// We only marshal actual data without container to reduce clutter.
	out, err := yaml.Marshal(e.plan.Updates)
	if err != nil {
		t.Fatalf("failed to marshal plan into yaml for verification: %v", err)
	}
	return string(out), nil
}

func (e *quorumRecoveryEnv) getOrCreateStore(
	ctx context.Context, t *testing.T, storeID roachpb.StoreID, nodeID roachpb.NodeID,
) storage.Engine {
	wrapped := e.stores[storeID]
	if wrapped.nodeID == 0 {
		var err error
		eng, err := storage.Open(ctx, storage.InMemory(), storage.CacheSize(1<<20 /* 1 MiB */))
		if err != nil {
			t.Fatalf("failed to crate in mem store: %v", err)
		}
		sIdent := roachpb.StoreIdent{
			ClusterID: uuid.MakeV4(),
			NodeID:    nodeID,
			StoreID:   storeID,
		}
		if err = storage.MVCCPutProto(
			context.Background(), eng, nil, keys.StoreIdentKey(), hlc.Timestamp{}, nil,
			&sIdent); err != nil {
			t.Fatalf("failed to populate test store ident: %v", err)
		}
		wrapped.engine = eng
		wrapped.nodeID = nodeID
		e.stores[storeID] = wrapped
	}
	return wrapped.engine
}

func (e *quorumRecoveryEnv) handleCollectReplicas(
	t *testing.T, d datadriven.TestData,
) (string, error) {
	ctx := context.Background()
	stores := e.parseStoresArg(t, d, true /* defaultToAll */)
	nodes := e.groupStoresByNode(t, stores)
	// save collected results into environment
	e.replicas = nil
	for _, nodeStores := range nodes {
		info, err := CollectReplicaInfo(ctx, nodeStores)
		if err != nil {
			return "", err
		}
		e.replicas = append(e.replicas, info)
	}
	return "ok", nil
}

func (e *quorumRecoveryEnv) groupStoresByNode(
	t *testing.T, storeIDs []roachpb.StoreID,
) map[roachpb.NodeID][]storage.Engine {
	nodes := make(map[roachpb.NodeID][]storage.Engine)
	iterateSelectedStores(t, storeIDs, e.stores,
		func(store storage.Engine, nodeID roachpb.NodeID, storeID roachpb.StoreID) {
			nodes[nodeID] = append(nodes[nodeID], store)
		})
	return nodes
}

func (e *quorumRecoveryEnv) groupStoresByNodeStore(
	t *testing.T, storeIDs []roachpb.StoreID,
) map[roachpb.NodeID]map[roachpb.StoreID]storage.Batch {
	nodes := make(map[roachpb.NodeID]map[roachpb.StoreID]storage.Batch)
	iterateSelectedStores(t, storeIDs, e.stores,
		func(store storage.Engine, nodeID roachpb.NodeID, storeID roachpb.StoreID) {
			nodeStores, ok := nodes[nodeID]
			if !ok {
				nodeStores = make(map[roachpb.StoreID]storage.Batch)
				nodes[nodeID] = nodeStores
			}
			nodeStores[storeID] = store.NewBatch()
		})
	return nodes
}

func iterateSelectedStores(
	t *testing.T,
	storeIDs []roachpb.StoreID,
	stores map[roachpb.StoreID]wrappedStore,
	f func(store storage.Engine, nodeID roachpb.NodeID, storeID roachpb.StoreID),
) {
	for _, id := range storeIDs {
		wrappedStore, ok := stores[id]
		if !ok {
			t.Fatalf("replica info requested from store that was not populated: %d", id)
		}
		f(wrappedStore.engine, wrappedStore.nodeID, id)
	}
}

// parseStoresArg parses StoreIDs from stores arg if available, if no arguments are set
// all available stores are returned.
// Results are returned in sorted order to allow consistent output.
func (e *quorumRecoveryEnv) parseStoresArg(
	t *testing.T, d datadriven.TestData, defaultToAll bool,
) []roachpb.StoreID {
	// Prepare replica info
	var stores []roachpb.StoreID
	if d.HasArg("stores") {
		for _, arg := range d.CmdArgs {
			for _, id := range arg.Vals {
				id, err := strconv.ParseInt(id, 10, 32)
				if err != nil {
					t.Fatalf("failed to parse store id: %v", err)
				}
				stores = append(stores, roachpb.StoreID(id))
			}
		}
	} else {
		if defaultToAll {
			for id := range e.stores {
				stores = append(stores, id)
			}
		} else {
			stores = []roachpb.StoreID{}
		}
	}
	sort.Slice(stores, func(i, j int) bool { return i < j })
	return stores
}

func (e *quorumRecoveryEnv) handleDumpStore(t *testing.T, d datadriven.TestData) string {
	ctx := context.Background()
	stores := e.parseStoresArg(t, d, true /* defaultToAll */)
	var storesView []storeView
	for _, storeID := range stores {
		var descriptorViews []storeDescriptorView
		store := e.stores[storeID]
		err := kvserver.IterateRangeDescriptorsFromDisk(ctx, store.engine,
			func(desc roachpb.RangeDescriptor) error {
				descriptorViews = append(descriptorViews, descriptorView(desc))
				return nil
			})
		if err != nil {
			t.Fatalf("failed to make a dump of store replica data: %v", err)
		}
		storesView = append(storesView, storeView{
			NodeID:      e.stores[storeID].nodeID,
			StoreID:     storeID,
			Descriptors: descriptorViews,
		})
	}
	out, err := yaml.Marshal(storesView)
	if err != nil {
		t.Fatalf("failed to serialize range descriptors from store: %v", err)
	}
	return string(out)
}

func (e *quorumRecoveryEnv) handleApplyPlan(t *testing.T, d datadriven.TestData) (string, error) {
	ctx := context.Background()
	stores := e.parseStoresArg(t, d, true /* defaultToAll */)
	nodes := e.groupStoresByNodeStore(t, stores)
	for nodeID, stores := range nodes {
		_, err := PrepareUpdateReplicas(ctx, e.plan, nodeID, stores)
		if err != nil {
			return "", err
		}
		if _, err := CommitReplicaChanges(stores); err != nil {
			return "", err
		}
	}
	return "ok", nil
}

func (e *quorumRecoveryEnv) cleanupStores() {
	for _, store := range e.stores {
		store.engine.Close()
	}
	e.stores = nil
}

func descriptorView(desc roachpb.RangeDescriptor) storeDescriptorView {
	var replicas []descriptorViewWrapper
	for _, desc := range desc.InternalReplicas {
		replicas = append(replicas, descriptorViewWrapper{replicaDescriptorView{
			NodeID:      desc.NodeID,
			StoreID:     desc.StoreID,
			ReplicaID:   desc.ReplicaID,
			ReplicaType: desc.Type,
		}})
	}
	return storeDescriptorView{
		RangeID:  desc.RangeID,
		StartKey: desc.StartKey.String(),
		Replicas: replicas,
	}
}
