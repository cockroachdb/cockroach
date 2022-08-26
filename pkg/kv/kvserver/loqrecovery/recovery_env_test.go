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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	RangeAppliedIndex  uint64                        `yaml:"RangeAppliedIndex"`
	RaftCommittedIndex uint64                        `yaml:"RaftCommittedIndex"`
	DescriptorUpdates  []testReplicaDescriptorChange `yaml:"DescriptorUpdates,flow,omitempty"`

	// TODO(oleg): Add ability to have descriptor intents in the store for testing purposes
}

// Raft log descriptor changes.
type testReplicaDescriptorChange struct {
	// Change type determines which fields has to be set here. While this could be
	// error-prone, we use this for the sake of test spec brevity. Otherwise
	// we will need nested structures which would introduce clutter.
	ChangeType loqrecoverypb.DescriptorChangeType `yaml:"Type"`
	// Replicas are used to define descriptor change (Type = 2).
	Replicas []replicaDescriptorView `yaml:"Replicas,flow,omitempty"`
	// RangeID, StartKey and EndKey define right-hand side of split and merge
	// operations (Type = 0 or Type = 1).
	RangeID  roachpb.RangeID `yaml:"RangeID,omitempty"`
	StartKey string          `yaml:"StartKey,omitempty"`
	EndKey   string          `yaml:"EndKey,omitempty"`
}

type storeView struct {
	NodeID  roachpb.NodeID  `yaml:"NodeID"`
	StoreID roachpb.StoreID `yaml:"StoreID"`

	Descriptors []storeDescriptorView `yaml:"Descriptors"`
	LocalData   []localDataView       `yaml:"LocalData"`
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
	NodeID      roachpb.NodeID      `yaml:"NodeID"`
	StoreID     roachpb.StoreID     `yaml:"StoreID"`
	ReplicaID   roachpb.ReplicaID   `yaml:"ReplicaID"`
	ReplicaType roachpb.ReplicaType `yaml:"ReplicaType,omitempty"`
}

func (r replicaDescriptorView) asReplicaDescriptor() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{
		NodeID:    r.NodeID,
		StoreID:   r.StoreID,
		ReplicaID: r.ReplicaID,
		Type:      r.ReplicaType,
	}
}

// localDataView contains interesting local store data for each range.
type localDataView struct {
	RangeID       roachpb.RangeID `yaml:"RangeID"`
	RaftReplicaID int             `yaml:"RaftReplicaID"`
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
		// Make plan out of specified replica info "files".
		out, err = e.handleMakePlan(t, d)
	case "dump-store":
		// Dump the content of the store (all descriptors) for verification.
		out = e.handleDumpStore(t, d)
	case "apply-plan":
		out, err = e.handleApplyPlan(t, d)
	case "dump-events":
		out, err = e.dumpRecoveryEvents(t, d)
	default:
		t.Fatalf("%s: unknown command %s", d.Pos, d.Cmd)
	}
	if err != nil {
		// This is a special case of error. Coverage errors provide properly
		// formatted report as a separate function to better separate processing
		// from presentation.
		details := errors.GetAllDetails(err)
		if len(details) > 0 {
			return fmt.Sprintf("ERROR: %s\n%s", err.Error(), strings.Join(details, "\n"))
		}
		return fmt.Sprintf("ERROR: %s", err.Error())
	}
	if len(out) > 0 {
		return out
	}
	return "ok"
}

func (e *quorumRecoveryEnv) handleReplicationData(t *testing.T, d datadriven.TestData) string {
	ctx := context.Background()
	clock := hlc.NewClockWithSystemTimeSource(time.Millisecond * 100 /* maxOffset */)

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

		replicaID, key, desc, replicaState, hardState, raftLog :=
			buildReplicaDescriptorFromTestData(t, replica)

		eng := e.getOrCreateStore(ctx, t, replica.StoreID, replica.NodeID)
		if err = storage.MVCCPutProto(
			ctx, eng, nil, key, clock.Now(), hlc.ClockTimestamp{}, nil /* txn */, &desc,
		); err != nil {
			t.Fatalf("failed to write range descriptor into store: %v", err)
		}

		sl := stateloader.Make(replica.RangeID)
		if _, err := sl.Save(ctx, eng, replicaState, true /* gcHintsEnabled */); err != nil {
			t.Fatalf("failed to save raft replica state into store: %v", err)
		}
		if err := sl.SetHardState(ctx, eng, hardState); err != nil {
			t.Fatalf("failed to save raft hard state: %v", err)
		}
		if err := sl.SetRaftReplicaID(ctx, eng, replicaID); err != nil {
			t.Fatalf("failed to set raft replica ID: %v", err)
		}
		for i, entry := range raftLog {
			value, err := protoutil.Marshal(&entry)
			if err != nil {
				t.Fatalf("failed to serialize metadata entry for raft log")
			}
			if err := eng.PutUnversioned(keys.RaftLogKey(replica.RangeID,
				uint64(i)+hardState.Commit+1), value); err != nil {
				t.Fatalf("failed to insert raft log entry into store: %s", err)
			}
		}
	}
	return "ok"
}

func buildReplicaDescriptorFromTestData(
	t *testing.T, replica testReplicaInfo,
) (
	roachpb.ReplicaID,
	roachpb.Key,
	roachpb.RangeDescriptor,
	kvserverpb.ReplicaState,
	raftpb.HardState,
	[]enginepb.MVCCMetadata,
) {
	clock := hlc.NewClockWithSystemTimeSource(time.Millisecond * 100 /* maxOffset */)

	startKey := parsePrettyKey(t, replica.StartKey)
	endKey := parsePrettyKey(t, replica.EndKey)
	key := keys.RangeDescriptorKey(startKey)
	var replicas []roachpb.ReplicaDescriptor
	var replicaID roachpb.ReplicaID
	maxReplicaID := replica.Replicas[0].ReplicaID
	for _, r := range replica.Replicas {
		if r.ReplicaID > maxReplicaID {
			maxReplicaID = r.ReplicaID
		}
		replicas = append(replicas, r.asReplicaDescriptor())
		if r.NodeID == replica.NodeID && r.StoreID == replica.StoreID {
			replicaID = r.ReplicaID
		}
	}
	if replica.Generation == 0 {
		replica.Generation = roachpb.RangeGeneration(maxReplicaID)
	}
	desc := roachpb.RangeDescriptor{
		RangeID:          replica.RangeID,
		StartKey:         startKey,
		EndKey:           endKey,
		InternalReplicas: replicas,
		NextReplicaID:    maxReplicaID + 1,
		Generation:       replica.Generation,
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
		GCHint:              &roachpb.GCHint{},
		Version:             nil,
		Stats:               &enginepb.MVCCStats{},
		RaftClosedTimestamp: clock.Now().Add(-30*time.Second.Nanoseconds(), 0),
	}
	hardState := raftpb.HardState{
		Term:   0,
		Vote:   0,
		Commit: replica.RaftCommittedIndex,
	}
	var raftLog []enginepb.MVCCMetadata
	for i, u := range replica.DescriptorUpdates {
		entry := raftLogFromPendingDescriptorUpdate(t, replica, u, desc, uint64(i))
		raftLog = append(raftLog, enginepb.MVCCMetadata{RawBytes: entry.RawBytes})
	}
	return replicaID, key, desc, replicaState, hardState, raftLog
}

func raftLogFromPendingDescriptorUpdate(
	t *testing.T,
	replica testReplicaInfo,
	update testReplicaDescriptorChange,
	desc roachpb.RangeDescriptor,
	entryIndex uint64,
) roachpb.Value {
	// We mimic EndTxn messages with commit triggers here. We don't construct
	// full batches with descriptor updates as we only need data that would be
	// used by collect stage. Actual data extraction from raft log is tested
	// elsewhere using test cluster.
	r := kvserverpb.ReplicatedEvalResult{}
	switch update.ChangeType {
	case loqrecoverypb.DescriptorChangeType_Split:
		r.Split = &kvserverpb.Split{
			SplitTrigger: roachpb.SplitTrigger{
				RightDesc: roachpb.RangeDescriptor{
					RangeID:  update.RangeID,
					StartKey: roachpb.RKey(update.StartKey),
					EndKey:   roachpb.RKey(update.EndKey),
				},
			},
		}
	case loqrecoverypb.DescriptorChangeType_Merge:
		r.Merge = &kvserverpb.Merge{
			MergeTrigger: roachpb.MergeTrigger{
				RightDesc: roachpb.RangeDescriptor{
					RangeID:  update.RangeID,
					StartKey: roachpb.RKey(update.StartKey),
					EndKey:   roachpb.RKey(update.EndKey),
				},
			},
		}
	case loqrecoverypb.DescriptorChangeType_ReplicaChange:
		var newReplicas []roachpb.ReplicaDescriptor
		for _, r := range update.Replicas {
			newReplicas = append(newReplicas, r.asReplicaDescriptor())
		}
		r.ChangeReplicas = &kvserverpb.ChangeReplicas{
			ChangeReplicasTrigger: roachpb.ChangeReplicasTrigger{
				Desc: &roachpb.RangeDescriptor{
					RangeID:          desc.RangeID,
					InternalReplicas: newReplicas,
				},
			},
		}
	}
	raftCmd := kvserverpb.RaftCommand{ReplicatedEvalResult: r}
	out, err := protoutil.Marshal(&raftCmd)
	if err != nil {
		t.Fatalf("failed to serialize raftCommand: %v", err)
	}
	data := kvserverbase.EncodeTestRaftCommand(
		out, kvserverbase.CmdIDKey(fmt.Sprintf("%08d", entryIndex)))
	ent := raftpb.Entry{
		Term:  1,
		Index: replica.RaftCommittedIndex + entryIndex,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
	var value roachpb.Value
	if err := value.SetProto(&ent); err != nil {
		t.Fatalf("can't construct raft entry from test data: %s", err)
	}
	return value
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
	stores := e.parseStoresArg(t, d, false /* defaultToAll */)
	plan, report, err := PlanReplicas(context.Background(), e.replicas, stores)
	if err != nil {
		return "", err
	}
	err = report.Error()
	var force bool
	if d.HasArg("force") {
		d.ScanArgs(t, "force", &force)
	}
	if err != nil && !force {
		return "", err
	}
	e.plan = plan
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
			context.Background(), eng, nil, keys.StoreIdentKey(), hlc.Timestamp{}, hlc.ClockTimestamp{}, nil, &sIdent,
		); err != nil {
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
			if arg.Key == "stores" {
				for _, id := range arg.Vals {
					id, err := strconv.ParseInt(id, 10, 32)
					if err != nil {
						t.Fatalf("failed to parse store id: %v", err)
					}
					stores = append(stores, roachpb.StoreID(id))
				}
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
		var localDataViews []localDataView
		store := e.stores[storeID]
		err := kvserver.IterateRangeDescriptorsFromDisk(ctx, store.engine,
			func(desc roachpb.RangeDescriptor) error {
				descriptorViews = append(descriptorViews, descriptorView(desc))

				sl := stateloader.Make(desc.RangeID)
				raftReplicaID, _, err := sl.LoadRaftReplicaID(ctx, store.engine)
				if err != nil {
					t.Fatalf("failed to load Raft replica ID: %v", err)
				}
				localDataViews = append(localDataViews, localDataView{
					RangeID:       desc.RangeID,
					RaftReplicaID: int(raftReplicaID.ReplicaID),
				})
				return nil
			})
		if err != nil {
			t.Fatalf("failed to make a dump of store replica data: %v", err)
		}
		storesView = append(storesView, storeView{
			NodeID:      e.stores[storeID].nodeID,
			StoreID:     storeID,
			Descriptors: descriptorViews,
			LocalData:   localDataViews,
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
	updateTime := timeutil.Now()
	for nodeID, stores := range nodes {
		_, err := PrepareUpdateReplicas(ctx, e.plan, uuid.DefaultGenerator, updateTime, nodeID, stores)
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

func (e *quorumRecoveryEnv) dumpRecoveryEvents(
	t *testing.T, d datadriven.TestData,
) (string, error) {
	ctx := context.Background()

	removeEvents := false
	if d.HasArg("remove") {
		d.ScanArgs(t, "remove", &removeEvents)
	}

	var events []string
	logEvents := func(ctx context.Context, record loqrecoverypb.ReplicaRecoveryRecord) (bool, error) {
		event := record.AsStructuredLog()
		events = append(events, fmt.Sprintf("Updated range r%d, Key:%s, Store:s%d ReplicaID:%d",
			event.RangeID, event.StartKey, event.StoreID, event.UpdatedReplicaID))
		return removeEvents, nil
	}

	stores := e.parseStoresArg(t, d, true /* defaultToAll */)
	for _, store := range stores {
		if _, err := RegisterOfflineRecoveryEvents(ctx, e.stores[store].engine, logEvents); err != nil {
			return "", err
		}
	}
	return strings.Join(events, "\n"), nil
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
