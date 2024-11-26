// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/strutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"gopkg.in/yaml.v2"
)

/*
quorumRecoveryEnv provides an environment to run data driven tests for loss of
quorum recovery. Provided commands cover replica info collection, change
planning, plan application and checking or generated recovery reports.
It doesn't cover plan and replica info serialization and actual cli commands and
interaction with user.

Supported commands:
replication-data

  Loads following data into stores. All previously existing data is wiped. Only
  range local information about replicas in populated together with optional
  raft log entries. Data is created using proxy structs to avoid the need to
  populate unnecessary fields.

descriptor-data

  Initializes optional "meta range" info with provided descriptor data.

collect-replica-info [stores=(1,2,3)]

  Collects replica info and saves into env. stores argument provides ids of
  stores test wants to analyze. This allows test not to collect all replicas if
  needed. If omitted, then all stores are collected.

make-plan [stores=(1,2,3)|nodes=(1,2,3)] [force=<bool>]

  Creates a recovery plan based on replica info saved to env by
  collect-replica-info command and optional descriptors. Stores or nodes args
  provide optional list of dead nodes or dead stores which reflect cli command
  line flags. force flag forces plan creation in presence of range
  inconsistencies.
  Resulting plan is saved into the environment.

dump-store [stores=(1,2,3)]

  Prints content of the replica states in the store. If no stores are provided,
  uses all populated stores.

apply-plan [stores=(1,2,3)] [restart=<bool>]

  Applies recovery plan on specified stores. If no stores are provided, uses all
  populated stores. If restart = false, simulates cli version of command where
  application could fail and roll back with an error. If restart = true,
  simulates half online approach where unattended operation succeeds but writes
  potential error into store.

dump-events [remove=<bool>] [status=<bool>]

  Dumps events about replica recovery found in stores.
  If remove=true then corresponding method is told to remove events after
  dumping in a way range log population does when consuming those events.
  If status=true, half-online approach recovery report is dumped for each store.
*/

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
	RangeAppliedIndex  kvpb.RaftIndex                `yaml:"RangeAppliedIndex"`
	RaftCommittedIndex kvpb.RaftIndex                `yaml:"RaftCommittedIndex"`
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
	Leaseholder bool                `yaml:"Leaseholder,omitempty"`
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

// testDescriptorData yaml optimized representation of RangeDescriptor
type testDescriptorData struct {
	RangeID  roachpb.RangeID `yaml:"RangeID"`
	StartKey string          `yaml:"StartKey"`
	// EndKey is optional, it will be filled up with next descriptor's StartKey
	// if omitted
	EndKey     string                  `yaml:"EndKey"`
	Replicas   []replicaDescriptorView `yaml:"Replicas,flow"`
	Generation roachpb.RangeGeneration `yaml:"Generation,omitempty"`
}

type seqUUIDGen struct {
	seq uint32
}

func NewSeqGen(val uint32) uuid.Generator {
	return &seqUUIDGen{seq: val}
}

func (g *seqUUIDGen) NewV1() (uuid.UUID, error) {
	nextV1 := g.next()
	nextV1.SetVersion(uuid.V1)
	return nextV1, nil
}

func (*seqUUIDGen) NewV3(uuid.UUID, string) uuid.UUID {
	panic("not implemented")
}

func (g *seqUUIDGen) NewV4() uuid.UUID {
	nextV4 := g.next()
	nextV4.SetVersion(uuid.V4)
	return nextV4
}

func (*seqUUIDGen) NewV5(uuid.UUID, string) uuid.UUID {
	panic("not implemented")
}

func (g *seqUUIDGen) next() uuid.UUID {
	next := uuid.UUID{}
	binary.BigEndian.PutUint32(next[:], g.seq)
	next.SetVariant(uuid.VariantRFC4122)
	g.seq++
	return next
}

// Store with its owning NodeID for easier grouping by owning nodes.
type wrappedStore struct {
	engine storage.Engine
	nodeID roachpb.NodeID
}

type quorumRecoveryEnv struct {
	// Stores with data.
	clusterID uuid.UUID
	stores    map[roachpb.StoreID]wrappedStore
	// Optional mata ranges content.
	meta []roachpb.RangeDescriptor

	// Collected info from nodes.
	replicas loqrecoverypb.ClusterReplicaInfo

	// plan to update replicas.
	plan loqrecoverypb.ReplicaUpdatePlan

	// Helper resources to make time and identifiers predictable.
	uuidGen uuid.Generator
	clock   timeutil.TimeSource
}

func (e *quorumRecoveryEnv) Handle(t *testing.T, d datadriven.TestData) string {
	var err error
	var out string
	switch d.Cmd {
	case "replication-data":
		// Populate in-mem engines with data.
		out = e.handleReplicationData(t, d)
	case "descriptor-data":
		// Populate optional descriptor data.
		out = e.handleDescriptorData(t, d)
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
	clock := hlc.NewClockForTesting(nil)

	// Close existing stores in case we have multiple use cases within a data file.
	e.cleanupStores()
	e.stores = make(map[roachpb.StoreID]wrappedStore)
	e.clusterID = uuid.MakeV4()

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

		replicaID, key, desc, replicaState, truncState, hardState, raftLog :=
			buildReplicaDescriptorFromTestData(t, replica)

		eng := e.getOrCreateStore(ctx, t, replica.StoreID, replica.NodeID)
		if err = storage.MVCCPutProto(
			ctx, eng, key, clock.Now(), &desc, storage.MVCCWriteOptions{},
		); err != nil {
			t.Fatalf("failed to write range descriptor into store: %v", err)
		}

		sl := stateloader.Make(replica.RangeID)
		if _, err := sl.Save(ctx, eng, replicaState); err != nil {
			t.Fatalf("failed to save raft replica state into store: %v", err)
		}
		if err := sl.SetRaftTruncatedState(ctx, eng, &truncState); err != nil {
			t.Fatalf("failed to save raft truncated state: %v", err)
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
				kvpb.RaftIndex(uint64(i)+hardState.Commit+1)), value); err != nil {
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
	kvserverpb.RaftTruncatedState,
	raftpb.HardState,
	[]enginepb.MVCCMetadata,
) {
	clock := hlc.NewClockForTesting(nil)

	startKey := parsePrettyKey(t, replica.StartKey)
	endKey := parsePrettyKey(t, replica.EndKey)
	key := keys.RangeDescriptorKey(startKey)
	var replicas []roachpb.ReplicaDescriptor
	var replicaID roachpb.ReplicaID
	maxReplicaID := replica.Replicas[0].ReplicaID
	var lhIndex int
	for i, r := range replica.Replicas {
		if r.ReplicaID > maxReplicaID {
			maxReplicaID = r.ReplicaID
		}
		replicas = append(replicas, r.asReplicaDescriptor())
		if r.NodeID == replica.NodeID && r.StoreID == replica.StoreID {
			replicaID = r.ReplicaID
		}
		if r.Leaseholder {
			lhIndex = i
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
		Replica:         desc.InternalReplicas[lhIndex],
		ProposedTS:      hlc.ClockTimestamp{},
		Epoch:           0,
		Sequence:        0,
		AcquisitionType: 0,
	}
	replicaState := kvserverpb.ReplicaState{
		RaftAppliedIndex:    replica.RangeAppliedIndex,
		LeaseAppliedIndex:   0,
		Desc:                &desc,
		Lease:               &lease,
		GCThreshold:         &hlc.Timestamp{},
		GCHint:              &roachpb.GCHint{},
		Version:             nil,
		Stats:               &enginepb.MVCCStats{},
		RaftClosedTimestamp: clock.Now().Add(-30*time.Second.Nanoseconds(), 0),
	}
	truncState := kvserverpb.RaftTruncatedState{
		Index: 1,
		Term:  1,
	}
	hardState := raftpb.HardState{
		Term:   0,
		Vote:   0,
		Commit: uint64(replica.RaftCommittedIndex),
	}
	var raftLog []enginepb.MVCCMetadata
	for i, u := range replica.DescriptorUpdates {
		entry := raftLogFromPendingDescriptorUpdate(t, replica, u, desc, kvpb.RaftIndex(i))
		raftLog = append(raftLog, enginepb.MVCCMetadata{RawBytes: entry.RawBytes})
	}
	return replicaID, key, desc, replicaState, truncState, hardState, raftLog
}

func raftLogFromPendingDescriptorUpdate(
	t *testing.T,
	replica testReplicaInfo,
	update testReplicaDescriptorChange,
	desc roachpb.RangeDescriptor,
	entryIndex kvpb.RaftIndex,
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
	data := raftlog.EncodeCommandBytes(
		raftlog.EntryEncodingStandardWithoutAC,
		kvserverbase.CmdIDKey(fmt.Sprintf("%08d", entryIndex)), out, 0 /* pri */)
	ent := raftpb.Entry{
		Term:  1,
		Index: uint64(replica.RaftCommittedIndex + entryIndex),
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
	scanner := keysutil.MakePrettyScanner(nil /* tableParser */, nil /* tenantParser */)
	key, err := scanner.Scan(pretty)
	if err != nil {
		t.Fatalf("failed to parse key %s: %v", pretty, err)
	}
	return roachpb.RKey(key)
}

func (e *quorumRecoveryEnv) handleDescriptorData(t *testing.T, d datadriven.TestData) string {
	var descriptors []testDescriptorData
	err := yaml.UnmarshalStrict([]byte(d.Input), &descriptors)
	if err != nil {
		t.Fatalf("failed to unmarshal test range descriptor data: %v", err)
	}
	e.meta = nil
	if len(descriptors) == 0 {
		return "ok"
	}

	var testDesc testDescriptorData

	makeLastDescriptor := func(nextStartKey string) roachpb.RangeDescriptor {
		var replicas []roachpb.ReplicaDescriptor
		var maxReplicaID roachpb.ReplicaID
		for _, r := range testDesc.Replicas {
			replicas = append(replicas, r.asReplicaDescriptor())
			if r.ReplicaID > maxReplicaID {
				maxReplicaID = r.ReplicaID
			}
		}
		gen := testDesc.Generation
		if gen == 0 {
			gen = roachpb.RangeGeneration(maxReplicaID)
		}
		endKeyStr := testDesc.EndKey
		if endKeyStr == "" {
			endKeyStr = nextStartKey
		}
		return roachpb.RangeDescriptor{
			RangeID:          testDesc.RangeID,
			StartKey:         parsePrettyKey(t, testDesc.StartKey),
			EndKey:           parsePrettyKey(t, endKeyStr),
			InternalReplicas: replicas,
			Generation:       gen,
			NextReplicaID:    maxReplicaID + 1,
		}
	}

	for _, d := range descriptors {
		if testDesc.RangeID != 0 {
			e.meta = append(e.meta, makeLastDescriptor(d.StartKey))
		}
		if d.RangeID == 0 {
			t.Fatal("RangeID in the test range descriptor can't be 0")
		}
		testDesc = d
	}
	e.meta = append(e.meta, makeLastDescriptor("/Max"))
	return "ok"
}

func (e *quorumRecoveryEnv) handleMakePlan(t *testing.T, d datadriven.TestData) (string, error) {
	stores := e.parseStoresArg(t, d, false /* defaultToAll */)
	nodes := e.parseNodesArg(t, d)
	plan, report, err := PlanReplicas(context.Background(), e.replicas, stores, nodes, e.uuidGen)
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
	if len(e.plan.Updates) == 0 {
		return "", nil
	}
	// We only marshal actual data without container to reduce clutter.
	out, err := yaml.Marshal(e.plan.Updates)
	if err != nil {
		t.Fatalf("failed to marshal plan into yaml for verification: %v", err)
	}
	var b strings.Builder
	b.WriteString("Replica updates:\n")
	b.WriteString(string(out))
	if len(e.plan.DecommissionedNodeIDs) > 0 {
		b.WriteString(fmt.Sprintf("Decommissioned nodes:\n[%s]\n",
			strutil.JoinIDs("n", e.plan.DecommissionedNodeIDs)))
	}
	if len(e.plan.StaleLeaseholderNodeIDs) > 0 {
		b.WriteString(fmt.Sprintf("Nodes to restart:\n[%s]\n",
			strutil.JoinIDs("n", e.plan.StaleLeaseholderNodeIDs)))
	}
	return b.String(), nil
}

func (e *quorumRecoveryEnv) getOrCreateStore(
	ctx context.Context, t *testing.T, storeID roachpb.StoreID, nodeID roachpb.NodeID,
) storage.Engine {
	wrapped := e.stores[storeID]
	if wrapped.nodeID == 0 {
		var err error
		eng, err := storage.Open(ctx,
			storage.InMemory(),
			cluster.MakeClusterSettings(),
			storage.CacheSize(1<<20 /* 1 MiB */))
		if err != nil {
			t.Fatalf("failed to crate in mem store: %v", err)
		}
		sIdent := roachpb.StoreIdent{
			ClusterID: e.clusterID,
			NodeID:    nodeID,
			StoreID:   storeID,
		}
		if err = storage.MVCCPutProto(
			context.Background(), eng, keys.StoreIdentKey(), hlc.Timestamp{}, &sIdent, storage.MVCCWriteOptions{},
		); err != nil {
			t.Fatalf("failed to populate test store ident: %v", err)
		}
		v := clusterversion.Latest.Version()
		if err := kvstorage.WriteClusterVersionToEngines(ctx, []storage.Engine{eng}, clusterversion.ClusterVersion{Version: v}); err != nil {
			t.Fatalf("failed to populate test store cluster version: %v", err)
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
	e.replicas = loqrecoverypb.ClusterReplicaInfo{}
	for _, nodeStores := range nodes {
		info, _, err := CollectStoresReplicaInfo(ctx, nodeStores)
		if err != nil {
			return "", err
		}
		if err = e.replicas.Merge(info); err != nil {
			return "", err
		}
	}
	if len(e.replicas.LocalInfo) == 0 {
		// This is unrealistic as we don't have metadata. We need to fake it here
		// to pass planner checks.
		e.replicas.ClusterID = e.clusterID.String()
		e.replicas.Version = clusterversion.Latest.Version()
	}
	e.replicas.Descriptors = e.meta
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
			t.Fatalf("attempt to get store that was not populated: %d", id)
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
	slices.Sort(stores)
	return stores
}

// parseNodesArg parses NodeIDs from nodes arg if available.
// Results are returned in sorted order to allow consistent output.
func (e *quorumRecoveryEnv) parseNodesArg(t *testing.T, d datadriven.TestData) []roachpb.NodeID {
	var nodes []roachpb.NodeID
	if d.HasArg("nodes") {
		for _, arg := range d.CmdArgs {
			if arg.Key == "nodes" {
				for _, id := range arg.Vals {
					id, err := strconv.ParseInt(id, 10, 32)
					if err != nil {
						t.Fatalf("failed to parse node id: %v", err)
					}
					nodes = append(nodes, roachpb.NodeID(id))
				}
			}
		}
	}
	if len(nodes) > 0 {
		slices.Sort(nodes)
	}
	return nodes
}

func (e *quorumRecoveryEnv) handleDumpStore(t *testing.T, d datadriven.TestData) string {
	ctx := context.Background()
	stores := e.parseStoresArg(t, d, true /* defaultToAll */)
	var storesView []storeView
	for _, storeID := range stores {
		var descriptorViews []storeDescriptorView
		var localDataViews []localDataView
		store := e.stores[storeID]
		err := kvstorage.IterateRangeDescriptorsFromDisk(ctx, store.engine,
			func(desc roachpb.RangeDescriptor) error {
				descriptorViews = append(descriptorViews, descriptorView(desc))

				sl := stateloader.Make(desc.RangeID)
				raftReplicaID, err := sl.LoadRaftReplicaID(ctx, store.engine)
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
	var restart bool
	if d.HasArg("restart") {
		d.ScanArgs(t, "restart", &restart)
	}

	if !restart {
		nodes := e.groupStoresByNodeStore(t, stores)
		defer func() {
			for _, storeBatches := range nodes {
				for _, b := range storeBatches {
					b.Close()
				}
			}
		}()
		updateTime := timeutil.Now()
		for nodeID, stores := range nodes {
			_, err := PrepareUpdateReplicas(ctx, e.plan, e.uuidGen, updateTime, nodeID,
				stores)
			if err != nil {
				return "", err
			}
			if _, err = CommitReplicaChanges(stores); err != nil {
				return "", err
			}
		}
		return "ok", nil
	}

	nodes := e.groupStoresByNode(t, stores)
	for _, stores := range nodes {
		ps := PlanStore{
			path: "",
			fs:   vfs.NewMem(),
		}
		if err := ps.SavePlan(e.plan); err != nil {
			t.Fatal("failed to save plan to plan store", err)
		}

		err := MaybeApplyPendingRecoveryPlan(ctx, ps, stores, e.clock)
		if err != nil {
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
	dumpStatus := false
	if d.HasArg("status") {
		d.ScanArgs(t, "status", &dumpStatus)
	}

	var events []string
	logEvents := func(ctx context.Context, record loqrecoverypb.ReplicaRecoveryRecord) (bool, error) {
		event := record.AsStructuredLog()
		events = append(events, fmt.Sprintf("updated range r%d, Key:%s, Store:s%d ReplicaID:%d",
			event.RangeID, event.StartKey, event.StoreID, event.UpdatedReplicaID))
		return removeEvents, nil
	}

	var statuses []string
	stores := e.parseStoresArg(t, d, true /* defaultToAll */)
	for _, storeID := range stores {
		store, ok := e.stores[storeID]
		if !ok {
			t.Fatalf("store s%d doesn't exist, but event dump is requested for it", store)
		}
		if _, err := RegisterOfflineRecoveryEvents(ctx, store.engine, logEvents); err != nil {
			return "", err
		}
		status, ok, err := readNodeRecoveryStatusInfo(ctx, store.engine)
		if err != nil {
			return "", err
		}
		if ok {
			statuses = append(statuses,
				fmt.Sprintf("node n%d applied plan %s at %s:%s", store.nodeID, status.AppliedPlanID, status.ApplyTimestamp,
					status.Error))
		}
	}

	var sb strings.Builder
	if len(events) > 0 {
		sb.WriteString(fmt.Sprintf("Events:\n%s\n", strings.Join(events, "\n")))
	}
	if dumpStatus && len(statuses) > 0 {
		sb.WriteString(fmt.Sprintf("Statuses:\n%s", strings.Join(statuses, "\n")))
	}
	if sb.Len() > 0 {
		return sb.String(), nil
	}
	return "ok", nil
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
