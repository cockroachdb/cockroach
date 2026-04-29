// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmasnappb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// fieldDisposition records how one declared field of an mmaprototype-owned
// struct is represented in the proto snapshot graph rooted at
// mmasnappb.ClusterStateSnapshot. Exactly one of ProtoField or OmitReason
// must be set.
//
// ProtoField names a Go field on the corresponding generated proto message
// (looked up via snapshotProtoTargets); the test verifies that field exists.
//
// OmitReason explains why this field is intentionally not snapshotted (e.g.
// it is a runtime injection, a recomputable cache, or transient scratch
// space). Reasons prefixed with "TODO(mma-snapshot)" are placeholders for
// fields that subsequent commits in this arc will promote to a ProtoField.
type fieldDisposition struct {
	ProtoField string
	OmitReason string
}

// snapshotFieldDispositions describes the snapshot disposition of every
// declared field of every mmaprototype-owned struct that is reachable from
// clusterState. Adding a new field to any registered struct fails
// TestSnapshotCoversAllFields until an entry is added here, forcing a
// conscious decision about whether the new state should be snapshotted.
//
// The anonymous storeState.adjusted struct is registered via the
// reflect.Type extracted from a zero storeState; it has no Go-level name.
var snapshotFieldDispositions = map[reflect.Type]map[string]fieldDisposition{
	reflect.TypeOf(clusterState{}): {
		"ts":                      {OmitReason: "runtime clock injection; not state"},
		"nodes":                   {ProtoField: "Nodes"},
		"stores":                  {ProtoField: "Stores"},
		"ranges":                  {ProtoField: "Ranges"},
		"scratchRangeMap":         {OmitReason: "transient workspace"},
		"scratchStoreSet":         {OmitReason: "transient workspace"},
		"scratchMeans":            {OmitReason: "transient workspace"},
		"scratchDisj":             {OmitReason: "transient workspace"},
		"pendingChanges":          {ProtoField: "PendingChanges"},
		"changeSeqGen":            {ProtoField: "ChangeSeqGen"},
		"constraintMatcher":       {OmitReason: "derived index over StoreAttributes (already in StoreSnapshot) and the constraint patterns referenced by snapshotted span configs"},
		"localityTierInterner":    {OmitReason: "dedup table; all interned codes are resolved to plain strings at snapshot time, so no consumer ever needs the table"},
		"meansMemo":               {OmitReason: "recomputable cache (loadInfoProvider back-ref)"},
		"mmaid":                   {ProtoField: "MMAID"},
		"diskUtilRefuseThreshold": {ProtoField: "DiskUtilRefuseThreshold"},
		"diskUtilShedThreshold":   {ProtoField: "DiskUtilShedThreshold"},
		"outerLogEvery":           {OmitReason: "log rate limiter; not allocation state"},
	},
	reflect.TypeOf(nodeState{}): {
		"stores":      {ProtoField: "StoreIDs"},
		"NodeLoad":    {ProtoField: "NodeLoad"},
		"adjustedCPU": {ProtoField: "AdjustedCPU"},
	},
	reflect.TypeOf(NodeLoad{}): {
		"NodeID":          {ProtoField: "NodeID"},
		"NodeCPULoad":     {ProtoField: "NodeCPULoad"},
		"NodeCPUCapacity": {ProtoField: "NodeCPUCapacity"},
	},
	reflect.TypeOf(storeState{}): {
		"status":                                 {ProtoField: "Status"},
		"storeLoad":                              {ProtoField: "StoreLoad"},
		"storeAttributesAndLocalityWithNodeTier": {ProtoField: "StoreAttributes"},
		"adjusted":                               {ProtoField: "Adjusted"},
		"loadSeqNum":                             {ProtoField: "LoadSeqNum"},
		"maxFractionPendingIncrease":             {ProtoField: "MaxFractionPendingIncrease"},
		"maxFractionPendingDecrease":             {ProtoField: "MaxFractionPendingDecrease"},
		"localityTiers":                          {ProtoField: "LocalityTiers"},
		"overloadStartTime":                      {ProtoField: "OverloadStartTime"},
		"overloadEndTime":                        {ProtoField: "OverloadEndTime"},
		"lastDetailedLogTimes":                   {OmitReason: "per-store log rate limiter; not allocation state"},
	},
	reflect.TypeOf(storeLoad{}): {
		"reportedLoad":          {ProtoField: "ReportedLoad"},
		"capacity":              {ProtoField: "Capacity"},
		"reportedSecondaryLoad": {ProtoField: "ReportedSecondaryLoad"},
	},
	reflect.TypeOf(storeAttributesAndLocalityWithNodeTier{}): {
		"StoreID":      {ProtoField: "StoreID"},
		"NodeID":       {ProtoField: "NodeID"},
		"NodeAttrs":    {ProtoField: "NodeAttrs"},
		"NodeLocality": {ProtoField: "NodeLocality"},
		"StoreAttrs":   {ProtoField: "StoreAttrs"},
	},
	storeAdjustedType: {
		"load":               {ProtoField: "Load"},
		"secondaryLoad":      {ProtoField: "SecondaryLoad"},
		"loadPendingChanges": {ProtoField: "LoadPendingChangeIds"},
		"replicas":           {ProtoField: "Replicas"},
		"topKRanges":         {ProtoField: "TopKRanges"},
	},
	reflect.TypeOf(Status{}): {
		"Health":      {ProtoField: "Health"},
		"Disposition": {ProtoField: "Disposition"},
	},
	reflect.TypeOf(Disposition{}): {
		"Lease":   {ProtoField: "Lease"},
		"Replica": {ProtoField: "Replica"},
	},
	reflect.TypeOf(ReplicaState{}): {
		"ReplicaIDAndType": {ProtoField: "ReplicaIDAndType"},
		"LeaseDisposition": {ProtoField: "LeaseDisposition"},
	},
	reflect.TypeOf(ReplicaIDAndType{}): {
		"ReplicaID":   {ProtoField: "ReplicaID"},
		"ReplicaType": {ProtoField: "ReplicaType"},
	},
	reflect.TypeOf(ReplicaType{}): {
		"ReplicaType":   {ProtoField: "ReplicaType"},
		"IsLeaseholder": {ProtoField: "IsLeaseholder"},
	},
	reflect.TypeOf(rangeState{}): {
		"localRangeOwner":                    {ProtoField: "LocalRangeOwner"},
		"replicas":                           {ProtoField: "Replicas"},
		"conf":                               {OmitReason: "TODO(mma-snapshot): normalized span config not yet snapshotted"},
		"hasNormalizationError":              {ProtoField: "HasNormalizationError"},
		"load":                               {ProtoField: "Load"},
		"pendingChanges":                     {ProtoField: "PendingChangeIds"},
		"constraints":                        {OmitReason: "TODO(mma-snapshot): analyzed constraints not yet snapshotted"},
		"lastFailedChange":                   {ProtoField: "LastFailedChange"},
		"diversityIncreaseLastFailedAttempt": {ProtoField: "DiversityIncreaseLastFailedAttempt"},
	},
	reflect.TypeOf(StoreIDAndReplicaState{}): {
		"StoreID":      {ProtoField: "StoreID"},
		"ReplicaState": {ProtoField: "ReplicaState"},
	},
	reflect.TypeOf(RangeLoad{}): {
		"Load":    {ProtoField: "Load"},
		"RaftCPU": {ProtoField: "RaftCPU"},
	},
	reflect.TypeOf(pendingReplicaChange{}): {
		"changeID":      {ProtoField: "ChangeID"},
		"ReplicaChange": {ProtoField: "Change"},
		"startTime":     {ProtoField: "StartTime"},
		"gcTime":        {ProtoField: "GcTime"},
		"enactedAtTime": {ProtoField: "EnactedAtTime"},
	},
	reflect.TypeOf(ReplicaChange{}): {
		"loadDelta":          {ProtoField: "LoadDelta"},
		"secondaryLoadDelta": {ProtoField: "SecondaryLoadDelta"},
		"target":             {ProtoField: "Target"},
		"rangeID":            {ProtoField: "RangeID"},
		"prev":               {ProtoField: "Prev"},
		"next":               {ProtoField: "Next"},
	},
	reflect.TypeOf(localityTiers{}): {
		"tiers": {ProtoField: "LocalityTiers"},
		"str":   {OmitReason: "internal map-key cache (concatenation of stringInterner codes); not meaningful outside MMA"},
	},
	reflect.TypeOf(topKReplicas{}): {
		"k":           {ProtoField: "K"},
		"dim":         {ProtoField: "Dim"},
		"threshold":   {ProtoField: "Threshold"},
		"replicas":    {ProtoField: "Replicas"},
		"replicaHeap": {OmitReason: "transient construction-time heap"},
	},
	reflect.TypeOf(replicaLoad{}): {
		"RangeID": {ProtoField: "RangeID"},
		"load":    {ProtoField: "Load"},
	},
}

// storeAdjustedType is the reflect.Type of the unnamed storeState.adjusted
// struct, which has no Go-level name to reflect.TypeOf directly.
var storeAdjustedType = func() reflect.Type {
	f, ok := reflect.TypeOf(storeState{}).FieldByName("adjusted")
	if !ok {
		panic("mmaprototype: storeState.adjusted not found")
	}
	return f.Type
}()

// snapshotProtoTargets pairs each owned source struct registered in
// snapshotFieldDispositions with the generated proto message that mirrors
// it. It exists so the test can verify that ProtoField names reference real
// fields on the proto side.
//
// For embedded source structs whose fields are flattened into the parent's
// proto message (e.g. ReplicaIDAndType inside ReplicaState), the target is
// the proto for the parent that holds the flattened fields.
var snapshotProtoTargets = map[reflect.Type]reflect.Type{
	reflect.TypeOf(clusterState{}):                           reflect.TypeOf(mmasnappb.ClusterStateSnapshot{}),
	reflect.TypeOf(nodeState{}):                              reflect.TypeOf(mmasnappb.NodeSnapshot{}),
	reflect.TypeOf(NodeLoad{}):                               reflect.TypeOf(mmasnappb.NodeLoad{}),
	reflect.TypeOf(storeState{}):                             reflect.TypeOf(mmasnappb.StoreSnapshot{}),
	reflect.TypeOf(storeLoad{}):                              reflect.TypeOf(mmasnappb.StoreLoad{}),
	reflect.TypeOf(storeAttributesAndLocalityWithNodeTier{}): reflect.TypeOf(mmasnappb.StoreAttributes{}),
	storeAdjustedType:                                        reflect.TypeOf(mmasnappb.StoreAdjusted{}),
	reflect.TypeOf(Status{}):                                 reflect.TypeOf(mmasnappb.Status{}),
	reflect.TypeOf(Disposition{}):                            reflect.TypeOf(mmasnappb.Disposition{}),
	reflect.TypeOf(ReplicaState{}):                           reflect.TypeOf(mmasnappb.ReplicaState{}),
	reflect.TypeOf(ReplicaIDAndType{}):                       reflect.TypeOf(mmasnappb.ReplicaIDAndType{}),
	reflect.TypeOf(ReplicaType{}):                            reflect.TypeOf(mmasnappb.ReplicaType{}),
	reflect.TypeOf(rangeState{}):                             reflect.TypeOf(mmasnappb.RangeSnapshot{}),
	reflect.TypeOf(StoreIDAndReplicaState{}):                 reflect.TypeOf(mmasnappb.StoreIDAndReplicaState{}),
	reflect.TypeOf(RangeLoad{}):                              reflect.TypeOf(mmasnappb.RangeLoad{}),
	reflect.TypeOf(pendingReplicaChange{}):                   reflect.TypeOf(mmasnappb.PendingReplicaChange{}),
	reflect.TypeOf(ReplicaChange{}):                          reflect.TypeOf(mmasnappb.ReplicaChange{}),
	// localityTiers is inlined into the parent StoreSnapshot's repeated
	// locality_tiers field rather than wrapped in its own message; the
	// proto target is therefore the parent.
	reflect.TypeOf(localityTiers{}): reflect.TypeOf(mmasnappb.StoreSnapshot{}),
	reflect.TypeOf(topKReplicas{}):  reflect.TypeOf(mmasnappb.TopKReplicas{}),
	reflect.TypeOf(replicaLoad{}):   reflect.TypeOf(mmasnappb.ReplicaLoad{}),
}

// typeLabel returns a stable label for a registered source type. Anonymous
// struct types (e.g. storeState.adjusted) have an empty Name(); fall back to
// String() in that case.
func typeLabel(t reflect.Type) string {
	if name := t.Name(); name != "" {
		return name
	}
	return t.String()
}

// TestSnapshotDispositionsCoverReachable walks the source-side type graph
// starting at clusterState and visits every type reachable through fields
// that are NOT marked OmitReason. Every owned struct (declared in the
// mmaprototype package) discovered by the walk must be registered in
// snapshotFieldDispositions; otherwise the walk would silently omit its
// fields from coverage.
//
// Combined with TestSnapshotCoversAllFields, this gives the property that
// any new owned struct introduced into the snapshotted graph fails the
// suite until either (a) it is registered with explicit dispositions, or
// (b) the field that pulled it in is marked omitted.
func TestSnapshotDispositionsCoverReachable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ownedPkg := reflect.TypeOf(clusterState{}).PkgPath()
	visited := map[reflect.Type]bool{}
	var unregistered []reflect.Type
	var visit func(t reflect.Type)
	visit = func(t reflect.Type) {
		// Unwrap pointer/slice/array wrappers; recurse through map keys and
		// values.
		for {
			switch t.Kind() {
			case reflect.Ptr, reflect.Slice, reflect.Array:
				t = t.Elem()
				continue
			case reflect.Map:
				visit(t.Key())
				t = t.Elem()
				continue
			}
			break
		}
		if t.Kind() != reflect.Struct {
			return
		}
		// Treat the type as owned if it is registered in the dispositions
		// (covers the unnamed storeState.adjusted struct), or its declared
		// package matches mmaprototype.
		_, registered := snapshotFieldDispositions[t]
		owned := registered || t.PkgPath() == ownedPkg
		if !owned {
			return
		}
		if visited[t] {
			return
		}
		visited[t] = true
		if !registered {
			unregistered = append(unregistered, t)
			return
		}
		for fieldName, d := range snapshotFieldDispositions[t] {
			if d.OmitReason != "" {
				continue
			}
			f, ok := t.FieldByName(fieldName)
			if !ok {
				// TestSnapshotCoversAllFields reports stale entries; nothing
				// to recurse into here.
				continue
			}
			visit(f.Type)
		}
	}
	visit(reflect.TypeOf(clusterState{}))
	for _, ut := range unregistered {
		t.Errorf("type %s is reachable from clusterState through non-omitted "+
			"fields but has no entry in snapshotFieldDispositions; either "+
			"register it or mark the field that pulls it in as omitted",
			typeLabel(ut))
	}
}

// TestSnapshotProtoRoundTrip exercises the converter end-to-end on a small
// hand-built clusterState and confirms the result round-trips through the
// protobuf wire format.
func TestSnapshotProtoRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := timeutil.NewManualTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	cs := newClusterState(ts, newStringInterner())
	cs.diskUtilRefuseThreshold = 0.925
	cs.diskUtilShedThreshold = 0.95
	cs.mmaid = 42

	snap := cs.Snapshot()
	require.NotNil(t, snap)
	require.EqualValues(t, 42, snap.MMAID)
	require.InDelta(t, 0.925, snap.DiskUtilRefuseThreshold, 1e-9)
	require.InDelta(t, 0.95, snap.DiskUtilShedThreshold, 1e-9)

	buf, err := protoutil.Marshal(snap)
	require.NoError(t, err)
	var got mmasnappb.ClusterStateSnapshot
	require.NoError(t, protoutil.Unmarshal(buf, &got))
	require.EqualValues(t, snap.MMAID, got.MMAID)
	require.InDelta(t, snap.DiskUtilRefuseThreshold, got.DiskUtilRefuseThreshold, 1e-9)
}

// TestSnapshotCoversAllFields enforces three invariants on every owned
// struct registered in snapshotFieldDispositions:
//
//  1. Every declared field of the source type has a disposition entry.
//  2. Every disposition entry refers to a field that still exists on the
//     source type (no stale entries after a rename or removal).
//  3. Every ProtoField names a field that exists on the corresponding
//     generated proto message.
//
// If a new field is added to a registered source type without a disposition
// entry, this test fails with a directive to add one — which forces the
// author to think about whether the new field should be snapshotted.
func TestSnapshotCoversAllFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for typ, dispositions := range snapshotFieldDispositions {
		t.Run(typeLabel(typ), func(t *testing.T) {
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				d, ok := dispositions[f.Name]
				if !ok {
					t.Errorf("%s.%s has no snapshot disposition; add an entry to "+
						"snapshotFieldDispositions in cluster_state_snapshot_test.go",
						typeLabel(typ), f.Name)
					continue
				}
				if (d.ProtoField == "") == (d.OmitReason == "") {
					t.Errorf("%s.%s must set exactly one of ProtoField or OmitReason",
						typeLabel(typ), f.Name)
				}
			}
			for name := range dispositions {
				if _, ok := typ.FieldByName(name); !ok {
					t.Errorf("%s.%s is in snapshotFieldDispositions but no longer "+
						"declared on the type; remove the stale entry",
						typeLabel(typ), name)
				}
			}
			// A type with no ProtoField entries (every field carries an
			// OmitReason) is not mirrored by a single proto target. Skip the
			// proto target check in that case; the type is still registered
			// so the reachability walk recognizes it.
			anyProto := false
			for _, d := range dispositions {
				if d.ProtoField != "" {
					anyProto = true
					break
				}
			}
			if !anyProto {
				return
			}
			protoT, ok := snapshotProtoTargets[typ]
			if !ok {
				t.Fatalf("%s has ProtoField dispositions but no entry in snapshotProtoTargets",
					typeLabel(typ))
			}
			for name, d := range dispositions {
				if d.ProtoField == "" {
					continue
				}
				if _, ok := protoT.FieldByName(d.ProtoField); !ok {
					t.Errorf("%s.%s -> %s.%s: proto field not found",
						typeLabel(typ), name, protoT.Name(), d.ProtoField)
				}
			}
		})
	}
}
