// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmasnappb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
		"ranges":                  {OmitReason: "TODO(mma-snapshot): ranges not yet snapshotted"},
		"scratchRangeMap":         {OmitReason: "transient workspace"},
		"scratchStoreSet":         {OmitReason: "transient workspace"},
		"scratchMeans":            {OmitReason: "transient workspace"},
		"scratchDisj":             {OmitReason: "transient workspace"},
		"pendingChanges":          {OmitReason: "TODO(mma-snapshot): pending changes not yet snapshotted"},
		"changeSeqGen":            {ProtoField: "ChangeSeqGen"},
		"constraintMatcher":       {OmitReason: "TODO(mma-snapshot): constraint matcher not yet snapshotted"},
		"localityTierInterner":    {OmitReason: "TODO(mma-snapshot): locality interner not yet snapshotted"},
		"meansMemo":               {OmitReason: "recomputable cache (loadInfoProvider back-ref)"},
		"mmaid":                   {ProtoField: "MMAID"},
		"diskUtilRefuseThreshold": {ProtoField: "DiskUtilRefuseThreshold"},
		"diskUtilShedThreshold":   {ProtoField: "DiskUtilShedThreshold"},
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
		"ReplicaIDAndType": {OmitReason: "embedded; fields flattened into ReplicaState proto via ReplicaIDAndType disposition"},
		"LeaseDisposition": {ProtoField: "LeaseDisposition"},
	},
	reflect.TypeOf(ReplicaIDAndType{}): {
		"ReplicaID":   {ProtoField: "ReplicaID"},
		"ReplicaType": {OmitReason: "embedded; fields flattened into ReplicaState proto via ReplicaType disposition"},
	},
	reflect.TypeOf(ReplicaType{}): {
		"ReplicaType":   {ProtoField: "ReplicaType"},
		"IsLeaseholder": {ProtoField: "IsLeaseholder"},
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
	reflect.TypeOf(ReplicaIDAndType{}):                       reflect.TypeOf(mmasnappb.ReplicaState{}),
	reflect.TypeOf(ReplicaType{}):                            reflect.TypeOf(mmasnappb.ReplicaState{}),
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
			protoT, ok := snapshotProtoTargets[typ]
			if !ok {
				t.Fatalf("%s has dispositions but no entry in snapshotProtoTargets",
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
