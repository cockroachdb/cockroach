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
var snapshotFieldDispositions = map[reflect.Type]map[string]fieldDisposition{
	reflect.TypeOf(clusterState{}): {
		"ts":                      {OmitReason: "runtime clock injection; not state"},
		"nodes":                   {OmitReason: "TODO(mma-snapshot): nodes not yet snapshotted"},
		"stores":                  {OmitReason: "TODO(mma-snapshot): stores not yet snapshotted"},
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
}

// snapshotProtoTargets pairs each owned source struct registered in
// snapshotFieldDispositions with the generated proto message that mirrors
// it. It exists so the test can verify that ProtoField names reference real
// fields on the proto side.
var snapshotProtoTargets = map[reflect.Type]reflect.Type{
	reflect.TypeOf(clusterState{}): reflect.TypeOf(mmasnappb.ClusterStateSnapshot{}),
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
		t.Run(typ.Name(), func(t *testing.T) {
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				d, ok := dispositions[f.Name]
				if !ok {
					t.Errorf("%s.%s has no snapshot disposition; add an entry to "+
						"snapshotFieldDispositions in cluster_state_snapshot_test.go",
						typ.Name(), f.Name)
					continue
				}
				if (d.ProtoField == "") == (d.OmitReason == "") {
					t.Errorf("%s.%s must set exactly one of ProtoField or OmitReason",
						typ.Name(), f.Name)
				}
			}
			for name := range dispositions {
				if _, ok := typ.FieldByName(name); !ok {
					t.Errorf("%s.%s is in snapshotFieldDispositions but no longer "+
						"declared on the type; remove the stale entry",
						typ.Name(), name)
				}
			}
			protoT, ok := snapshotProtoTargets[typ]
			if !ok {
				t.Fatalf("%s has dispositions but no entry in snapshotProtoTargets",
					typ.Name())
			}
			for name, d := range dispositions {
				if d.ProtoField == "" {
					continue
				}
				if _, ok := protoT.FieldByName(d.ProtoField); !ok {
					t.Errorf("%s.%s -> %s.%s: proto field not found",
						typ.Name(), name, protoT.Name(), d.ProtoField)
				}
			}
		})
	}
}
