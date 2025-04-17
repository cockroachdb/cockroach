// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestObsoleteCode contains nudges for cleanups that may be possible in the
// future. When this test fails (which is necessarily a result of bumping the
// MinSupportedVersion), please carry out the cleanups that are now possible or
// file issues against the KV team asking them to do so (at which point you may
// comment out the failing check).
func TestObsoleteCode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	msv := clusterversion.RemoveDevOffset(clusterversion.MinSupported.Version())
	t.Logf("MinSupported: %v", msv)

	v25dot2 := clusterversion.RemoveDevOffset(clusterversion.V25_2.Version())

	// v25.2 is the last version to interpret RangeKeysInOrder. 25.3+ ignores
	// the field on incoming snapshots but continues to set it on outgoing
	// snapshots for compatibility reasons. This can be removed when the below
	// check fires.
	//
	// See https://github.com/cockroachdb/cockroach/pull/144613.
	//
	// NB: the below comparison intentionally minimizes the number of assumptions
	// on what release follows 25.2.
	if !msv.LessEq(v25dot2) {
		_ = kvserverpb.SnapshotRequest_Header{}.RangeKeysInOrder
		t.Fatalf("SnapshotRequest_Header.RangeKeysInOrder can be removed")
	}
}
