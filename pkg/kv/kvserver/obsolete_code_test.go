// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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

	// Example: https://github.com/cockroachdb/cockroach/pull/144616
	//
	//	v25dot2 := clusterversion.RemoveDevOffset(clusterversion.V25_2.Version())
	//	if !msv.LessEq(v25dot2) {
	//		_ = some.Type{}.DeprecatedField
	//		t.Fatalf("DeprecatedField can be removed")
	//	}
}
