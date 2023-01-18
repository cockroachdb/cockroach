// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestStoresClusterVersionIncompatible verifies an error occurs when
// setting up the cluster version from stores that are incompatible with the
// running binary.
func TestStoresClusterVersionIncompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	vOneDashOne := roachpb.Version{Major: 1, Internal: 1}
	vOne := roachpb.Version{Major: 1}

	type testCase struct {
		binV, minV roachpb.Version // binary version and min supported version
		engV       roachpb.Version // version found on engine in test
		expErr     string
	}
	for name, tc := range map[string]testCase{
		"StoreTooNew": {
			// This is what the node is running.
			binV: vOneDashOne,
			// This is what the running node requires from its stores.
			minV: vOne,
			// Version is way too high for this node.
			engV:   roachpb.Version{Major: 9},
			expErr: `cockroach version v1\.0-1 is incompatible with data in store <no-attributes>=<in-mem>; use version v9\.0 or later`,
		},
		"StoreTooOldVersion": {
			// This is what the node is running.
			binV: roachpb.Version{Major: 9},
			// This is what the running node requires from its stores.
			minV: roachpb.Version{Major: 5},
			// Version is way too low.
			engV:   roachpb.Version{Major: 4},
			expErr: `store <no-attributes>=<in-mem>, last used with cockroach version v4\.0, is too old for running version v9\.0 \(which requires data from v5\.0 or later\)`,
		},
		"StoreTooOldMinVersion": {
			// Like the previous test case, but this time cv.MinimumVersion is the culprit.
			binV:   roachpb.Version{Major: 9},
			minV:   roachpb.Version{Major: 5},
			engV:   roachpb.Version{Major: 4},
			expErr: `store <no-attributes>=<in-mem>, last used with cockroach version v4\.0, is too old for running version v9\.0 \(which requires data from v5\.0 or later\)`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engs := []storage.Engine{storage.NewDefaultInMemForTesting()}
			defer engs[0].Close()
			// Configure versions and write.
			cv := clusterversion.ClusterVersion{Version: tc.engV}
			if err := WriteClusterVersionToEngines(ctx, engs, cv); err != nil {
				t.Fatal(err)
			}
			if cv, err := SynthesizeClusterVersionFromEngines(
				ctx, engs, tc.binV, tc.minV,
			); !testutils.IsError(err, tc.expErr) {
				t.Fatalf("unexpected error: %+v, got version %v", err, cv)
			}
		})
	}
}
