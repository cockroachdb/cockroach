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

	current := clusterversion.ByKey(clusterversion.BinaryVersionKey)
	minSupported := clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey)

	future := current
	future.Major++

	tooOld := minSupported
	tooOld.Major--

	type testCase struct {
		binV, minV roachpb.Version // binary version and min supported version
		engV       roachpb.Version // version found on engine in test
		expErr     string
	}
	for name, tc := range map[string]testCase{
		"StoreTooNew": {
			// This is what the node is running.
			binV: current,
			// This is what the running node requires from its stores.
			minV: minSupported,
			// Version is way too high for this node.
			engV:   future,
			expErr: `cockroach version .* is incompatible with data in store <no-attributes>=<in-mem>; use version .* or later`,
		},
		"StoreTooOldVersion": {
			// This is what the node is running.
			binV: current,
			// This is what the running node requires from its stores.
			minV: minSupported,
			// Version is way too low.
			engV:   tooOld,
			expErr: `version .* is no longer supported`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engs := []storage.Engine{storage.NewDefaultInMemForTesting()}
			defer engs[0].Close()
			// Configure versions and write.
			cv := clusterversion.ClusterVersion{Version: tc.engV}
			err := WriteClusterVersionToEngines(ctx, engs, cv)
			if err == nil {
				cv, err = SynthesizeClusterVersionFromEngines(ctx, engs, tc.binV, tc.minV)
			}
			if !testutils.IsError(err, tc.expErr) {
				t.Fatalf("unexpected error: %+v, got version %v", err, cv)
			}
		})
	}
}
