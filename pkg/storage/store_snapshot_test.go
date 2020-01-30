// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestSnapshotPreemptiveOnUninitializedReplica is a targeted regression test
// against a bug that once accepted these snapshots without forcing them to
// check for overlapping ranges.
func TestSnapshotPreemptiveOnUninitializedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	store, _ := createTestStore(t, testStoreOpts{}, stopper)

	// Create an uninitialized replica.
	repl, created, err := store.getOrCreateReplica(ctx, 77, 1, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("no replica created")
	}

	// Make a descriptor that overlaps r1 (any descriptor does because r1 covers
	// all of the keyspace).
	desc := *repl.Desc()
	desc.StartKey = roachpb.RKey("a")
	desc.EndKey = roachpb.RKey("b")

	header := &SnapshotRequest_Header{}
	header.State.Desc = &desc

	if !header.IsPreemptive() {
		t.Fatal("mock snapshot isn't preemptive")
	}

	if _, err := store.canApplyPreemptiveSnapshot(
		ctx, header, true, /* authoritative */
	); !testutils.IsError(err, "intersects existing range") {
		t.Fatal(err)
	}
}
