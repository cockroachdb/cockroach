// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
}

var verifyBelowRaftProtos bool

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	// The below-Raft proto tracking is fairly expensive in terms of allocations
	// which significantly impacts the tests under -race. We're already doing the
	// below-Raft proto tracking in non-race builds, so there is little benefit
	// to also doing it in race builds.
	if util.RaceEnabled {
		os.Exit(m.Run())
	}

	// Create a set of all protos we believe to be marshaled downstream of raft.
	// After the tests are run, we'll subtract the encountered protos from this
	// set.
	notBelowRaftProtos := make(map[reflect.Type]struct{}, len(belowRaftGoldenProtos))
	for typ := range belowRaftGoldenProtos {
		notBelowRaftProtos[typ] = struct{}{}
	}

	// Before running the tests, enable instrumentation that tracks protos which
	// are marshaled downstream of raft.
	stopTrackingAndGetTypes := kvserver.TrackRaftProtos()

	code := m.Run()

	// Only do this verification if the associated test was run. Without this
	// condition, the verification here would spuriously fail when running a
	// small subset of tests e.g. as we often do with `stress`.
	if verifyBelowRaftProtos {
		failed := false
		// Retrieve all the observed downstream-of-raft protos and confirm that they
		// are all present in our expected set.
		for _, typ := range stopTrackingAndGetTypes() {
			if _, ok := belowRaftGoldenProtos[typ]; ok {
				delete(notBelowRaftProtos, typ)
			} else {
				failed = true
				fmt.Printf("%s: missing fixture! Please adjust belowRaftGoldenProtos if necessary\n", typ)
			}
		}

		// Confirm that our expected set is now empty; we don't want to cement any
		// protos needlessly.
		// Two exceptions: these are sometimes observed below raft, sometimes not.
		// It supposedly depends on whether any test server runs for long enough to
		// write internal time series.
		delete(notBelowRaftProtos, reflect.TypeOf(&roachpb.InternalTimeSeriesData{}))
		delete(notBelowRaftProtos, reflect.TypeOf(&enginepb.MVCCMetadataSubsetForMergeSerialization{}))
		for typ := range notBelowRaftProtos {
			failed = true
			fmt.Printf("%s: not observed below raft!\n", typ)
		}

		// Make sure our error messages make it out.
		if failed && code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}
