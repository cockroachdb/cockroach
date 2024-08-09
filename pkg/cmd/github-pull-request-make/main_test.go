// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
)

func TestPkgsFromDiff(t *testing.T) {
	for filename, expPkgs := range map[string]map[string]pkg{
		datapathutils.TestDataPath(t, "10305.diff"): {
			"pkg/roachpb": {tests: []string{"TestLeaseEquivalence"}},
			"pkg/storage": {tests: []string{"TestStoreRangeLease", "TestStoreRangeLeaseSwitcheroo"}},
		},
		datapathutils.TestDataPath(t, "skip.diff"): {
			"pkg/ccl/storageccl": {tests: []string{"TestPutS3"}},
		},
		// This PR had some churn and renamed packages. This was formerly problematic
		// because nonexistent packages would be emitted.
		datapathutils.TestDataPath(t, "27595.diff"): {
			"pkg/storage/closedts/transport": {tests: []string{"TestTransportConnectOnRequest", "TestTransportClientReceivesEntries"}},
			"pkg/storage/closedts/container": {tests: []string{"TestTwoNodes"}},
			"pkg/storage/closedts/storage":   {tests: []string{"TestConcurrent"}},
		},
		datapathutils.TestDataPath(t, "removed.diff"): {},
	} {
		t.Run(filename, func(t *testing.T) {
			f, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			pkgs, err := pkgsFromDiff(f)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(pkgs, expPkgs) {
				t.Errorf("expected %s, got %s", expPkgs, pkgs)
			}
		})
	}
}
