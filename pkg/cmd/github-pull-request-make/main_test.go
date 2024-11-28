// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
)

func TestPkgsFromDiff(t *testing.T) {
	for filename, expPkgs := range map[string]map[string]pkg{
		datapathutils.TestDataPath(t, "skip.diff"): {
			"pkg/ccl/storageccl": makePkg([]string{"TestPutS3"}),
		},
		datapathutils.TestDataPath(t, "modified.diff"): {
			"pkg/crosscluster/physical": makePkg([]string{"TestStreamingAutoReplan"}),
		},
		datapathutils.TestDataPath(t, "removed.diff"): {},
		datapathutils.TestDataPath(t, "not_go.diff"):  {},
		datapathutils.TestDataPath(t, "new_test.diff"): {
			"pkg/crosscluster/streamclient": makePkg([]string{
				"TestExternalConnectionClient",
				"TestGetFirstActiveClientEmpty",
			}),
		},
		datapathutils.TestDataPath(t, "dont_stress.diff"): {},
		datapathutils.TestDataPath(t, "27595.diff"): {
			"pkg/storage/closedts/container": makePkg([]string{
				"TestTwoNodes",
			}),
			"pkg/storage/closedts/minprop": makePkg([]string{
				"TestTrackerConcurrentUse",
			}),
			"pkg/storage/closedts/storage": makePkg([]string{
				"TestConcurrent",
			}),
			"pkg/storage/closedts/transport": makePkg([]string{
				"TestTransportConnectOnRequest",
				"TestTransportClientReceivesEntries",
			}),
		},
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

func TestUpTo5TestsPerPackage(t *testing.T) {
	fileName := datapathutils.TestDataPath(t, "five_tests.diff")
	f, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pkgs, err := pkgsFromDiff(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range pkgs {
		if len(pkg.tests) > 5 {
			t.Fatal("more than 5 tests in package")
		}
	}
}
