// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
)

func ciStressTestdataFile(t *testing.T, file string) string {
	if bazel.BuiltWithBazel() {
		f, err := bazel.Runfile(filepath.Join("pkg", "cmd", "ci-stress", "testdata", file))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	return filepath.Join("..", "ci-stress", "testdata", file)
}

func TestPkgsFromDiff(t *testing.T) {
	for filename, expPkgs := range map[string]map[string]pkg{
		ciStressTestdataFile(t, "skip.diff"): {
			"pkg/storage": makePkg([]string{"TestEncryptDecrypt"}),
		},
		ciStressTestdataFile(t, "modified.diff"): {
			"pkg/crosscluster/physical": makePkg([]string{"TestStreamingAutoReplan"}),
		},
		ciStressTestdataFile(t, "removed.diff"): {},
		ciStressTestdataFile(t, "not_go.diff"):  {},
		ciStressTestdataFile(t, "new_test.diff"): {
			"pkg/crosscluster/streamclient": makePkg([]string{
				"TestExternalConnectionClient",
				"TestGetFirstActiveClientEmpty",
			}),
		},
		datapathutils.TestDataPath(t, "dont_stress.diff"): {},
		ciStressTestdataFile(t, "27595.diff"): {
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
