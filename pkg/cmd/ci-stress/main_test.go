// Copyright 2025 The Cockroach Authors.
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

func TestPkgToTests(t *testing.T) {
	for filename, expPkgs := range map[string]map[string][]string{
		datapathutils.TestDataPath(t, "skip.diff"): {
			"pkg/storage": []string{"TestEncryptDecrypt"},
		},
		datapathutils.TestDataPath(t, "modified.diff"): {
			"pkg/crosscluster/physical": []string{"TestStreamingAutoReplan"},
		},
		datapathutils.TestDataPath(t, "removed.diff"): {},
		datapathutils.TestDataPath(t, "not_go.diff"):  {},
		datapathutils.TestDataPath(t, "new_test.diff"): {
			"pkg/crosscluster/streamclient": []string{
				"TestExternalConnectionClient",
				"TestGetFirstActiveClientEmpty",
			},
		},
		datapathutils.TestDataPath(t, "27595.diff"): {
			"pkg/storage/closedts/container": []string{
				"TestTwoNodes",
			},
			"pkg/storage/closedts/minprop": []string{
				"TestTrackerConcurrentUse",
			},
			"pkg/storage/closedts/storage": []string{
				"TestConcurrent",
			},
			"pkg/storage/closedts/transport": []string{
				"TestTransportClientReceivesEntries",
				"TestTransportConnectOnRequest",
			},
		},
	} {
		t.Run(filename, func(t *testing.T) {
			content, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			pkgToTests := getPkgToTests(string(content))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(pkgToTests, expPkgs) {
				t.Errorf("expected %s, got %s", expPkgs, pkgToTests)
			}
		})
	}
}
