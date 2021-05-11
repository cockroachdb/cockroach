// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nodelocal

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testSettings := cluster.MakeTestingClusterSettings()
	testSettings.ExternalIODir = p
	dest := MakeLocalStorageURI(p)

	cloudtestutils.CheckExportStore(t, dest, false, security.RootUserName(), nil, nil, testSettings)
	cloudtestutils.CheckListFiles(t, "nodelocal://0/listing-test/basepath",
		security.RootUserName(), nil, nil, testSettings)
}

func TestLocalIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	testSettings := cluster.MakeTestingClusterSettings()

	const allowed = "/allowed"
	testSettings.ExternalIODir = allowed

	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	user := security.RootUserName()

	baseDir, err := cloud.ExternalStorageFromURI(ctx, "nodelocal://0/", base.ExternalIODirConfig{},
		testSettings, clientFactory, user, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	for dest, expected := range map[string]string{allowed: "", "/../../blah": "not allowed"} {
		u := fmt.Sprintf("nodelocal://0%s", dest)
		e, err := cloud.ExternalStorageFromURI(ctx, u, base.ExternalIODirConfig{}, testSettings,
			clientFactory, user, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if _, err = e.ListFiles(ctx, ""); !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
		if _, err = baseDir.ListFiles(ctx, dest); !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
	}

	for host, expectErr := range map[string]bool{"": false, "1": false, "0": false, "blah": true} {
		u := fmt.Sprintf("nodelocal://0%s/path/to/file", host)

		var expected string
		if expectErr {
			expected = "host component of nodelocal URI must be a node ID"
		}
		if _, err := cloud.ExternalStorageConfFromURI(u, user); !testutils.IsError(err, expected) {
			t.Fatalf("%q: expected error %q, got %v", u, expected, err)
		}
	}
}
