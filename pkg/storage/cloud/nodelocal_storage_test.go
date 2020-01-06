// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testSettings.ExternalIODir = p
	dest := MakeLocalStorageURI(p)

	testExportStore(t, dest, false)
	testListFiles(t, fmt.Sprintf("nodelocal:///%s", "listing-test"))
}

func TestLocalIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	const allowed = "/allowed"
	testSettings.ExternalIODir = allowed

	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	for dest, expected := range map[string]string{allowed: "", "/../../blah": "not allowed"} {
		u := fmt.Sprintf("nodelocal://%s", dest)
		e, err := ExternalStorageFromURI(ctx, u, testSettings, clientFactory)
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.ListFiles(ctx)
		if !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
	}

	for host, expectErr := range map[string]bool{"": false, "1": false, "0": false, "blah": true} {
		u := fmt.Sprintf("nodelocal://%s/path/to/file", host)

		var expected string
		if expectErr {
			expected = "host component of nodelocal URI must be a node ID"
		}
		if _, err := ExternalStorageConfFromURI(u); !testutils.IsError(err, expected) {
			t.Fatalf("%q: expected error %q, got %v", u, expected, err)
		}
	}
}
