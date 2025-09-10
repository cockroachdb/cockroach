// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nodelocal

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	dest := MakeLocalStorageURI(p)

	info := cloudtestutils.StoreInfo{
		URI:           dest,
		User:          username.RootUserName(),
		ExternalIODir: p,
	}
	cloudtestutils.CheckExportStore(t, info)
	info.URI = "nodelocal://1/listing-test/basepath"
	cloudtestutils.CheckListFiles(t, info)
}

func TestNodelocalWriteAndFullReadRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	prefix := fmt.Sprintf("%s-%d", t.Name(), cloudtestutils.NewTestID())
	uri := fmt.Sprintf("nodelocal://1/%s", prefix)
	info := cloudtestutils.StoreInfo{
		URI:           uri,
		User:          username.RootUserName(),
		ExternalIODir: dir,
	}

	cloudtestutils.CheckWriteAndFullReadRoundtrip(t, info)
}
