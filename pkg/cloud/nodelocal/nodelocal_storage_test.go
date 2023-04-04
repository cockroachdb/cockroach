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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

	cloudtestutils.CheckExportStore(
		t, dest, false, username.RootUserName(), nil /* db */, testSettings)
	url := "nodelocal://1/listing-test/basepath"
	cloudtestutils.CheckListFiles(
		t, url, username.RootUserName(), nil /*db */, testSettings,
	)
}
