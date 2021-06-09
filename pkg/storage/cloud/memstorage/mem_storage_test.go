// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memstorage

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"testing"
)

func TestMemStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.IgnoreLint(t)

	ctx := context.Background()

	conf, err := cloud.ExternalStorageConfFromURI("mem-test:///", security.RootUserName())
	if err != nil {
		t.Fatal(err)
	}

	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	testSettings := cluster.MakeTestingClusterSettings()
	cloudtestutils.CheckExportStore(t, "mem-test:///", false, security.RootUserName(), nil, nil, testSettings)
}
