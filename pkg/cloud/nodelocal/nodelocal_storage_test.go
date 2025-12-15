// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nodelocal

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
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

func TestListLocalSkipsTempFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	ctx := context.Background()
	externalStorage := TestingMakeNodelocalStorage(
		dir,
		cluster.MakeTestingClusterSettings(),
		cloudpb.ExternalStorage{},
	)
	writer, err := externalStorage.Writer(ctx, "foo.txt")
	require.NoError(t, err)
	writer.Close()

	tmpWriter, err := externalStorage.Writer(ctx, "foo.txt1234.tmp")
	require.NoError(t, err)
	tmpWriter.Close()

	var files []string
	err = externalStorage.List(ctx, "/", "", func(name string) error {
		files = append(files, name)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"/foo.txt"}, files)
}
