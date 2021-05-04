// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpltests

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func requireImplicitGoogleCredentials(t *testing.T) {
	t.Helper()
	// TODO(yevgeniy): Fix default credentials check.
	skip.IgnoreLint(t, "implicit credentials not configured")
}

func TestAntagonisticGCSRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requireImplicitGoogleCredentials(t)

	gsFile := "gs://cockroach-fixtures/tpch-csv/sf-1/region.tbl?AUTH=implicit"
	conf, err := cloudimpl.ExternalStorageConfFromURI(gsFile, security.RootUserName())
	require.NoError(t, err)

	testAntagonisticRead(t, conf)
}

// TestFileDoesNotExist ensures that the ReadFile method of google cloud storage
// returns a sentinel error when the `Bucket` or `Object` being read do not
// exist.
func TestFileDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requireImplicitGoogleCredentials(t)
	user := security.RootUserName()

	{
		// Invalid gsFile.
		gsFile := "gs://cockroach-fixtures/tpch-csv/sf-1/invalid_region.tbl?AUTH=implicit"
		conf, err := cloudimpl.ExternalStorageConfFromURI(gsFile, user)
		require.NoError(t, err)

		s, err := cloudimpl.MakeExternalStorage(
			context.Background(), conf, base.ExternalIODirConfig{}, testSettings,
			nil, nil, nil)
		require.NoError(t, err)
		_, err = s.ReadFile(context.Background(), "")
		require.Error(t, err, "")
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist))
	}

	{
		// Invalid gsBucket.
		gsFile := "gs://cockroach-fixtures-invalid/tpch-csv/sf-1/region.tbl?AUTH=implicit"
		conf, err := cloudimpl.ExternalStorageConfFromURI(gsFile, user)
		require.NoError(t, err)

		s, err := cloudimpl.MakeExternalStorage(
			context.Background(), conf, base.ExternalIODirConfig{}, testSettings, nil,
			nil, nil)
		require.NoError(t, err)
		_, err = s.ReadFile(context.Background(), "")
		require.Error(t, err, "")
		require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist))
	}
}

func TestCompressedGCS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requireImplicitGoogleCredentials(t)

	user := security.RootUserName()
	ctx := context.Background()

	// gsutil cp /usr/share/dict/words gs://cockroach-fixtures/words-compressed.txt
	gsFile1 := "gs://cockroach-fixtures/words.txt?AUTH=implicit"

	// gsutil cp -Z /usr/share/dict/words gs://cockroach-fixtures/words-compressed.txt
	gsFile2 := "gs://cockroach-fixtures/words-compressed.txt?AUTH=implicit"

	conf1, err := cloudimpl.ExternalStorageConfFromURI(gsFile1, user)
	require.NoError(t, err)
	conf2, err := cloudimpl.ExternalStorageConfFromURI(gsFile2, user)
	require.NoError(t, err)

	s1, err := cloudimpl.MakeExternalStorage(ctx, conf1, base.ExternalIODirConfig{}, testSettings, nil, nil, nil)
	require.NoError(t, err)
	s2, err := cloudimpl.MakeExternalStorage(ctx, conf2, base.ExternalIODirConfig{}, testSettings, nil, nil, nil)
	require.NoError(t, err)

	reader1, err := s1.ReadFile(context.Background(), "")
	require.NoError(t, err)
	reader2, err := s2.ReadFile(context.Background(), "")
	require.NoError(t, err)

	content1, err := ioutil.ReadAll(reader1)
	require.NoError(t, err)
	content2, err := ioutil.ReadAll(reader2)
	require.NoError(t, err)

	require.Equal(t, string(content1), string(content2))
}
