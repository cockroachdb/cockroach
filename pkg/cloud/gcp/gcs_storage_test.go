// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

func TestPutGoogleCloud(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "GS_BUCKET env var must be set")
	}

	user := security.RootUserName()
	testSettings := cluster.MakeTestingClusterSettings()

	testutils.RunTrueAndFalse(t, "specified", func(t *testing.T, specified bool) {
		credentials := os.Getenv("GS_JSONKEY")
		if credentials == "" {
			skip.IgnoreLint(t, "GS_JSONKEY env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
		uri := fmt.Sprintf("gs://%s/%s?%s=%s",
			bucket,
			"backup-test-specified",
			CredentialsParam,
			url.QueryEscape(encoded),
		)
		if specified {
			uri += fmt.Sprintf("&%s=%s", cloud.AuthParam, cloud.AuthParamSpecified)
		}
		t.Log(uri)
		cloudtestutils.CheckExportStore(t, uri, false, user, nil, nil, testSettings)
		cloudtestutils.CheckListFiles(t,
			fmt.Sprintf("gs://%s/%s/%s?%s=%s&%s=%s",
				bucket,
				"backup-test-specified",
				"listing-test",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				CredentialsParam,
				url.QueryEscape(encoded),
			),
			security.RootUserName(), nil, nil, testSettings,
		)
	})
	t.Run("implicit", func(t *testing.T) {
		// Only test these if they exist.
		if _, err := google.FindDefaultCredentials(context.Background()); err != nil {
			skip.IgnoreLint(t, err)
		}
		cloudtestutils.CheckExportStore(t, fmt.Sprintf("gs://%s/%s?%s=%s", bucket, "backup-test-implicit",
			cloud.AuthParam, cloud.AuthParamImplicit), false, user, nil, nil, testSettings)
		cloudtestutils.CheckListFiles(t,
			fmt.Sprintf("gs://%s/%s/%s?%s=%s",
				bucket,
				"backup-test-implicit",
				"listing-test",
				cloud.AuthParam,
				cloud.AuthParamImplicit,
			),
			security.RootUserName(), nil, nil, testSettings,
		)
	})
}

func requireImplicitGoogleCredentials(t *testing.T) {
	t.Helper()
	// TODO(yevgeniy): Fix default credentials check.
	skip.IgnoreLint(t, "implicit credentials not configured")
}

func TestAntagonisticGCSRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requireImplicitGoogleCredentials(t)

	testSettings := cluster.MakeTestingClusterSettings()

	gsFile := "gs://cockroach-fixtures/tpch-csv/sf-1/region.tbl?AUTH=implicit"
	conf, err := cloud.ExternalStorageConfFromURI(gsFile, security.RootUserName())
	require.NoError(t, err)

	cloudtestutils.CheckAntagonisticRead(t, conf, testSettings)
}

// TestFileDoesNotExist ensures that the ReadFile method of google cloud storage
// returns a sentinel error when the `Bucket` or `Object` being read do not
// exist.
func TestFileDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	requireImplicitGoogleCredentials(t)
	user := security.RootUserName()

	testSettings := cluster.MakeTestingClusterSettings()

	{
		// Invalid gsFile.
		gsFile := "gs://cockroach-fixtures/tpch-csv/sf-1/invalid_region.tbl?AUTH=implicit"
		conf, err := cloud.ExternalStorageConfFromURI(gsFile, user)
		require.NoError(t, err)

		s, err := cloud.MakeExternalStorage(
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
		conf, err := cloud.ExternalStorageConfFromURI(gsFile, user)
		require.NoError(t, err)

		s, err := cloud.MakeExternalStorage(
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

	testSettings := cluster.MakeTestingClusterSettings()

	// gsutil cp /usr/share/dict/words gs://cockroach-fixtures/words-compressed.txt
	gsFile1 := "gs://cockroach-fixtures/words.txt?AUTH=implicit"

	// gsutil cp -Z /usr/share/dict/words gs://cockroach-fixtures/words-compressed.txt
	gsFile2 := "gs://cockroach-fixtures/words-compressed.txt?AUTH=implicit"

	conf1, err := cloud.ExternalStorageConfFromURI(gsFile1, user)
	require.NoError(t, err)
	conf2, err := cloud.ExternalStorageConfFromURI(gsFile2, user)
	require.NoError(t, err)

	s1, err := cloud.MakeExternalStorage(ctx, conf1, base.ExternalIODirConfig{}, testSettings, nil, nil, nil)
	require.NoError(t, err)
	s2, err := cloud.MakeExternalStorage(ctx, conf2, base.ExternalIODirConfig{}, testSettings, nil, nil, nil)
	require.NoError(t, err)

	reader1, err := s1.ReadFile(context.Background(), "")
	require.NoError(t, err)
	reader2, err := s2.ReadFile(context.Background(), "")
	require.NoError(t, err)

	content1, err := ioctx.ReadAll(ctx, reader1)
	require.NoError(t, err)
	content2, err := ioctx.ReadAll(ctx, reader2)
	require.NoError(t, err)

	require.Equal(t, string(content1), string(content2))
}
