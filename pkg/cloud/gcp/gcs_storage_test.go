// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

func TestPutGoogleCloud(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GOOGLE_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "GOOGLE_BUCKET env var must be set")
	}

	user := username.RootUserName()
	testID := cloudtestutils.NewTestID()

	testutils.RunTrueAndFalse(t, "auth-specified-with-auth-param", func(t *testing.T, specified bool) {
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
		uri := fmt.Sprintf("gs://%s/%s-%d?%s=%s",
			bucket,
			"backup-test-specified",
			testID,
			CredentialsParam,
			url.QueryEscape(encoded),
		)
		if specified {
			uri += fmt.Sprintf("&%s=%s", cloud.AuthParam, cloud.AuthParamSpecified)
		}
		info := cloudtestutils.StoreInfo{
			URI:  uri,
			User: user,
		}
		cloudtestutils.CheckExportStore(t, info)
		info = cloudtestutils.StoreInfo{
			URI: fmt.Sprintf("gs://%s/%s-%d/%s?%s=%s&%s=%s",
				bucket,
				"backup-test-specified",
				testID,
				"listing-test",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				CredentialsParam,
				url.QueryEscape(encoded),
			),
			User: username.RootUserName(),
		}
		cloudtestutils.CheckListFiles(t, info)
	})
	t.Run("auth-implicit", func(t *testing.T) {
		if !cloudtestutils.IsImplicitAuthConfigured() {
			skip.IgnoreLint(t, "implicit auth is not configured")
		}
		info := cloudtestutils.StoreInfo{
			URI: fmt.Sprintf("gs://%s/%s-%d?%s=%s", bucket, "backup-test-implicit", testID,
				cloud.AuthParam, cloud.AuthParamImplicit),
			User: user,
		}
		cloudtestutils.CheckExportStore(t, info)
		info = cloudtestutils.StoreInfo{
			URI: fmt.Sprintf("gs://%s/%s-%d/%s?%s=%s",
				bucket,
				"backup-test-implicit",
				testID,
				"listing-test",
				cloud.AuthParam,
				cloud.AuthParamImplicit,
			),
			User: username.RootUserName(),
		}
		cloudtestutils.CheckListFiles(t, info)
	})

	t.Run("auth-specified-bearer-token", func(t *testing.T) {
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}

		ctx := context.Background()
		source, err := google.JWTConfigFromJSON([]byte(credentials), gcs.ScopeReadWrite)
		require.NoError(t, err, "creating GCS oauth token source from specified credentials")
		ts := source.TokenSource(ctx)

		token, err := ts.Token()
		require.NoError(t, err, "getting token")

		uri := fmt.Sprintf("gs://%s/%s-%d?%s=%s",
			bucket,
			"backup-test-specified",
			testID,
			BearerTokenParam,
			token.AccessToken,
		)
		uri += fmt.Sprintf("&%s=%s", cloud.AuthParam, cloud.AuthParamSpecified)
		info := cloudtestutils.StoreInfo{
			URI:  uri,
			User: user,
		}
		cloudtestutils.CheckExportStore(t, info)
		info = cloudtestutils.StoreInfo{
			URI: fmt.Sprintf("gs://%s/%s-%d/%s?%s=%s&%s=%s",
				bucket,
				"backup-test-specified",
				testID,
				"listing-test",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				BearerTokenParam,
				token.AccessToken,
			),
			User: username.RootUserName(),
		}
		cloudtestutils.CheckListFiles(t, info)
	})
}

func TestGCSAssumeRole(t *testing.T) {
	user := username.RootUserName()

	limitedBucket := os.Getenv("GOOGLE_LIMITED_BUCKET")
	if limitedBucket == "" {
		skip.IgnoreLint(t, "GOOGLE_LIMITED_BUCKET env var must be set")
	}
	assumedAccount := os.Getenv("ASSUME_SERVICE_ACCOUNT")
	if assumedAccount == "" {
		skip.IgnoreLint(t, "ASSUME_SERVICE_ACCOUNT env var must be set")
	}

	testID := cloudtestutils.NewTestID()

	t.Run("specified", func(t *testing.T) {
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))

		info := cloudtestutils.StoreInfo{
			URI: fmt.Sprintf(
				"gs://%s/%s-%d?%s=%s", limitedBucket, "backup-test-assume-role", testID,
				CredentialsParam, url.QueryEscape(encoded)),
			User: user,
		}
		// Verify that specified permissions with the credentials do not give us
		// access to the bucket.
		cloudtestutils.CheckNoPermission(t, info)
		info.URI = fmt.Sprintf("gs://%s/%s-%d?%s=%s&%s=%s&%s=%s",
			limitedBucket,
			"backup-test-assume-role",
			testID,
			cloud.AuthParam,
			cloud.AuthParamSpecified,
			AssumeRoleParam,
			assumedAccount, CredentialsParam,
			url.QueryEscape(encoded),
		)
		cloudtestutils.CheckExportStore(t, info)
		info = cloudtestutils.StoreInfo{
			URI: fmt.Sprintf("gs://%s/%s-%d/%s?%s=%s&%s=%s&%s=%s",
				limitedBucket,
				"backup-test-assume-role",
				testID,
				"listing-test",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AssumeRoleParam,
				assumedAccount,
				CredentialsParam,
				url.QueryEscape(encoded),
			),
			User: username.RootUserName(),
		}
		cloudtestutils.CheckListFiles(t, info)
	})

	t.Run("implicit", func(t *testing.T) {
		if _, err := google.FindDefaultCredentials(context.Background()); err != nil {
			skip.IgnoreLint(t, err)
		}

		info := cloudtestutils.StoreInfo{
			URI: fmt.Sprintf(
				"gs://%s/%s-%d?%s=%s", limitedBucket, "backup-test-assume-role", testID,
				cloud.AuthParam, cloud.AuthParamImplicit,
			),
			User: user,
		}
		cloudtestutils.CheckNoPermission(t, info)
		info.URI = fmt.Sprintf(
			"gs://%s/%s-%d?%s=%s&%s=%s", limitedBucket, "backup-test-assume-role", testID,
			cloud.AuthParam, cloud.AuthParamImplicit, AssumeRoleParam, assumedAccount,
		)
		cloudtestutils.CheckExportStore(t, info)
		info.URI = fmt.Sprintf("gs://%s/%s-%d/%s?%s=%s&%s=%s",
			limitedBucket,
			"backup-test-assume-role",
			testID,
			"listing-test",
			cloud.AuthParam,
			cloud.AuthParamImplicit,
			AssumeRoleParam,
			assumedAccount,
		)
		cloudtestutils.CheckListFiles(t, info)
	})

	t.Run("role-chaining", func(t *testing.T) {
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))

		roleChainStr := os.Getenv("ASSUME_SERVICE_ACCOUNT_CHAIN")
		if roleChainStr == "" {
			skip.IgnoreLint(t, "ASSUME_SERVICE_ACCOUNT_CHAIN env var must be set")
		}

		roleChain := strings.Split(roleChainStr, ",")

		for _, tc := range []struct {
			auth        string
			credentials string
		}{
			{cloud.AuthParamSpecified, encoded},
			{cloud.AuthParamImplicit, ""},
		} {
			t.Run(tc.auth, func(t *testing.T) {
				q := make(url.Values)
				q.Set(cloud.AuthParam, tc.auth)
				q.Set(CredentialsParam, tc.credentials)

				// First verify that none of the individual roles in the chain can be used
				// to access the storage.
				for _, role := range roleChain {
					q.Set(AssumeRoleParam, role)
					info := cloudtestutils.StoreInfo{
						URI: fmt.Sprintf("gs://%s/%s-%d/%s?%s",
							limitedBucket,
							"backup-test-assume-role",
							testID,
							"listing-test",
							q.Encode(),
						),
						User: user,
					}
					cloudtestutils.CheckNoPermission(t, info)
				}

				// Finally, check that the chain of roles can be used to access the storage.
				q.Set(AssumeRoleParam, roleChainStr)
				info := cloudtestutils.StoreInfo{
					URI: fmt.Sprintf("gs://%s/%s-%d/%s?%s",
						limitedBucket,
						"backup-test-assume-role",
						testID,
						"listing-test",
						q.Encode(),
					),
					User: user,
				}
				cloudtestutils.CheckExportStore(t, info)
				cloudtestutils.CheckListFiles(t, info)
			})
		}
	})
}

func TestAntagonisticGCSRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !cloudtestutils.IsImplicitAuthConfigured() {
		skip.IgnoreLint(t, "implicit auth is not configured")
	}

	testSettings := cluster.MakeTestingClusterSettings()

	gsFile := "gs://nightly-cloud-unit-tests/antagonistic-read?AUTH=implicit"
	conf, err := cloud.ExternalStorageConfFromURI(gsFile, username.RootUserName())
	require.NoError(t, err)

	cloudtestutils.CheckAntagonisticRead(t, conf, testSettings)
}

// TestFileDoesNotExist ensures that the ReadFile method of google cloud storage
// returns a sentinel error when the `Bucket` or `Object` being read do not
// exist.
func TestFileDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !cloudtestutils.IsImplicitAuthConfigured() {
		skip.IgnoreLint(t, "implicit auth is not configured")
	}

	user := username.RootUserName()

	testSettings := cluster.MakeTestingClusterSettings()

	{
		// Invalid gsFile.
		gsFile := "gs://cockroach-fixtures-us-east1/tpch-csv/sf-1/invalid_region.tbl?AUTH=implicit"
		conf, err := cloud.ExternalStorageConfFromURI(gsFile, user)
		require.NoError(t, err)

		s, err := cloud.MakeExternalStorage(context.Background(), conf, base.ExternalIODirConfig{}, testSettings,
			nil, /* blobClientFactory */
			nil, /* db */
			nil, /* limiters */
			cloud.NilMetrics,
		)
		require.NoError(t, err)
		_, _, err = s.ReadFile(context.Background(), "", cloud.ReadOptions{NoFileSize: true})
		require.ErrorIs(t, err, cloud.ErrFileDoesNotExist)
	}

	{
		// Use a random UUID as the name of the bucket that does not exist in order
		// to avoid name squating.
		gsFile := fmt.Sprintf("gs://%s/tpch-csv/sf-1/region.tbl?AUTH=implicit", uuid.NewV4())
		conf, err := cloud.ExternalStorageConfFromURI(gsFile, user)
		require.NoError(t, err)

		s, err := cloud.MakeExternalStorage(context.Background(), conf, base.ExternalIODirConfig{}, testSettings,
			nil, /* blobClientFactory */
			nil, /* db */
			nil, /* limiters */
			cloud.NilMetrics,
		)
		require.NoError(t, err)
		_, _, err = s.ReadFile(context.Background(), "", cloud.ReadOptions{NoFileSize: true})
		require.ErrorIs(t, err, cloud.ErrFileDoesNotExist)
	}
}

func TestCompressedGCS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !cloudtestutils.IsImplicitAuthConfigured() {
		skip.IgnoreLint(t, "implicit auth is not configured")
	}

	user := username.RootUserName()
	ctx := context.Background()

	testSettings := cluster.MakeTestingClusterSettings()

	// gsutil cp /usr/share/dict/words gs://cockroach-fixtures-us-east1/words-compressed.txt
	gsFile1 := "gs://cockroach-fixtures-us-east1/words.txt?AUTH=implicit"

	// gsutil cp -Z /usr/share/dict/words gs://cockroach-fixtures-us-east1/words-compressed.txt
	gsFile2 := "gs://cockroach-fixtures-us-east1/words-compressed.txt?AUTH=implicit"

	conf1, err := cloud.ExternalStorageConfFromURI(gsFile1, user)
	require.NoError(t, err)
	conf2, err := cloud.ExternalStorageConfFromURI(gsFile2, user)
	require.NoError(t, err)

	s1, err := cloud.MakeExternalStorage(ctx, conf1, base.ExternalIODirConfig{}, testSettings,
		nil, /* blobClientFactory */
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	s2, err := cloud.MakeExternalStorage(ctx, conf2, base.ExternalIODirConfig{}, testSettings,
		nil, /* blobClientFactory */
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)

	reader1, _, err := s1.ReadFile(context.Background(), "", cloud.ReadOptions{NoFileSize: true})
	require.NoError(t, err)
	reader2, _, err := s2.ReadFile(context.Background(), "", cloud.ReadOptions{NoFileSize: true})
	require.NoError(t, err)

	content1, err := ioctx.ReadAll(ctx, reader1)
	require.NoError(t, err)
	require.NoError(t, reader1.Close(context.Background()))
	content2, err := ioctx.ReadAll(ctx, reader2)
	require.NoError(t, err)
	require.NoError(t, reader2.Close(context.Background()))

	require.Equal(t, string(content1), string(content2))

	// Test reading parts of the uncompressed object.
	for i := 0; i < 10; i++ {
		ofs := rand.Intn(len(content1) - 1)
		l := rand.Intn(len(content1) - ofs)
		reader, _, err := s1.ReadFile(context.Background(), "", cloud.ReadOptions{
			Offset:     int64(ofs),
			LengthHint: int64(l),
			NoFileSize: true,
		})
		require.NoError(t, err)
		content, err := ioctx.ReadAll(ctx, reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close(context.Background()))
		require.Equal(t, len(content), l)
		require.Equal(t, string(content), string(content1[ofs:ofs+l]))
	}
}

// TestReadFileAtReturnsSize tests that ReadFileAt returns
// a cloud.ResumingReader that contains the size of the file.
func TestReadFileAtReturnsSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !cloudtestutils.IsImplicitAuthConfigured() {
		skip.IgnoreLint(t, "implicit auth is not configured")
	}

	bucket := os.Getenv("GOOGLE_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "GOOGLE_BUCKET env var must be set")
	}

	testID := cloudtestutils.NewTestID()
	user := username.RootUserName()
	ctx := context.Background()
	testSettings := cluster.MakeTestingClusterSettings()
	file := "testfile"
	data := []byte("hello world")

	gsURI := fmt.Sprintf("gs://%s/%s-%d?AUTH=implicit", bucket, "read-file-at-returns-size", testID)
	conf, err := cloud.ExternalStorageConfFromURI(gsURI, user)
	require.NoError(t, err)
	args := cloud.EarlyBootExternalStorageContext{
		IOConf:   base.ExternalIODirConfig{},
		Settings: testSettings,
		Options:  nil,
		Limiters: nil,
	}
	s, err := makeGCSStorage(ctx, args, conf)
	require.NoError(t, err)

	w, err := s.Writer(ctx, file)
	require.NoError(t, err)

	_, err = w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	reader, _, err := s.ReadFile(ctx, file, cloud.ReadOptions{})
	require.NoError(t, err)

	rr, ok := reader.(*cloud.ResumingReader)
	require.True(t, ok)
	require.Equal(t, int64(len(data)), rr.Size)
}
