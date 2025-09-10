// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudimpltests

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	azurecloud "github.com/Azure/go-autorest/autorest/azure"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/azure"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

//go:generate ../../../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestExternalStorageWriteAndReadRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test ensures that the read and write behavior of all external storage
	// providers is consistent. In particular, we test that when reading from a
	// file, we only get EOF once all bytes of the file have been read.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	testcases := []struct {
		name string
		uri  func(prefix string) (uri string, skipErr error)
	}{
		{
			name: "s3",
			uri: func(prefix string) (string, error) {
				envConfig, err := config.NewEnvConfig()
				require.NoError(t, err)
				if !envConfig.Credentials.HasKeys() {
					return "", errors.New("no AWS credentials")
				}
				baseBucket := os.Getenv("AWS_S3_BUCKET")
				if baseBucket == "" {
					return "", errors.New("AWS_S3_BUCKET env var must be set")
				}
				return amazon.S3URI(
					baseBucket, prefix, &cloudpb.ExternalStorage_S3{
						AccessKey: envConfig.Credentials.AccessKeyID,
						Secret:    envConfig.Credentials.SecretAccessKey,
						Region:    "us-east-1",
					},
				), nil
			},
		},
		{
			name: "gcs",
			uri: func(prefix string) (string, error) {
				bucket := os.Getenv("GCS_BUCKET")
				if bucket == "" {
					return "", errors.New("GCS_BUCKET env var must be set")
				}

				if !cloudtestutils.IsImplicitAuthConfigured() {
					return "", errors.New("implicit auth is not configured")
				}

				return fmt.Sprintf("gs://%s/%s?AUTH=implicit", bucket, prefix), nil
			},
		},
		{
			name: "azure",
			uri: func(prefix string) (string, error) {
				conf := &cloudpb.ExternalStorage_Azure{
					AccountName:  os.Getenv("AZURE_ACCOUNT_NAME"),
					AccountKey:   os.Getenv("AZURE_ACCOUNT_KEY"),
					Container:    os.Getenv("AZURE_CONTAINER"),
					ClientID:     os.Getenv("AZURE_CLIENT_ID"),
					TenantID:     os.Getenv("AZURE_TENANT_ID"),
					ClientSecret: os.Getenv("AZURE_CLIENT_SECRET"),
					Environment:  azurecloud.PublicCloud.Name,
				}
				if conf.AccountName == "" || conf.AccountKey == "" || conf.Container == "" ||
					conf.ClientID == "" || conf.ClientSecret == "" || conf.TenantID == "" {
					return "", errors.New(
						"AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID must all be set")
				}
				return azure.AzureURI(prefix, conf), nil
			},
		},
		{
			name: "nodelocal",
			uri: func(prefix string) (string, error) {
				require.NoError(t, os.Mkdir(path.Join(dir, prefix), 0o755))
				return nodelocal.MakeLocalStorageURI(prefix), nil
			},
		},
	}

	const filename = "testfile"
	const size = 1 << 10 // 1 KB
	prefix := fmt.Sprintf("%s-%d", t.Name(), cloudtestutils.NewTestID())
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			uri, skipErr := tc.uri(prefix)
			if skipErr != nil {
				t.Skipf("skipping test: %v", skipErr)
			}
			t.Logf("testing with uri %s", uri)
			store, err := cloud.ExternalStorageFromURI(
				ctx,
				uri,
				base.ExternalIODirConfig{},
				st,
				blobs.TestBlobServiceClient(dir),
				username.RootUserName(),
				nil, /* db */
				nil, /* limiters */
				cloud.NilMetrics,
			)
			require.NoError(t, err)
			defer store.Close()

			writer, err := store.Writer(ctx, filename)
			require.NoError(t, err)

			bytesWritten, err := writer.Write(data)
			require.NoError(t, err)
			require.Equal(t, size, bytesWritten)

			// Make sure we close before reading to ensure file is flushed.
			require.NoError(t, writer.Close())

			reader, readSize, err := store.ReadFile(ctx, filename, cloud.ReadOptions{})
			require.NoError(t, err)
			defer reader.Close(ctx)
			require.Equal(t, size, int(readSize))

			readData := make([]byte, size)
			bytesRead, err := reader.Read(ctx, readData)
			require.NoError(t, err)
			require.Equal(t, size, bytesRead)
			require.Equal(t, data, readData)

			subseqBytesRead, err := reader.Read(ctx, readData)
			require.Equal(t, 0, subseqBytesRead)
			require.ErrorIs(t, err, io.EOF)
		})
	}
}
