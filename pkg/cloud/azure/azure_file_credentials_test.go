// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestAzureFileCredential(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}

	ctx := context.Background()
	tmpDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	verifyCredentialsAccess := func(cred azcore.TokenCredential) error {
		client, err := service.NewClient(fmt.Sprintf("https://%s.blob.%s", cfg.account, azure.PublicCloud.StorageEndpointSuffix), cred, nil)
		if err != nil {
			return err
		}

		cclient := client.NewContainerClient(cfg.bucket)
		_, err = cclient.GetProperties(ctx, nil)

		return err
	}

	type testCase struct {
		name                  string
		input                 azureCredentialsYAML
		expectedCredentialErr string
		expectedAccessErr     string
	}

	for _, tt := range []testCase{
		{
			name: "valid",
			input: azureCredentialsYAML{
				AzureTenantID:     cfg.tenantID,
				AzureClientID:     cfg.clientID,
				AzureClientSecret: cfg.clientSecret,
			},
		},
		{
			name: "invalid-secret",
			input: azureCredentialsYAML{
				AzureTenantID:     cfg.tenantID,
				AzureClientID:     cfg.clientID,
				AzureClientSecret: cfg.clientSecret + "garbage",
			},
			expectedAccessErr: "Invalid client secret provided",
		},
		{
			name: "missing-field",
			input: azureCredentialsYAML{
				AzureTenantID:     cfg.tenantID,
				AzureClientSecret: cfg.clientSecret,
			},
			expectedCredentialErr: "missing client ID",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			credFile := path.Join(tmpDir, tt.name)

			require.NoError(t, writeAzureCredentialsFile(credFile, tt.input.AzureTenantID, tt.input.AzureClientID, tt.input.AzureClientSecret))

			cred, err := NewAzureFileCredential(credFile, nil)
			if tt.expectedCredentialErr != "" {
				require.ErrorContains(t, err, tt.expectedCredentialErr)
				return
			} else {
				require.NoError(t, err)
			}

			err = verifyCredentialsAccess(cred)
			if tt.expectedAccessErr != "" {
				require.ErrorContains(t, err, tt.expectedAccessErr)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("default-chain-with-file", func(t *testing.T) {
		credFile := path.Join(tmpDir, "default-chain")

		cleanup := envutil.TestSetEnv(t, "AZURE_CLIENT_SECRET", "")
		defer cleanup()

		cleanup2 := envutil.TestSetEnv(t, "COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", credFile)
		defer cleanup2()

		defaultCreds, err := azidentity.NewDefaultAzureCredential(nil)
		require.NoError(t, err)

		require.ErrorContains(t, verifyCredentialsAccess(defaultCreds), "DefaultAzureCredential authentication failed. failed to acquire a token")

		require.NoError(t, writeAzureCredentialsFile(credFile, cfg.tenantID, cfg.clientID, cfg.clientSecret))

		defaultCredsWithFile, err := NewDefaultAzureCredentialWithFile(nil)
		require.NoError(t, err)

		require.NoError(t, verifyCredentialsAccess(defaultCredsWithFile))
	})

	t.Run("default-chain-fallback-on-invalid-file", func(t *testing.T) {
		credFile := path.Join(tmpDir, "default-chain-fallback-on-invalid-file")

		cleanup := envutil.TestSetEnv(t, "COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", credFile)
		defer cleanup()

		require.NoError(t, writeAzureCredentialsFile(credFile, cfg.tenantID, "" /* clientID */, cfg.clientSecret))
		_, err := NewAzureFileCredential(credFile, nil)
		require.ErrorContains(t, err, "missing client ID")

		defaultCredsWithFile, err := NewDefaultAzureCredentialWithFile(nil)
		require.NoError(t, err)

		require.NoError(t, verifyCredentialsAccess(defaultCredsWithFile))
	})

	// Test the file reload behavior of FileCredential when an error is
	// encountered while getting a token. This is to validate that if the
	// store's current credentials are revoked, we will reload the file
	// credentials in the event that they have been updated, and attempt to
	// reacquire another token. If a token is successfully acquired after the
	// reload, there should be no error that bubbles up to the rest of the Azure
	// SDK and storage.
	t.Run("reload-on-error", func(t *testing.T) {
		ioConf := base.ExternalIODirConfig{}
		storeURI := cfg.filePathImplicitAuth("backup-test")

		makeStoreWithErrOnce := func() cloud.ExternalStorage {
			errProduceOnce := sync.Once{}
			knobs := TestingKnobs{
				MapFileCredentialToken: func(token azcore.AccessToken, tErr error) (azcore.AccessToken, error) {
					if tErr == nil {
						errProduceOnce.Do(func() {
							tErr = errors.New("injected test token error")
							token = azcore.AccessToken{}
						})
					}

					return token, tErr
				},
			}

			conf, err := cloud.ExternalStorageConfFromURI(storeURI, username.RootUserName())
			if err != nil {
				t.Fatal(err)
			}

			// Setup a sink for the given args.
			clientFactory := blobs.TestBlobServiceClient("")
			testSettings := cluster.MakeTestingClusterSettings()
			s, err := cloud.MakeExternalStorage(ctx, conf, ioConf, testSettings, clientFactory,
				nil, nil, cloud.NilMetrics, cloud.WithAzureStorageTestingKnobs(&knobs))
			if err != nil {
				t.Fatal(err)
			}
			return s
		}

		t.Run("valid-on-reload", func(t *testing.T) {
			credFile := path.Join(tmpDir, "reload-on-error-valid-credentials")
			cleanup := envutil.TestSetEnv(t, "COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", credFile)
			defer cleanup()

			require.NoError(t, writeAzureCredentialsFile(credFile, cfg.tenantID, cfg.clientID, cfg.clientSecret))

			s := makeStoreWithErrOnce()
			defer s.Close()

			// Despite an error being produced, reloading the file will still
			// yield a valid token for storage access.
			require.NoError(t, s.List(ctx, "/", "", func(f string) error {
				return nil
			}))
		})

		t.Run("invalid-on-reload", func(t *testing.T) {
			credFile := path.Join(tmpDir, "reload-on-error-invalid-on-reload")
			cleanup := envutil.TestSetEnv(t, "COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", credFile)
			defer cleanup()

			require.NoError(t, writeAzureCredentialsFile(credFile, cfg.tenantID, cfg.clientID, cfg.clientSecret))

			s := makeStoreWithErrOnce()
			defer s.Close()

			// Immediately update the file with invalid credentials, which will
			// be expected to cause the store to error when these credentials
			// are loaded.
			require.NoError(t, writeAzureCredentialsFile(credFile, cfg.tenantID, cfg.clientID, cfg.clientSecret+"garbage"))

			err := s.List(ctx, "/", "", func(f string) error {
				return nil
			})
			require.ErrorContains(t, err, "authentication failed")
		})
	})
}

func writeAzureCredentialsFile(
	path string, tenantID string, clientID string, clientSecret string,
) error {
	creds := azureCredentialsYAML{
		AzureTenantID:     tenantID,
		AzureClientID:     clientID,
		AzureClientSecret: clientSecret,
	}

	bytes, err := yaml.Marshal(creds)
	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0600)
}
