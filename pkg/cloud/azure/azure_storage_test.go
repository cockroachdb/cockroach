// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"context"
	"encoding/base64"
	"net/url"
	"os"
	"testing"

	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type azureConfig struct {
	account, key, bucket, environment string
}

func (a azureConfig) filePath(f string) string {
	return a.filePathWithScheme("azure", f)
}

func (a azureConfig) filePathWithScheme(scheme string, f string) string {
	uri := url.URL{Scheme: scheme, Host: a.bucket, Path: f}
	values := uri.Query()
	values.Add(AzureAccountNameParam, a.account)
	values.Add(AzureAccountKeyParam, a.key)
	values.Add(AzureEnvironmentKeyParam, a.environment)
	uri.RawQuery = values.Encode()
	return uri.String()
}

func getAzureConfig() (azureConfig, error) {
	// NB: the Azure Account key must not be url encoded.
	cfg := azureConfig{
		account:     os.Getenv("AZURE_ACCOUNT_NAME"),
		key:         os.Getenv("AZURE_ACCOUNT_KEY"),
		bucket:      os.Getenv("AZURE_CONTAINER"),
		environment: azure.PublicCloud.Name,
	}
	if cfg.account == "" || cfg.key == "" || cfg.bucket == "" {
		return azureConfig{}, errors.New("AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER must all be set")
	}
	if v, ok := os.LookupEnv(AzureEnvironmentKeyParam); ok {
		cfg.environment = v
	}
	return cfg, nil
}
func TestAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}
	testSettings := cluster.MakeTestingClusterSettings()
	cloudtestutils.CheckExportStore(t, cfg.filePath("backup-test"),
		false, username.RootUserName(),
		nil, /* ie */
		nil, /* ief */
		nil, /* kvDB */
		testSettings,
	)
	cloudtestutils.CheckListFiles(t, cfg.filePath("listing-test"), username.RootUserName(),
		nil, /* ie */
		nil, /* ief */
		nil, /* kvDB */
		testSettings,
	)
}

func TestAzureSchemes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
	}
	for _, scheme := range []string{"azure", "azure-storage", "azure-blob"} {
		uri := cfg.filePathWithScheme(scheme, "not-used")
		_, err := cloud.ExternalStorageConfFromURI(uri, username.RootUserName())
		require.NoError(t, err)
	}
}

func TestAntagonisticAzureRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}
	testSettings := cluster.MakeTestingClusterSettings()

	conf, err := cloud.ExternalStorageConfFromURI(
		cfg.filePath("antagonistic-read"), username.RootUserName())
	require.NoError(t, err)

	cloudtestutils.CheckAntagonisticRead(t, conf, testSettings)
}

func TestParseAzureURL(t *testing.T) {
	t.Run("Defaults to Public Cloud when AZURE_ENVIRONEMNT unset", func(t *testing.T) {
		u, err := url.Parse("azure://container/path?AZURE_ACCOUNT_NAME=account&AZURE_ACCOUNT_KEY=key")
		require.NoError(t, err)

		sut, err := parseAzureURL(cloud.ExternalStorageURIContext{}, u)
		require.NoError(t, err)

		require.Equal(t, azure.PublicCloud.Name, sut.AzureConfig.Environment)
	})

	t.Run("Can Override AZURE_ENVIRONMENT", func(t *testing.T) {
		u, err := url.Parse("azure-storage://container/path?AZURE_ACCOUNT_NAME=account&AZURE_ACCOUNT_KEY=key&AZURE_ENVIRONMENT=AzureUSGovernmentCloud")
		require.NoError(t, err)

		sut, err := parseAzureURL(cloud.ExternalStorageURIContext{}, u)
		require.NoError(t, err)

		require.Equal(t, azure.USGovernmentCloud.Name, sut.AzureConfig.Environment)
	})
}

func TestMakeAzureStorageURLFromEnvironment(t *testing.T) {
	for _, tt := range []struct {
		environment string
		expected    string
	}{
		{environment: azure.PublicCloud.Name, expected: "https://account.blob.core.windows.net/container"},
		{environment: azure.USGovernmentCloud.Name, expected: "https://account.blob.core.usgovcloudapi.net/container"},
	} {
		t.Run(tt.environment, func(t *testing.T) {
			sut, err := makeAzureStorage(context.Background(), cloud.ExternalStorageContext{}, cloudpb.ExternalStorage{
				AzureConfig: &cloudpb.ExternalStorage_Azure{
					Container:   "container",
					Prefix:      "path",
					AccountName: "account",
					AccountKey:  base64.StdEncoding.EncodeToString([]byte("key")),
					Environment: tt.environment,
				},
			})

			require.NoError(t, err)
			require.Equal(t, tt.expected, sut.(*azureStorage).container.URL())
		})
	}
}
