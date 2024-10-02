// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package azure

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var azureKMSTestSettings *cluster.Settings

func init() {
	azureKMSTestSettings = cluster.MakeTestingClusterSettings()
}

type azureKMSConfig struct {
	keyName, keyVersion, clientID, clientSecret, tenantID, vaultName, limitedVaultName, environment string
}

func getAzureKMSConfig() (azureKMSConfig, error) {
	cfg := azureKMSConfig{
		keyName:          os.Getenv("AZURE_KMS_KEY_NAME"),
		keyVersion:       os.Getenv("AZURE_KMS_KEY_VERSION"),
		clientID:         os.Getenv("AZURE_CLIENT_ID"),
		clientSecret:     os.Getenv("AZURE_CLIENT_SECRET"),
		tenantID:         os.Getenv("AZURE_TENANT_ID"),
		vaultName:        os.Getenv("AZURE_VAULT_NAME"),
		limitedVaultName: os.Getenv("AZURE_LIMITED_VAULT_NAME"),
		environment:      azure.PublicCloud.Name,
	}

	if cfg.keyName == "" || cfg.keyVersion == "" || cfg.clientID == "" || cfg.clientSecret == "" || cfg.tenantID == "" {
		return azureKMSConfig{}, errors.New(
			"AZURE_KMS_KEY_NAME, AZURE_KMS_KEY_VERSION, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID must all be set")
	}
	if v, ok := os.LookupEnv(AzureEnvironmentKeyParam); ok {
		cfg.environment = v
	}
	return cfg, nil
}

func TestEncryptDecryptAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	cfg, err := getAzureKMSConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}
	params := make(url.Values)
	params.Add(AzureEnvironmentKeyParam, cfg.environment)
	params.Add(AzureClientIDParam, cfg.clientID)
	params.Add(AzureClientSecretParam, cfg.clientSecret)
	params.Add(AzureTenantIDParam, cfg.tenantID)
	params.Add(AzureVaultName, cfg.vaultName)

	t.Run("fails without credentials", func(t *testing.T) {
		redactedParams := make(url.Values)
		for k, v := range params {
			redactedParams[k] = v
		}
		redactedParams.Del(AzureClientSecretParam)

		uri := fmt.Sprintf("azure-kms:///%s/%s?%s", cfg.keyName, cfg.keyVersion, redactedParams.Encode())

		_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{ExternalIOConfig: &base.ExternalIODirConfig{}})
		require.Error(t, err)
	})

	t.Run("explicit auth", func(t *testing.T) {
		uri := fmt.Sprintf("azure-kms:///%s/%s?%s", cfg.keyName, cfg.keyVersion, params.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         azureKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})

	t.Run("implicit auth", func(t *testing.T) {
		redactedParams := make(url.Values)
		for k, v := range params {
			redactedParams[k] = v
		}
		redactedParams.Del(AzureClientIDParam)
		redactedParams.Del(AzureClientSecretParam)
		redactedParams.Del(AzureTenantIDParam)
		redactedParams.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		uri := fmt.Sprintf("azure-kms:///%s/%s?%s", cfg.keyName, cfg.keyVersion, redactedParams.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         azureKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})

	t.Run("implicit file auth", func(t *testing.T) {
		redactedParams := make(url.Values)
		for k, v := range params {
			redactedParams[k] = v
		}
		redactedParams.Del(AzureClientIDParam)
		redactedParams.Del(AzureClientSecretParam)
		redactedParams.Del(AzureTenantIDParam)
		redactedParams.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		kmsEnv := &cloud.TestKMSEnv{
			Settings:         azureKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		}

		cleanup := envutil.TestSetEnv(t, "AZURE_CLIENT_ID", "")
		defer cleanup()

		uri := fmt.Sprintf("azure-kms:///%s/%s?%s", cfg.keyName, cfg.keyVersion, redactedParams.Encode())
		cloud.CheckNoKMSAccess(t, uri, kmsEnv)

		tmpDir, cleanup2 := testutils.TempDir(t)
		defer cleanup2()

		credFile := path.Join(tmpDir, "credentials.json")
		require.NoError(t, writeAzureCredentialsFile(credFile, cfg.tenantID, cfg.clientID, cfg.clientSecret))

		cleanup3 := envutil.TestSetEnv(t, "COCKROACH_AZURE_APPLICATION_CREDENTIALS_FILE", credFile)
		defer cleanup3()

		cloud.KMSEncryptDecrypt(t, uri, kmsEnv)
	})
}

func TestAzureKMSInaccessibleError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	cfg, err := getAzureKMSConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}
	q := make(url.Values)
	q.Add(AzureEnvironmentKeyParam, cfg.environment)
	q.Add(AzureClientIDParam, cfg.clientID)
	q.Add(AzureClientSecretParam, cfg.clientSecret)
	q.Add(AzureTenantIDParam, cfg.tenantID)
	q.Add(AzureVaultName, cfg.vaultName)

	t.Run("success-sanity-check", func(t *testing.T) {
		uri := fmt.Sprintf("%s:///%s/%s?%s", kmsScheme, cfg.keyName, cfg.keyVersion, q.Encode())
		cloudtestutils.RequireSuccessfulKMS(ctx, t, uri)
	})

	t.Run("incorrect-credentials", func(t *testing.T) {
		q2 := make(url.Values)
		for k, v := range q {
			q2[k] = v
		}
		q2.Set(AzureClientSecretParam, q.Get(AzureClientSecretParam)+"garbage")
		uri := fmt.Sprintf("%s:///%s/%s?%s", kmsScheme, cfg.keyName, cfg.keyVersion, q2.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "ClientSecretCredential authentication failed")
	})

	t.Run("incorrect-kms", func(t *testing.T) {
		incorrectKey := cfg.keyName + "-non-existent"
		uri := fmt.Sprintf("%s:///%s/%s?%s", kmsScheme, incorrectKey, cfg.keyVersion, q.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "KeyNotFound")
	})

	t.Run("no-kms-permission", func(t *testing.T) {
		q2 := make(url.Values)
		for k, v := range q {
			q2[k] = v
		}
		q2.Set(AzureVaultName, cfg.limitedVaultName)
		uri := fmt.Sprintf("%s:///%s/%s?%s", kmsScheme, "somekey", "00000000000000000000000000000000", q2.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "not authorized to perform action on resource")
	})

}
