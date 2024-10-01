// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package gcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	kms "cloud.google.com/go/kms/apiv1"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
	_ "google.golang.org/api/impersonate"
)

var gcpKMSTestSettings *cluster.Settings

func init() {
	gcpKMSTestSettings = cluster.MakeTestingClusterSettings()
}

func TestEncryptDecryptGCS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	q := make(url.Values)

	// The KeyID for GCS is the following format:
	// projects/{project name}/locations/{key region}/keyRings/{keyring name}/cryptoKeys/{key name}
	//
	// Get GCS Key identifier from env variable.
	keyID := os.Getenv("GOOGLE_KMS_KEY_NAME")
	if keyID == "" {
		skip.IgnoreLint(t, "GOOGLE_KMS_KEY_NAME env var must be set")
	}

	t.Run("auth-empty-no-cred", func(t *testing.T) {
		// Set AUTH to specified but don't provide CREDENTIALS params.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamSpecified)

		uri := fmt.Sprintf("gs:///%s?%s", keyID, params.Encode())

		_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{ExternalIOConfig: &base.ExternalIODirConfig{}})
		require.EqualError(t, err, fmt.Sprintf(
			"%s or %s must be set if %q is %q",
			CredentialsParam,
			BearerTokenParam,
			cloud.AuthParam,
			cloud.AuthParamSpecified,
		))
	})

	t.Run("auth-implicit", func(t *testing.T) {
		if !cloudtestutils.IsImplicitAuthConfigured() {
			skip.IgnoreLint(t, "implicit auth is not configured")
		}

		// Set the AUTH to implicit.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		uri := fmt.Sprintf("gs:///%s?%s", keyID, params.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         gcpKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})

	t.Run("auth-specified", func(t *testing.T) {
		// Fetch the base64 encoded JSON credentials.
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
		q.Set(CredentialsParam, url.QueryEscape(encoded))

		// Set AUTH to specified.
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         gcpKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})

	t.Run("auth-specified-bearer-token", func(t *testing.T) {
		// Fetch the base64 encoded JSON credentials.
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}

		ctx := context.Background()
		source, err := google.JWTConfigFromJSON([]byte(credentials), kms.DefaultAuthScopes()...)
		require.NoError(t, err, "creating GCS oauth token source from specified credentials")
		ts := source.TokenSource(ctx)

		token, err := ts.Token()
		require.NoError(t, err, "getting token")
		q.Set(BearerTokenParam, token.AccessToken)

		// Set AUTH to specified.
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         gcpKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})
}

func TestKMSAssumeRoleGCP(t *testing.T) {
	envVars := []string{
		"GOOGLE_CREDENTIALS_JSON",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"ASSUME_SERVICE_ACCOUNT",
		"GOOGLE_LIMITED_KEY_ID",
	}
	for _, env := range envVars {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
	}

	keyID := os.Getenv("GOOGLE_LIMITED_KEY_ID")
	assumedAccount := os.Getenv("ASSUME_SERVICE_ACCOUNT")
	encodedCredentials := base64.StdEncoding.EncodeToString([]byte(os.Getenv("GOOGLE_CREDENTIALS_JSON")))

	t.Run("auth-assume-role-implicit", func(t *testing.T) {
		testEnv := &cloud.TestKMSEnv{
			ExternalIOConfig: &base.ExternalIODirConfig{},
			Settings:         gcpKMSTestSettings,
		}
		cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s=%s", keyID, cloud.AuthParam, cloud.AuthParamImplicit), testEnv)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamImplicit)
		q.Set(AssumeRoleParam, assumedAccount)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})

	t.Run("auth-assume-role-specified", func(t *testing.T) {
		testEnv := &cloud.TestKMSEnv{
			ExternalIOConfig: &base.ExternalIODirConfig{},
			Settings:         gcpKMSTestSettings,
		}
		cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s=%s&%s=%s", keyID, cloud.AuthParam,
			cloud.AuthParamSpecified, CredentialsParam, url.QueryEscape(encodedCredentials)), testEnv)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		q.Set(AssumeRoleParam, assumedAccount)
		q.Set(CredentialsParam, encodedCredentials)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})

	t.Run("auth-assume-role-chaining", func(t *testing.T) {
		roleChainStr := os.Getenv("ASSUME_SERVICE_ACCOUNT_CHAIN")
		if roleChainStr == "" {
			skip.IgnoreLint(t, "ASSUME_SERVICE_ACCOUNT_CHAIN env var must be set")
		}
		roleChain := strings.Split(roleChainStr, ",")
		testEnv := &cloud.TestKMSEnv{
			ExternalIOConfig: &base.ExternalIODirConfig{},
			Settings:         gcpKMSTestSettings,
		}

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		q.Set(CredentialsParam, encodedCredentials)

		// First verify that none of the individual roles in the chain can be used
		// to access the KMS.
		for _, role := range roleChain {
			q.Set(AssumeRoleParam, role)
			cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s", keyID, q.Encode()), testEnv)
		}

		q.Set(AssumeRoleParam, roleChainStr)
		cloud.KMSEncryptDecrypt(t, fmt.Sprintf("gs:///%s?%s", keyID, q.Encode()), testEnv)
	})
}

func TestGCSKMSDisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !cloudtestutils.IsImplicitAuthConfigured() {
		skip.IgnoreLint(t, "implicit auth is not configured")
	}

	ctx := context.Background()
	q := make(url.Values)

	// Set AUTH to implicit.
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	keyID := os.Getenv("GOOGLE_KMS_KEY_NAME")
	if keyID == "" {
		skip.IgnoreLint(t, "GOOGLE_KMS_KEY_NAME env var must be set")
	}

	uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
	_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
		Settings:         gcpKMSTestSettings,
		ExternalIOConfig: &base.ExternalIODirConfig{DisableImplicitCredentials: true}})
	require.True(t, testutils.IsError(err,
		"implicit credentials disallowed for gcs due to --external-io-disable-implicit-credentials flag"),
	)
}

func TestGCPKMSInaccessibleError(t *testing.T) {
	envVars := []string{
		"GOOGLE_CREDENTIALS_JSON",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"ASSUME_SERVICE_ACCOUNT_CHAIN",
		"GOOGLE_LIMITED_KEY_ID",
	}
	for _, env := range envVars {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
	}

	ctx := context.Background()
	roleChainStr := os.Getenv("ASSUME_SERVICE_ACCOUNT_CHAIN")
	roleChain := strings.Split(roleChainStr, ",")
	limitedKeyID := os.Getenv("GOOGLE_LIMITED_KEY_ID")

	t.Run("success-sanity-check", func(t *testing.T) {
		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamImplicit)
		q.Set(AssumeRoleParam, roleChainStr)

		uriImplicit := fmt.Sprintf("%s:///%s?%s", gcpScheme, limitedKeyID, q.Encode())
		cloudtestutils.RequireSuccessfulKMS(ctx, t, uriImplicit)

		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		q.Set(CredentialsParam, base64.StdEncoding.EncodeToString([]byte(os.Getenv("GOOGLE_CREDENTIALS_JSON"))))
		uriSpecified := fmt.Sprintf("%s:///%s?%s", gcpScheme, limitedKeyID, q.Encode())
		cloudtestutils.RequireSuccessfulKMS(ctx, t, uriSpecified)
	})

	t.Run("incorrect-credentials", func(t *testing.T) {
		credentialsJSON := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		var credsKeyValues map[string]interface{}
		err := json.Unmarshal([]byte(credentialsJSON), &credsKeyValues)
		require.NoError(t, err)

		credsKeyValues["client_email"] = fmt.Sprintf("%s%s", "non-existent-sa-", credsKeyValues["client_email"])
		badCreds, err := json.Marshal(credsKeyValues)
		require.NoError(t, err)

		encodedCredentials := base64.StdEncoding.EncodeToString(badCreds)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		q.Set(CredentialsParam, encodedCredentials)
		uri := fmt.Sprintf("%s:///%s?%s", gcpScheme, limitedKeyID, q.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "Request had invalid authentication credentials")
	})

	t.Run("incorrect-kms", func(t *testing.T) {
		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamImplicit)
		q.Set(AssumeRoleParam, roleChainStr)
		incorrectKey := limitedKeyID + "-non-existent"

		uri := fmt.Sprintf("%s:///%s?%s", gcpScheme, incorrectKey, q.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "NotFound")
	})

	t.Run("no-kms-permission", func(t *testing.T) {
		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamImplicit)
		uri := fmt.Sprintf("%s:///%s?%s", gcpScheme, limitedKeyID, q.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "PermissionDenied")
	})

	t.Run("cannot-assume-role", func(t *testing.T) {
		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamImplicit)
		q.Set(AssumeRoleParam, roleChain[len(roleChain)-1])

		uri := fmt.Sprintf("%s:///%s?%s", gcpScheme, limitedKeyID, q.Encode())
		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "impersonate: status code 403")
	})
}
