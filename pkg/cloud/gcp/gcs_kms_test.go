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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

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
			`%s is set to '%s', but %s is not set`,
			cloud.AuthParam,
			cloud.AuthParamSpecified,
			CredentialsParam,
		))
	})

	t.Run("auth-implicit", func(t *testing.T) {
		if !isImplicitAuthConfigured() {
			skip.IgnoreLint(t, "implicit auth is not configured")
		}

		// Set the AUTH to implicit.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		uri := fmt.Sprintf("gs:///%s?%s", keyID, params.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         cluster.NoSettings,
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
			Settings:         cluster.NoSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})
}

func TestKMSAssumeRoleGCP(t *testing.T) {
	envVars := []string{
		"CREDENTIALS",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"GCP_SERVICE_ACCOUNT",
		"GCS_LIMITED_KEY_ID",
	}
	for _, env := range envVars {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
	}

	keyID := os.Getenv("GCS_LIMITED_KEY_ID")
	serviceAccount := os.Getenv("GCP_SERVICE_ACCOUNT")
	credentials := os.Getenv("CREDENTIALS")

	t.Run("auth-assume-role-implicit", func(t *testing.T) {
		testEnv := &cloud.TestKMSEnv{ExternalIOConfig: &base.ExternalIODirConfig{}}
		cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s=%s", keyID, cloud.AuthParam, cloud.AuthParamImplicit), testEnv)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamAssume)
		q.Set(ServiceAccountParam, serviceAccount)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})

	t.Run("auth-assume-role-specified", func(t *testing.T) {
		testEnv := &cloud.TestKMSEnv{ExternalIOConfig: &base.ExternalIODirConfig{}}
		cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s=%s&%s=%s", keyID, cloud.AuthParam,
			cloud.AuthParamSpecified, CredentialsParam, credentials), testEnv)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamAssume)
		q.Set(ServiceAccountParam, serviceAccount)
		q.Set(CredentialsParam, credentials)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})
}

func TestGCSKMSDisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if !isImplicitAuthConfigured() {
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
		Settings:         cluster.NoSettings,
		ExternalIOConfig: &base.ExternalIODirConfig{DisableImplicitCredentials: true}})
	require.True(t, testutils.IsError(err,
		"implicit credentials disallowed for gcs due to --external-io-implicit-credentials flag"),
	)
}
