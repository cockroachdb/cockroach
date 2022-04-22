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
	expect := map[string]string{
		"CREDENTIALS":                    CredentialsParam,
		"GOOGLE_APPLICATION_CREDENTIALS": "GOOGLE_APPLICATION_CREDENTIALS",
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	// The KeyID for GCS is the following format:
	// projects/{project name}/locations/{key region}/keyRings/{keyring name}/cryptoKeys/{key name}
	// It can be specified as the following:
	// - GCS_KEY_ID
	// - GCS_KEY_NAME
	for _, id := range []string{"GCS_KEY_ID", "GCS_KEY_NAME"} {
		// Get GCS Key identifier from env variable.
		keyID := os.Getenv(id)
		if keyID == "" {
			skip.IgnoreLint(t, fmt.Sprintf("%s env var must be set", id))
		}

		t.Run(fmt.Sprintf("auth-empty-no-cred-%s", id), func(t *testing.T) {
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

		t.Run(fmt.Sprintf("auth-implicit-%s", id), func(t *testing.T) {
			// Set the AUTH params.
			params := make(url.Values)
			params.Add(cloud.AuthParam, cloud.AuthParamImplicit)

			uri := fmt.Sprintf("gs:///%s?%s", keyID, params.Encode())
			cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
				Settings:         cluster.NoSettings,
				ExternalIOConfig: &base.ExternalIODirConfig{},
			})
		})

		t.Run(fmt.Sprintf("auth-specified-%s", id), func(t *testing.T) {
			// Set AUTH to specified.
			q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
			uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())

			cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
				Settings:         cluster.NoSettings,
				ExternalIOConfig: &base.ExternalIODirConfig{},
			})
		})
	}
}

func TestKMSAssumeRoleGCP(t *testing.T) {
	envVars := []string{
		"CREDENTIALS",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"GCP_TARGET_PRINCIPAL",
		"GCS_LIMITED_KEY_ID",
	}
	for _, env := range envVars {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
	}

	keyID := os.Getenv("GCS_LIMITED_KEY_ID")
	target := os.Getenv("GCP_TARGET_PRINCIPAL")
	credentials := os.Getenv("CREDENTIALS")

	t.Run("auth-assume-role-implicit", func(t *testing.T) {
		testEnv := &cloud.TestKMSEnv{ExternalIOConfig: &base.ExternalIODirConfig{}}
		cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s=%s", keyID, cloud.AuthParam, cloud.AuthParamImplicit), testEnv)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamAssume)
		q.Set(TargetPrincipalParam, target)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})

	t.Run("auth-assume-role-specified", func(t *testing.T) {
		testEnv := &cloud.TestKMSEnv{ExternalIOConfig: &base.ExternalIODirConfig{}}
		cloud.CheckNoKMSAccess(t, fmt.Sprintf("gs:///%s?%s=%s&%s=%s", keyID, cloud.AuthParam,
			cloud.AuthParamSpecified, CredentialsParam, credentials), testEnv)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamAssume)
		q.Set(TargetPrincipalParam, target)
		q.Set(CredentialsParam, credentials)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})
}

func TestGCSKMSDisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	q := make(url.Values)

	// Set AUTH to implicit.
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	for _, id := range []string{"GCS_KEY_ID", "GCS_KEY_NAME"} {
		keyID := os.Getenv(id)
		if keyID == "" {
			skip.IgnoreLint(t, "%s env var must be set", id)
		}
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
			Settings:         cluster.NoSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{DisableImplicitCredentials: true}})
		require.True(t, testutils.IsError(err,
			"implicit credentials disallowed for gcs due to --external-io-implicit-credentials flag"),
		)
	}
}
