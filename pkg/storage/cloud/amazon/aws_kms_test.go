// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package amazon

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var awsKMSTestSettings *cluster.Settings

func init() {
	awsKMSTestSettings = cluster.MakeTestingClusterSettings()
}

func TestEncryptDecryptAWS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all AWS KMS tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "Test only works with AWS credentials")
	}

	q := make(url.Values)
	expect := map[string]string{
		"AWS_ACCESS_KEY_ID":     AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": AWSSecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	// Get AWS KMS region from env variable.
	kmsRegion := os.Getenv("AWS_KMS_REGION_A")
	if kmsRegion == "" {
		skip.IgnoreLint(t, "AWS_KMS_REGION_A env var must be set")
	}
	q.Add(KMSRegionParam, kmsRegion)

	// The KeyID for AWS can be specified as any of the following:
	// - AWS_KEY_ARN
	// - AWS_KEY_ID
	// - AWS_KEY_ALIAS
	for _, id := range []string{"AWS_KMS_KEY_ARN_A", "AWS_KEY_ID", "AWS_KEY_ALIAS"} {
		// Get AWS Key identifier from env variable.
		keyID := os.Getenv(id)
		if keyID == "" {
			skip.IgnoreLint(t, fmt.Sprintf("%s env var must be set", id))
		}

		t.Run(fmt.Sprintf("auth-empty-no-cred-%s", id), func(t *testing.T) {
			// Set AUTH to specified but don't provide AccessKey params.
			params := make(url.Values)
			params.Add(cloud.AuthParam, cloud.AuthParamSpecified)
			params.Add(KMSRegionParam, kmsRegion)

			uri := fmt.Sprintf("aws:///%s?%s", keyID, params.Encode())
			_, err := cloud.KMSFromURI(uri, &testKMSEnv{})
			require.EqualError(t, err, fmt.Sprintf(
				`%s is set to '%s', but %s is not set`,
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSAccessKeyParam,
			))
		})

		t.Run(fmt.Sprintf("auth-implicit-%s", id), func(t *testing.T) {
			// You can create an IAM that can access AWS KMS
			// in the AWS console, then set it up locally.
			// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
			// We only run this test if default role exists.
			credentialsProvider := credentials.SharedCredentialsProvider{}
			_, err := credentialsProvider.Retrieve()
			if err != nil {
				skip.IgnoreLint(t, err)
			}

			// Set the AUTH and REGION params.
			params := make(url.Values)
			params.Add(cloud.AuthParam, cloud.AuthParamImplicit)
			params.Add(KMSRegionParam, kmsRegion)

			uri := fmt.Sprintf("aws:///%s?%s", keyID, params.Encode())
			testEncryptDecrypt(t, uri, testKMSEnv{
				cluster.NoSettings, &base.ExternalIODirConfig{},
			})
		})

		t.Run(fmt.Sprintf("auth-specified-%s", id), func(t *testing.T) {
			// Set AUTH to specified.
			q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
			uri := fmt.Sprintf("aws:///%s?%s", keyID, q.Encode())

			testEncryptDecrypt(t, uri, testKMSEnv{
				cluster.NoSettings, &base.ExternalIODirConfig{},
			})
		})
	}
}

func TestPutAWSKMSEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_KMS_ENDPOINT":        AWSEndpointParam,
		"AWS_KMS_ENDPOINT_KEY":    AWSAccessKeyParam,
		"AWS_KMS_ENDPOINT_REGION": KMSRegionParam,
		"AWS_KMS_ENDPOINT_SECRET": AWSSecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	keyARN := os.Getenv("AWS_KMS_KEY_ARN_A")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN_A env var must be set")
	}

	t.Run("allow-endpoints", func(t *testing.T) {
		uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
		testEncryptDecrypt(t, uri, testKMSEnv{
			awsKMSTestSettings, &base.ExternalIODirConfig{},
		})
	})

	t.Run("disallow-endpoints", func(t *testing.T) {
		uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
		_, err := cloud.KMSFromURI(uri, &testKMSEnv{awsKMSTestSettings,
			&base.ExternalIODirConfig{DisableHTTP: true}})
		require.True(t, testutils.IsError(err, "custom endpoints disallowed"))
	})
}

func TestAWSKMSDisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	q := make(url.Values)
	q.Add(KMSRegionParam, "region")

	// Set AUTH to implicit
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)

	keyARN := os.Getenv("AWS_KMS_KEY_ARN_A")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN_A env var must be set")
	}
	uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
	_, err := cloud.KMSFromURI(uri, &testKMSEnv{cluster.NoSettings,
		&base.ExternalIODirConfig{DisableImplicitCredentials: true}})
	require.True(t, testutils.IsError(err, "implicit credentials disallowed"))
}
