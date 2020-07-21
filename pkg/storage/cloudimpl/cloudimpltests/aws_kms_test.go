// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cloudimpltests

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
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
		"AWS_ACCESS_KEY_ID":     cloudimpl.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": cloudimpl.AWSSecretParam,
		"AWS_REGION":            cloudimpl.KMSRegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	// Get AWS Key ARN from env variable.
	// TODO(adityamaru): Check if there is a way to specify this in the default
	// role and if we can derive it from there instead?
	keyARN := os.Getenv("AWS_KEY_ARN")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KEY_ARN env var must be set")
	}

	t.Run("auth-empty-no-cred", func(t *testing.T) {
		// Set AUTH to specified but don't provide AccessKey params.
		params := make(url.Values)
		params.Add(cloudimpl.AuthParam, cloudimpl.AuthParamSpecified)

		uri := fmt.Sprintf("aws:///%s?%s", keyARN, params.Encode())
		_, err := cloud.KMSFromURI(uri, &testKMSEnv{})
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			cloudimpl.AuthParam,
			cloudimpl.AuthParamSpecified,
			cloudimpl.AWSAccessKeyParam,
		))
	})

	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access AWS KMS
		// in the AWS console, then set it up locally.
		// https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLint(t, err)
		}

		// Set AUTH to implicit
		q.Set(cloudimpl.AuthParam, cloudimpl.AuthParamImplicit)

		uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
		testEncryptDecrypt(t, uri, testKMSEnv{
			cluster.NoSettings, &base.ExternalIODirConfig{},
		})
	})

	t.Run("auth-specified", func(t *testing.T) {
		// Set AUTH to specified.
		q.Set(cloudimpl.AuthParam, cloudimpl.AuthParamSpecified)
		uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())

		testEncryptDecrypt(t, uri, testKMSEnv{
			cluster.NoSettings, &base.ExternalIODirConfig{},
		})
	})
}

func TestPutAWSKMSEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_KMS_ENDPOINT":        cloudimpl.AWSEndpointParam,
		"AWS_KMS_ENDPOINT_KEY":    cloudimpl.AWSAccessKeyParam,
		"AWS_KMS_ENDPOINT_REGION": cloudimpl.KMSRegionParam,
		"AWS_KMS_ENDPOINT_SECRET": cloudimpl.AWSSecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	keyARN := os.Getenv("AWS_KMS_KEY_ARN")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
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
	q.Add("AWS_REGION", cloudimpl.KMSRegionParam)

	// Set AUTH to implicit
	q.Add(cloudimpl.AuthParam, cloudimpl.AuthParamImplicit)

	keyARN := os.Getenv("AWS_KMS_KEY_ARN")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}
	uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
	_, err := cloud.KMSFromURI(uri, &testKMSEnv{cluster.NoSettings,
		&base.ExternalIODirConfig{DisableImplicitCredentials: true}})
	require.True(t, testutils.IsError(err, "implicit credentials disallowed"))
}
