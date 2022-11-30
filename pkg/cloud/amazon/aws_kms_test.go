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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	ctx := context.Background()

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
	kmsRegion := os.Getenv("AWS_KMS_REGION")
	if kmsRegion == "" {
		skip.IgnoreLint(t, "AWS_KMS_REGION env var must be set")
	}
	q.Add(KMSRegionParam, kmsRegion)

	// Get AWS Key identifier from env variable.
	keyID := os.Getenv("AWS_KMS_KEY_ARN")
	if keyID == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	t.Run("auth-empty-no-cred", func(t *testing.T) {
		// Set AUTH to specified but don't provide AccessKey params.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamSpecified)
		params.Add(KMSRegionParam, kmsRegion)

		uri := fmt.Sprintf("aws:///%s?%s", keyID, params.Encode())
		_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
			ExternalIOConfig: &base.ExternalIODirConfig{},
			Settings:         awsKMSTestSettings,
		})
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			cloud.AuthParam,
			cloud.AuthParamSpecified,
			AWSAccessKeyParam,
		))
	})

	t.Run("auth-implicit", func(t *testing.T) {
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
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         awsKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})

	t.Run("auth-specified", func(t *testing.T) {
		// Set AUTH to specified.
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		uri := fmt.Sprintf("aws:///%s?%s", keyID, q.Encode())

		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         awsKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})
}

func TestEncryptDecryptAWSAssumeRole(t *testing.T) {
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
		"AWS_ASSUME_ROLE":       AssumeRoleParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}
	// Get AWS KMS region from env variable.
	kmsRegion := os.Getenv("AWS_KMS_REGION")
	if kmsRegion == "" {
		skip.IgnoreLint(t, "AWS_KMS_REGION env var must be set")
	}
	q.Add(KMSRegionParam, kmsRegion)
	q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

	// Get AWS Key identifier from env variable.
	keyID := os.Getenv("AWS_KMS_KEY_ARN")
	if keyID == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	testEnv := &cloud.TestKMSEnv{
		Settings:         awsKMSTestSettings,
		ExternalIOConfig: &base.ExternalIODirConfig{},
	}

	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access AWS KMS
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLint(t, err)
		}

		// Create params for implicit user.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)
		params.Add(AssumeRoleParam, q.Get(AssumeRoleParam))
		params.Add(KMSRegionParam, kmsRegion)

		uri := fmt.Sprintf("aws:///%s?%s", keyID, params.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})

	t.Run("specified", func(t *testing.T) {
		uri := fmt.Sprintf("aws:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})

	t.Run("role-chaining-external-id", func(t *testing.T) {
		roleChainStr := os.Getenv("AWS_ROLE_ARN_CHAIN")
		assumeRoleProvider, delegateRoleProviders := cloud.ParseRoleProvidersString(roleChainStr)
		providerChain := append(delegateRoleProviders, assumeRoleProvider)

		// First verify that none of the individual roles in the chain can be used
		// to access the KMS.
		for _, p := range providerChain {
			q.Set(AssumeRoleParam, p.EncodeAsString())
			roleURI := fmt.Sprintf("aws:///%s?%s", keyID, q.Encode())
			cloud.CheckNoKMSAccess(t, roleURI, testEnv)
		}

		// Next check that the role chain without any external IDs cannot be used to
		// access the KMS.
		roleChainWithoutExternalID := make([]string, 0, len(providerChain))
		for _, rp := range providerChain {
			roleChainWithoutExternalID = append(roleChainWithoutExternalID, rp.Role)
		}
		q.Set(AssumeRoleParam, strings.Join(roleChainWithoutExternalID, ","))
		uri := fmt.Sprintf("aws:///%s?%s", keyID, q.Encode())
		cloud.CheckNoKMSAccess(t, uri, testEnv)

		// Finally, check that the chain of roles can be used to access the KMS.
		q.Set(AssumeRoleParam, roleChainStr)
		uri = fmt.Sprintf("aws:///%s?%s", keyID, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, testEnv)
	})
}

func TestPutAWSKMSEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	q := make(url.Values)
	expectedS3Params := []string{
		AWSAccessKeyParam,
		AWSSecretParam}
	for _, param := range expectedS3Params {
		env := NightlyEnvVarS3Params[param]
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}
	for kmsParam, env := range NightlyEnvVarKMSParams {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(kmsParam, v)
	}
	keyARN := os.Getenv("AWS_KMS_KEY_ARN")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	t.Run("allow-endpoints", func(t *testing.T) {
		uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
		cloud.KMSEncryptDecrypt(t, uri, &cloud.TestKMSEnv{
			Settings:         awsKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{},
		})
	})

	t.Run("disallow-endpoints", func(t *testing.T) {
		uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
		_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
			Settings:         awsKMSTestSettings,
			ExternalIOConfig: &base.ExternalIODirConfig{DisableHTTP: true}})
		require.True(t, testutils.IsError(err, "custom endpoints disallowed"))
	})
}

func TestAWSKMSDisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	q := make(url.Values)
	q.Add(KMSRegionParam, "region")

	// Set AUTH to implicit
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)

	keyARN := os.Getenv("AWS_KMS_KEY_ARN")
	if keyARN == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}
	uri := fmt.Sprintf("aws:///%s?%s", keyARN, q.Encode())
	_, err := cloud.KMSFromURI(ctx, uri, &cloud.TestKMSEnv{
		Settings:         awsKMSTestSettings,
		ExternalIOConfig: &base.ExternalIODirConfig{DisableImplicitCredentials: true}})
	require.True(t, testutils.IsError(err, "implicit credentials disallowed"))
}
