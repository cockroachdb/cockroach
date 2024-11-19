// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package amazon

import (
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/settings"
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
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
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
		ctx := context.Background()
		cfg, err := config.LoadDefaultConfig(ctx)
		require.NoError(t, err)
		_, err = cfg.Credentials.Retrieve(ctx)
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
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
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
		ctx := context.Background()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(config.DefaultSharedConfigProfile))
		require.NoError(t, err)
		_, err = cfg.Credentials.Retrieve(ctx)
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

func TestKMSAgainstMockAWS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Setup a bogus credentials file so it doesn't try to use a metadata server.
	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	credFile := filepath.Join(tempDir, "credentials")
	require.NoError(t, os.Setenv("AWS_SHARED_CREDENTIALS_FILE", credFile))
	defer func() {
		require.NoError(t, os.Unsetenv("AWS_SHARED_CREDENTIALS_FILE"))
	}()
	require.NoError(t, os.WriteFile(credFile, []byte(`[default]
		aws_access_key_id = abc
		aws_secret_access_key = xyz
		`), 0644))

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		defer r.Body.Close()
		// Default to replying with static placeholder "ciphertext", unless req is
		// a decrypt req, in which case send static "decrypted" plaintext resp.
		resp := `{"CiphertextBlob": "dW51c2Vk"}` // "unused".
		if strings.Contains(string(body), "Ciphertext") {
			resp = `{"Plaintext": "aGVsbG8gd29ybGQ="}` // base64 for 'hello world'
		}
		_, err = w.Write([]byte(resp))
		require.NoError(t, err)
	}))
	defer srv.Close()

	tEnv := &cloud.TestKMSEnv{Settings: cluster.MakeTestingClusterSettings(), ExternalIOConfig: &base.ExternalIODirConfig{}}

	// Set the custom CA so testserver is trusted, and defer reset of it.
	u := tEnv.Settings.MakeUpdater()
	require.NoError(t, u.Set(ctx, "cloudstorage.http.custom_ca", settings.EncodedValue{
		Type: "s", Value: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})),
	}))

	t.Run("implicit", func(t *testing.T) {
		q := url.Values{
			KMSRegionParam: []string{"r"}, AWSEndpointParam: []string{srv.URL},
			cloud.AuthParam: []string{"implicit"},
		}
		cloud.KMSEncryptDecrypt(t, fmt.Sprintf("aws:///arn?%s", q.Encode()), tEnv)
	})

	t.Run("specified", func(t *testing.T) {
		q := url.Values{
			KMSRegionParam: []string{"r"}, AWSEndpointParam: []string{srv.URL},
			AWSAccessKeyParam: []string{"k"}, AWSSecretParam: []string{"s"},
		}
		cloud.KMSEncryptDecrypt(t, fmt.Sprintf("aws:///arn?%s", q.Encode()), tEnv)
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

func TestAWSKMSInaccessibleError(t *testing.T) {
	q := make(url.Values)
	expect := map[string]string{
		"AWS_ACCESS_KEY_ID":     AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": AWSSecretParam,
		"AWS_KMS_REGION":        KMSRegionParam,
	}

	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

	// Get AWS Key identifier from env variable.
	keyID := os.Getenv("AWS_KMS_KEY_ARN")
	if keyID == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	ctx := context.Background()
	roleChainStr := os.Getenv("AWS_ROLE_ARN_CHAIN")
	if roleChainStr == "" {
		skip.IgnoreLint(t, "AWS_ROLE_ARN_CHAIN env var must be set")
	}

	roleChain := strings.Split(roleChainStr, ",")

	t.Run("success-sanity-check", func(t *testing.T) {
		uri := fmt.Sprintf("%s:///%s?%s", awsKMSScheme, keyID, q.Encode())
		cloudtestutils.RequireSuccessfulKMS(ctx, t, uri)
	})

	t.Run("incorrect-credentials", func(t *testing.T) {
		q2 := make(url.Values)
		for k, v := range q {
			q2[k] = v
		}
		q2.Set(AWSSecretParam, q.Get(AWSSecretParam)+"garbage")
		uri := fmt.Sprintf("%s:///%s?%s", awsKMSScheme, keyID, q2.Encode())

		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "StatusCode: 400")
	})

	t.Run("incorrect-kms", func(t *testing.T) {
		incorrectKey := keyID + "-non-existent"
		uri := fmt.Sprintf("%s:///%s?%s", awsKMSScheme, incorrectKey, q.Encode())
		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "(NotFound|InvalidCiphertext)")
	})

	t.Run("no-kms-permission", func(t *testing.T) {
		q2 := make(url.Values)
		for k, v := range q {
			q2[k] = v
		}
		q2.Set(AssumeRoleParam, roleChain[0])

		uri := fmt.Sprintf("%s:///%s?%s", awsKMSScheme, keyID, q2.Encode())
		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "(not authorized to perform: kms:Encrypt|InvalidCiphertext)")
	})

	t.Run("cannot-assume-role", func(t *testing.T) {
		q2 := make(url.Values)
		for k, v := range q {
			q2[k] = v
		}
		q2.Set(AssumeRoleParam, roleChain[len(roleChain)-1])

		uri := fmt.Sprintf("%s:///%s?%s", awsKMSScheme, keyID, q2.Encode())
		cloudtestutils.RequireKMSInaccessibleErrorContaining(ctx, t, uri, "(not authorized to perform: sts:AssumeRole|InvalidCiphertext)")
	})

}

func TestAWSKMSImplicitAuthRequiresNoSharedConfigFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Tests if the AWS KMS client can be created when the shared config files
	// are not present, but are present through the rest of the credential/config
	// chain as specified in https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-authentication.html#cli-chap-authentication-precedence

	// If either KMS region or key ARN is not set, we can assume we are not
	// running in the nightly test environment and can skip.
	kmsRegion := os.Getenv("AWS_KMS_REGION")
	if kmsRegion == "" {
		skip.IgnoreLint(t, "AWS_KMS_REGION env var must be set")
	}
	keyID := os.Getenv("AWS_KMS_KEY_ARN")
	if keyID == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	// Setup a bogus credentials/configs file so that we can simulate a non-existent
	// shared config file. The test environment is already setup with the
	// default config/credential files at ~/.aws/config and ~/.aws/credentials,
	// so in order to circumvent this and test the case where the shared config
	// files are not present, we need to set the AWS_CONFIG_FILE and
	// AWS_SHARED_CREDENTIALS_FILE.
	require.NoError(t, os.Setenv("AWS_CONFIG_FILE", "/tmp/config"))
	require.NoError(t, os.Setenv("AWS_SHARED_CREDENTIALS_FILE",
		"/tmp/credentials"))
	defer func() {
		require.NoError(t, os.Unsetenv("AWS_CONFIG_FILE"))
		require.NoError(t, os.Unsetenv("AWS_SHARED_CREDENTIALS_FILE"))
	}()

	testEnv := &cloud.TestKMSEnv{
		Settings:         awsKMSTestSettings,
		ExternalIOConfig: &base.ExternalIODirConfig{},
	}

	params := make(url.Values)
	params.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	params.Add(KMSRegionParam, kmsRegion)

	uri := fmt.Sprintf("aws:///%s?%s", keyID, params.Encode())
	_, err := MakeAWSKMS(context.Background(), uri, testEnv)
	require.NoError(t, err)
}
