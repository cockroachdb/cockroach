// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package amazon

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/smithy-go"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeS3Storage(
	ctx context.Context, uri string, user username.SQLUsername,
) (cloud.ExternalStorage, error) {
	conf, err := cloud.ExternalStorageConfFromURI(uri, user)
	if err != nil {
		return nil, err
	}
	testSettings := cluster.MakeTestingClusterSettings()

	// Setup a sink for the given args.
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		blobs.TestEmptyBlobClientFactory,
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// You can create an IAM that can access S3 in the AWS console, then
// set it up locally.
// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
// We only run the calling test if default role exists.
func skipIfNoDefaultConfig(t *testing.T, ctx context.Context) {
	t.Helper()
	const helpMsg = "we only run this test if a default role exists, " +
		"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html"
	cfg, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err)
	_, err = cfg.Credentials.Retrieve(ctx)
	if err != nil {
		skip.IgnoreLintf(t, "%s: %s", helpMsg, err)
	}
}

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}
	envCreds := envConfig.Credentials
	baseBucket := os.Getenv("AWS_S3_BUCKET")
	if baseBucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testSettings := cluster.MakeTestingClusterSettings()

	ctx := context.Background()
	user := username.RootUserName()
	testID := cloudtestutils.NewTestID()
	for _, locked := range []bool{true, false} {
		bucket := baseBucket
		testName := "regular-bucket"
		if locked {
			testName = "object-locked-bucket"
			bucket += "-locked"
		}
		t.Run(testName, func(t *testing.T) {
			t.Run("auth-empty-no-cred", func(t *testing.T) {
				uri := fmt.Sprintf("s3://%s/%s-%d", bucket, "backup-test-default", testID)
				_, err := cloud.ExternalStorageFromURI(
					ctx, uri, base.ExternalIODirConfig{}, testSettings, blobs.TestEmptyBlobClientFactory, user,
					nil /* ie */, nil /* ief */, nil /* kvDB */, nil /* limiters */, nil, /* metrics */
				)
				require.EqualError(t, err, fmt.Sprintf(
					`%s is set to '%s', but %s is not set`,
					cloud.AuthParam,
					cloud.AuthParamSpecified,
					AWSAccessKeyParam,
				))
			})
			t.Run("auth-implicit", func(t *testing.T) {
				skipIfNoDefaultConfig(t, ctx)
				info := cloudtestutils.StoreInfo{
					URI: fmt.Sprintf(
						"s3://%s/%s-%d?%s=%s",
						bucket, "backup-test-default", testID,
						cloud.AuthParam, cloud.AuthParamImplicit,
					),
					User: user,
				}
				cloudtestutils.CheckExportStore(t, info)
			})
			t.Run("auth-specified", func(t *testing.T) {
				info := cloudtestutils.StoreInfo{
					URI: S3URI(
						bucket, fmt.Sprintf("backup-test-%d", testID),
						&cloudpb.ExternalStorage_S3{AccessKey: envCreds.AccessKeyID, Secret: envCreds.SecretAccessKey, Region: "us-east-1"},
					),
					User: user,
				}
				cloudtestutils.CheckExportStore(t, info)
				cloudtestutils.CheckListFiles(t, info)
			})

			// Tests that we can put an object with server side encryption specified.
			t.Run("server-side-encryption", func(t *testing.T) {
				skipIfNoDefaultConfig(t, ctx)
				info := cloudtestutils.StoreInfo{
					URI: fmt.Sprintf(
						"s3://%s/%s-%d?%s=%s&%s=%s",
						bucket, "backup-test-sse-256", testID,
						cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
						"AES256",
					),
					User: user,
				}
				cloudtestutils.CheckExportStore(t, info)
				v := os.Getenv("AWS_KMS_KEY_ARN")
				if v == "" {
					skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
				}
				info.URI = fmt.Sprintf(
					"s3://%s/%s-%d?%s=%s&%s=%s&%s=%s",
					bucket, "backup-test-sse-kms", testID,
					cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
					"aws:kms", AWSServerSideEncryptionKMSID, v,
				)
				cloudtestutils.CheckExportStore(t, info)
			})

			t.Run("server-side-encryption-invalid-params", func(t *testing.T) {
				skipIfNoDefaultConfig(t, ctx)
				// Unsupported server side encryption option.
				invalidSSEModeURI := fmt.Sprintf(
					"s3://%s/%s?%s=%s&%s=%s",
					bucket, "backup-test-sse-256",
					cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
					"unsupported-algorithm")

				_, err = makeS3Storage(ctx, invalidSSEModeURI, user)
				require.True(t, testutils.IsError(err, "unsupported server encryption mode unsupported-algorithm. Supported values are `aws:kms` and `AES256"))

				// Specify aws:kms encryption mode but don't specify kms ID.
				invalidKMSURI := fmt.Sprintf(
					"s3://%s/%s?%s=%s&%s=%s",
					bucket, "backup-test-sse-256",
					cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
					"aws:kms")
				_, err = makeS3Storage(ctx, invalidKMSURI, user)
				require.True(t, testutils.IsError(err, "AWS_SERVER_KMS_ID param must be set when using aws:kms server side encryption mode."))
			})
		})
	}
}

func TestPutS3AssumeRole(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}
	creds := envConfig.Credentials
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testID := cloudtestutils.NewTestID()
	testPath := fmt.Sprintf("backup-test-%d", testID)

	user := username.RootUserName()

	roleArn := os.Getenv("AWS_ASSUME_ROLE")
	if roleArn == "" {
		skip.IgnoreLint(t, "AWS_ASSUME_ROLE env var must be set")
	}
	ctx := context.Background()
	t.Run("auth-implicit", func(t *testing.T) {
		skipIfNoDefaultConfig(t, ctx)
		info := cloudtestutils.StoreInfo{
			URI: S3URI(bucket, testPath,
				&cloudpb.ExternalStorage_S3{Auth: cloud.AuthParamImplicit, RoleARN: roleArn, Region: "us-east-1"},
			),
			User: user,
		}
		cloudtestutils.CheckExportStore(t, info)
		cloudtestutils.CheckListFiles(t, info)
	})

	t.Run("auth-specified", func(t *testing.T) {
		info := cloudtestutils.StoreInfo{
			URI: S3URI(bucket, testPath,
				&cloudpb.ExternalStorage_S3{Auth: cloud.AuthParamSpecified, RoleARN: roleArn, AccessKey: creds.AccessKeyID, Secret: creds.SecretAccessKey, Region: "us-east-1"},
			),
			User: user,
		}
		cloudtestutils.CheckExportStore(t, info)
		cloudtestutils.CheckListFiles(t, info)
	})

	t.Run("role-chaining-external-id", func(t *testing.T) {
		roleChainStr := os.Getenv("AWS_ROLE_ARN_CHAIN")
		if roleChainStr == "" {
			skip.IgnoreLint(t, "AWS_ROLE_ARN_CHAIN env var must be set")
		}

		assumeRoleProvider, delegateRoleProviders := cloud.ParseRoleProvidersString(roleChainStr)
		providerChain := append(delegateRoleProviders, assumeRoleProvider)
		for _, tc := range []struct {
			auth      string
			accessKey string
			secretKey string
		}{
			{cloud.AuthParamSpecified, creds.AccessKeyID, creds.SecretAccessKey},
			{cloud.AuthParamImplicit, "", ""},
		} {
			t.Run(tc.auth, func(t *testing.T) {
				// First verify that none of the individual roles in the chain can be used to access the storage.
				for _, p := range providerChain {
					info := cloudtestutils.StoreInfo{
						URI: S3URI(bucket, testPath,
							&cloudpb.ExternalStorage_S3{
								Auth:               tc.auth,
								AssumeRoleProvider: p,
								AccessKey:          tc.accessKey,
								Secret:             tc.secretKey,
								Region:             "us-east-1",
							},
						),
						User: user,
					}
					cloudtestutils.CheckNoPermission(t, info)
				}

				// Next check that the role chain without any external IDs cannot be used to
				// access the storage.
				roleWithoutID := cloudpb.ExternalStorage_AssumeRoleProvider{Role: providerChain[len(providerChain)-1].Role}
				delegatesWithoutID := make([]cloudpb.ExternalStorage_AssumeRoleProvider, 0, len(providerChain)-1)
				for _, p := range providerChain[:len(providerChain)-1] {
					delegatesWithoutID = append(delegatesWithoutID, cloudpb.ExternalStorage_AssumeRoleProvider{Role: p.Role})
				}

				info := cloudtestutils.StoreInfo{
					URI: S3URI(bucket, testPath,
						&cloudpb.ExternalStorage_S3{
							Auth:                  tc.auth,
							AssumeRoleProvider:    roleWithoutID,
							DelegateRoleProviders: delegatesWithoutID,
							AccessKey:             tc.accessKey,
							Secret:                tc.secretKey,
							Region:                "us-east-1",
						},
					),
					User: user,
				}
				cloudtestutils.CheckNoPermission(t, info)

				// Finally, check that the chain of roles can be used to access the storage.
				info.URI = S3URI(bucket, testPath,
					&cloudpb.ExternalStorage_S3{
						Auth:                  tc.auth,
						AssumeRoleProvider:    providerChain[len(providerChain)-1],
						DelegateRoleProviders: providerChain[:len(providerChain)-1],
						AccessKey:             tc.accessKey,
						Secret:                tc.secretKey,
						Region:                "us-east-1",
					},
				)

				cloudtestutils.CheckExportStore(t, info)
			})
		}
	})
}

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	t.Run("default", func(t *testing.T) {
		q := make(url.Values)
		expectedParams := []string{
			AWSEndpointParam,
			AWSAccessKeyParam,
			AWSSecretParam,
			S3RegionParam}
		for _, param := range expectedParams {
			env := NightlyEnvVarS3Params[param]
			v := os.Getenv(env)
			if v == "" {
				skip.IgnoreLintf(t, "%s env var must be set", env)
			}
			q.Add(param, v)
		}

		user := username.RootUserName()
		testID := cloudtestutils.NewTestID()

		u := url.URL{
			Scheme:   "s3",
			Host:     bucket,
			Path:     fmt.Sprintf("backup-test-%d", testID),
			RawQuery: q.Encode(),
		}

		info := cloudtestutils.StoreInfo{
			URI:  u.String(),
			User: user,
		}
		cloudtestutils.CheckExportStore(t, info)
	})
	t.Run("use-path-style", func(t *testing.T) {
		// EngFlow machines have no internet access, and queries even to localhost will time out.
		// So this test is skipped above.
		ctx := context.Background()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer srv.Close()

		q := make(url.Values)
		// Convert IP address to `localhost` to exercise the DNS-defining path in the S3 client.
		localhostURL := strings.Replace(srv.URL, "127.0.0.1", "localhost", -1)
		q.Add(AWSEndpointParam, localhostURL)
		q.Add(AWSAccessKeyParam, "key")
		q.Add(AWSSecretParam, "secret")
		q.Add(S3RegionParam, "region")

		// To validate the test, firstassert that the call fails without the Path Style param.
		u := url.URL{
			Scheme:   "s3",
			Host:     "bucket",
			Path:     "subdir1/subdir2",
			RawQuery: q.Encode(),
		}

		user := username.RootUserName()
		ioConf := base.ExternalIODirConfig{}

		conf, err := cloud.ExternalStorageConfFromURI(u.String(), user)
		if err != nil {
			t.Fatal(err)
		}

		// Setup a sink for the given args.
		testSettings := cluster.MakeTestingClusterSettings()
		clientFactory := blobs.TestBlobServiceClient("")

		storage, err := cloud.MakeExternalStorage(ctx, conf, ioConf, testSettings, clientFactory,
			nil, nil, cloud.NilMetrics)
		if err != nil {
			t.Fatal(err)
		}
		defer storage.Close()

		_, _, err = storage.ReadFile(ctx, "test file", cloud.ReadOptions{NoFileSize: true})
		require.Error(t, err)
		require.Contains(t, err.Error(), "lookup bucket.localhost")
		require.Contains(t, err.Error(), "no such host")

		// Now add the Path Style param and try again.
		q.Add(AWSUsePathStyle, "true")
		u.RawQuery = q.Encode()
		pathStyleConf, err := cloud.ExternalStorageConfFromURI(u.String(), user)
		if err != nil {
			t.Fatal(err)
		}

		pathStyleStorage, err := cloud.MakeExternalStorage(ctx, pathStyleConf, ioConf, testSettings, clientFactory,
			nil, nil, cloud.NilMetrics)
		if err != nil {
			t.Fatal(err)
		}
		defer pathStyleStorage.Close()

		// Should successfuly return a (empty) response.
		_, _, err = pathStyleStorage.ReadFile(ctx, "test file", cloud.ReadOptions{NoFileSize: true})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestS3DisallowCustomEndpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to skip all S3 tests,
	// including auth-implicit, even though it is not used in auth-implicit.
	// Without credentials, it's unclear if we can even communicate with an s3
	// endpoint.
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}

	dest := cloudpb.ExternalStorage{S3Config: &cloudpb.ExternalStorage_S3{Endpoint: "http://do.not.go.there/"}}
	s3, err := MakeS3Storage(context.Background(),
		cloud.EarlyBootExternalStorageContext{
			IOConf: base.ExternalIODirConfig{DisableHTTP: true},
		},
		dest,
	)
	require.Nil(t, s3)
	require.Error(t, err)
}

func TestS3DisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to skip all S3 tests,
	// including auth-implicit, even though it is not used in auth-implicit.
	// Without credentials, it's unclear if we can even communicate with an s3
	// endpoint.
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}

	dest := cloudpb.ExternalStorage{S3Config: &cloudpb.ExternalStorage_S3{Endpoint: "http://do-not-go-there", Auth: cloud.AuthParamImplicit}}

	testSettings := cluster.MakeTestingClusterSettings()

	s3, err := MakeS3Storage(context.Background(),
		cloud.EarlyBootExternalStorageContext{
			IOConf:   base.ExternalIODirConfig{DisableImplicitCredentials: true},
			Settings: testSettings,
		},
		dest,
	)
	require.Nil(t, s3)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "implicit"))
}

type awserror struct {
	error
	code, message string
	fault         smithy.ErrorFault
}

var _ smithy.APIError = awserror{}

func (a awserror) ErrorCode() string {
	return a.code
}

func (a awserror) ErrorMessage() string {
	return a.message
}

func (a awserror) ErrorFault() smithy.ErrorFault {
	return a.fault
}

func TestInterpretAWSCode(t *testing.T) {
	{
		// with code
		err := types.BucketAlreadyOwnedByYou{}
		input := awserror{
			error: errors.New("hello"),
			code:  err.ErrorCode(),
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected tryAWSCode to recognize an awserr.Error type")
		require.False(t, errors.Is(got, cloud.ErrFileDoesNotExist), "should not include cloud.ErrFileDoesNotExist in the error chain")
		require.True(t, strings.Contains(got.Error(), err.ErrorCode()), "aws error code should be in the error chain")
	}

	{
		// with keywords
		input := awserror{
			error: errors.New("‹AccessDenied: User: arn:aws:sts::12345:assumed-role/12345 is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::12345›"),
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected interpretAWSError to recognize keywords")
		require.True(t, strings.Contains(got.Error(), "AccessDenied"), "expected to see AccessDenied in error chain")
		require.True(t, strings.Contains(got.Error(), "AssumeRole"), "expected to see AssumeRole in error chain")
	}

	{
		// with particular code
		err := types.NoSuchBucket{}
		input := awserror{
			error: errors.New("hello"),
			code:  err.ErrorCode(),
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected tryAWSCode to regognize awserr.Error")
		require.True(t, errors.Is(got, cloud.ErrFileDoesNotExist), "expected cloud.ErrFileDoesNotExist in the error chain")
		require.True(t, strings.Contains(got.Error(), err.ErrorCode()), "aws error code should be in the error chain")
	}

	{
		// with keywords and code
		err := types.ObjectNotInActiveTierError{}
		input := awserror{
			error: errors.New("‹AccessDenied: User: arn:aws:sts::12345:assumed-role/12345 is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::12345›"),
			code:  err.ErrorCode(),
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected interpretAWSError to recognize keywords")
		require.True(t, strings.Contains(got.Error(), "AccessDenied"), "expected to see AccessDenied in error chain")
		require.True(t, strings.Contains(got.Error(), "AssumeRole"), "expected to see AssumeRole in error chain")
		require.True(t, strings.Contains(got.Error(), err.ErrorCode()), "aws error code should be in the error chain")
		require.True(t, strings.Contains(got.Error(), "12345"), "SDK error should appear in the error chain")

		// the keywords and code should come through while the original got redacted
		redacted := errors.Redact(got)
		require.True(t, strings.Contains(got.Error(), "AccessDenied"), "expected to see AccessDenied in error chain after redaction")
		require.True(t, strings.Contains(got.Error(), "AssumeRole"), "expected to see AssumeRole in error chain after redaction")
		require.True(t, strings.Contains(got.Error(), err.ErrorCode()), "aws error code should be in the error chain after redaction")
		require.False(t, strings.Contains(redacted, "12345"), "SDK error should have been redacted")
	}

	{
		// no keywords or code
		input := awserror{
			error: errors.New("hello"),
		}
		got := interpretAWSError(input)
		require.Equal(t, input, got, "expected interpretAWSError to pass through the same error")
	}

	{
		// not an AWS error type
		input := errors.New("some other generic error")
		got := interpretAWSError(input)
		require.Equal(t, input, got, "expected interpretAWSError to pass through the same error")
	}
}

// S3 has two "does not exist" errors - ErrCodeNoSuchBucket and ErrCodeNoSuchKey.
// ErrCodeNoSuchKey is tested via the general test in external_storage_test.go.
// This test attempts to ReadFile from a bucket which does not exist.
func TestS3BucketDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	skipIfNoDefaultConfig(t, ctx)

	q := make(url.Values)
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	q.Add(S3RegionParam, "us-east-1")

	u := url.URL{
		Scheme: "s3",
		// Use UUID to create a random bucket name that does not exist. If this was
		// a constent, someone could create the bucket and break the test.
		Host:     uuid.NewV4().String(),
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	user := username.RootUserName()

	conf, err := cloud.ExternalStorageConfFromURI(u.String(), user)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient("")
	testSettings := cluster.MakeTestingClusterSettings()
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		clientFactory,
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if readConf := s.Conf(); readConf != conf {
		t.Fatalf("conf does not roundtrip: started with %+v, got back %+v", conf, readConf)
	}

	_, _, err = s.ReadFile(ctx, "", cloud.ReadOptions{NoFileSize: true})
	require.ErrorIs(t, err, cloud.ErrFileDoesNotExist)
}

func TestAntagonisticS3Read(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to skip all S3 tests,
	// including auth-implicit, even though it is not used in auth-implicit.
	// Without credentials, it's unclear if we can even communicate with an s3
	// endpoint.
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testSettings := cluster.MakeTestingClusterSettings()

	s3file := fmt.Sprintf(
		"s3://%s/%s?%s=%s", bucket, "antagonistic-read",
		cloud.AuthParam, cloud.AuthParamImplicit)
	conf, err := cloud.ExternalStorageConfFromURI(s3file, username.RootUserName())
	require.NoError(t, err)

	cloudtestutils.CheckAntagonisticRead(t, conf, testSettings)
}

func TestNewClientErrorsOnBucketRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to skip all S3 tests,
	// including auth-implicit, even though it is not used in auth-implicit.
	// Without credentials, it's unclear if we can even communicate with an s3
	// endpoint.
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}

	testSettings := cluster.MakeTestingClusterSettings()
	ctx := context.Background()
	s3 := s3Storage{
		opts: s3ClientConfig{
			bucket: "bucket-does-not-exist-v1i3m",
			auth:   cloud.AuthParamImplicit,
		},
		metrics:  cloud.NilMetrics,
		settings: testSettings,
	}
	_, _, err = s3.newClient(ctx)
	require.Regexp(t, "could not find s3 bucket's region", err)
}

// TestReadFileAtReturnsSize tests that ReadFileAt returns
// a cloud.ResumingReader that contains the size of the file.
func TestReadFileAtReturnsSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLint(t, "No AWS credentials")
	}

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	user := username.RootUserName()
	ctx := context.Background()
	testSettings := cluster.MakeTestingClusterSettings()
	file := "testfile"
	data := []byte("hello world")

	gsURI := fmt.Sprintf("s3://%s/%s?AUTH=implicit", bucket, "read-file-at-returns-size")
	conf, err := cloud.ExternalStorageConfFromURI(gsURI, user)
	require.NoError(t, err)
	args := cloud.EarlyBootExternalStorageContext{
		IOConf:   base.ExternalIODirConfig{},
		Settings: testSettings,
		Options:  nil,
		Limiters: nil,
	}
	s, err := MakeS3Storage(ctx, args, conf)
	require.NoError(t, err)

	w, err := s.Writer(ctx, file)
	require.NoError(t, err)

	_, err = w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	reader, _, err := s.ReadFile(ctx, file, cloud.ReadOptions{})
	require.NoError(t, err)

	rr, ok := reader.(*cloud.ResumingReader)
	require.True(t, ok)
	require.Equal(t, int64(len(data)), rr.Size)
}

func TestCanceledError(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	err := ctx.Err()
	require.Error(t, err)
	var aerr error
	aerr = awserr.New(request.CanceledErrorCode,
		"request context canceled", err)
	// Unfortunately, errors.Is returns false, so
	// kvpb.MaybeWrapReplicaCorruptionError will think this is corruption.
	require.False(t, errors.Is(aerr, ctx.Err()))
	aerr = errors.Mark(aerr, ctx.Err())
	require.True(t, errors.Is(aerr, ctx.Err()))
}

func TestAWSS3ImplicitAuthRequiresNoSharedConfigFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Tests if the AWS KMS client can be created when the shared config files
	// are not present, but are present through the rest of the credential/config
	// chain as specified in https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-authentication.html#cli-chap-authentication-precedence

	// If AWS S3 bucket variable is not set, we can assume we are not running in
	// the nightly test environment and can skip.
	baseBucket := os.Getenv("AWS_S3_BUCKET")
	if baseBucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
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

	s3 := s3Storage{
		opts: s3ClientConfig{
			bucket: baseBucket,
			auth:   cloud.AuthParamImplicit,
			region: "us-east-1",
		},
		metrics:  cloud.NilMetrics,
		settings: cluster.MakeTestingClusterSettings(),
	}
	_, _, err := s3.newClient(context.Background())
	require.NoError(t, err)
}
