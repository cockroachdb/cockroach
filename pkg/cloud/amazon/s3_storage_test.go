// Copyright 2019 The Cockroach Authors.
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

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		clientFactory,
		nil, /* db */
		nil, /* limiters */
		cloud.NilMetrics,
	)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testSettings := cluster.MakeTestingClusterSettings()

	ctx := context.Background()
	user := username.RootUserName()
	testID := cloudtestutils.NewTestID()

	t.Run("auth-empty-no-cred", func(t *testing.T) {
		_, err := cloud.ExternalStorageFromURI(ctx, fmt.Sprintf("s3://%s/%s-%d", bucket,
			"backup-test-default", testID), base.ExternalIODirConfig{}, testSettings,
			blobs.TestEmptyBlobClientFactory, user,
			nil, /* ie */
			nil, /* ief */
			nil, /* kvDB */
			nil, /* limiters */
			nil, /* metrics */
		)
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			cloud.AuthParam,
			cloud.AuthParamSpecified,
			AWSAccessKeyParam,
		))
	})
	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}

		cloudtestutils.CheckExportStore(t, fmt.Sprintf(
			"s3://%s/%s-%d?%s=%s",
			bucket, "backup-test-default", testID,
			cloud.AuthParam, cloud.AuthParamImplicit,
		), false, user,
			nil, /* db */
			testSettings)
	})
	t.Run("auth-specified", func(t *testing.T) {
		uri := S3URI(bucket, fmt.Sprintf("backup-test-%d", testID),
			&cloudpb.ExternalStorage_S3{AccessKey: creds.AccessKeyID, Secret: creds.SecretAccessKey, Region: "us-east-1"},
		)
		cloudtestutils.CheckExportStore(
			t, uri, false, user, nil /* db */, testSettings,
		)
		cloudtestutils.CheckListFiles(
			t, uri, user, nil /* db */, testSettings,
		)
	})

	// Tests that we can put an object with server side encryption specified.
	t.Run("server-side-encryption", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}

		cloudtestutils.CheckExportStore(t, fmt.Sprintf(
			"s3://%s/%s-%d?%s=%s&%s=%s",
			bucket, "backup-test-sse-256", testID,
			cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
			"AES256",
		),
			false,
			user,
			nil, /* db */
			testSettings,
		)

		v := os.Getenv("AWS_KMS_KEY_ARN")
		if v == "" {
			skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
		}
		cloudtestutils.CheckExportStore(t, fmt.Sprintf(
			"s3://%s/%s-%d?%s=%s&%s=%s&%s=%s",
			bucket, "backup-test-sse-kms", testID,
			cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
			"aws:kms", AWSServerSideEncryptionKMSID, v,
		),
			false,
			user,
			nil, /* db */
			testSettings)
	})

	t.Run("server-side-encryption-invalid-params", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}

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
}

func TestPutS3AssumeRole(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testSettings := cluster.MakeTestingClusterSettings()
	testID := cloudtestutils.NewTestID()
	testPath := fmt.Sprintf("backup-test-%d", testID)

	user := username.RootUserName()

	roleArn := os.Getenv("AWS_ASSUME_ROLE")
	if roleArn == "" {
		skip.IgnoreLint(t, "AWS_ASSUME_ROLE env var must be set")
	}
	t.Run("auth-implicit", func(t *testing.T) {
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}
		uri := S3URI(bucket, testPath,
			&cloudpb.ExternalStorage_S3{Auth: cloud.AuthParamImplicit, RoleARN: roleArn, Region: "us-east-1"},
		)
		cloudtestutils.CheckExportStore(
			t, uri, false, user, nil /* db */, testSettings,
		)
		cloudtestutils.CheckListFiles(
			t, uri, user, nil /* db */, testSettings,
		)
	})

	t.Run("auth-specified", func(t *testing.T) {
		uri := S3URI(bucket, testPath,
			&cloudpb.ExternalStorage_S3{Auth: cloud.AuthParamSpecified, RoleARN: roleArn, AccessKey: creds.AccessKeyID, Secret: creds.SecretAccessKey, Region: "us-east-1"},
		)
		cloudtestutils.CheckExportStore(
			t, uri, false, user, nil /* db */, testSettings,
		)
		cloudtestutils.CheckListFiles(
			t, uri, user, nil /* db */, testSettings,
		)
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
					roleURI := S3URI(bucket, testPath,
						&cloudpb.ExternalStorage_S3{
							Auth:               tc.auth,
							AssumeRoleProvider: p,
							AccessKey:          tc.accessKey,
							Secret:             tc.secretKey,
							Region:             "us-east-1",
						},
					)
					cloudtestutils.CheckNoPermission(
						t, roleURI, user, nil /* db */, testSettings,
					)
				}

				// Next check that the role chain without any external IDs cannot be used to
				// access the storage.
				roleWithoutID := cloudpb.ExternalStorage_AssumeRoleProvider{Role: providerChain[len(providerChain)-1].Role}
				delegatesWithoutID := make([]cloudpb.ExternalStorage_AssumeRoleProvider, 0, len(providerChain)-1)
				for _, p := range providerChain[:len(providerChain)-1] {
					delegatesWithoutID = append(delegatesWithoutID, cloudpb.ExternalStorage_AssumeRoleProvider{Role: p.Role})
				}

				uri := S3URI(bucket, testPath,
					&cloudpb.ExternalStorage_S3{
						Auth:                  tc.auth,
						AssumeRoleProvider:    roleWithoutID,
						DelegateRoleProviders: delegatesWithoutID,
						AccessKey:             tc.accessKey,
						Secret:                tc.secretKey,
						Region:                "us-east-1",
					},
				)
				cloudtestutils.CheckNoPermission(
					t, uri, user, nil /* db */, testSettings,
				)

				// Finally, check that the chain of roles can be used to access the storage.
				uri = S3URI(bucket, testPath,
					&cloudpb.ExternalStorage_S3{
						Auth:                  tc.auth,
						AssumeRoleProvider:    providerChain[len(providerChain)-1],
						DelegateRoleProviders: providerChain[:len(providerChain)-1],
						AccessKey:             tc.accessKey,
						Secret:                tc.secretKey,
						Region:                "us-east-1",
					},
				)

				cloudtestutils.CheckExportStore(
					t, uri, false, user, nil /* db */, testSettings,
				)
			})
		}
	})
}

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}
	user := username.RootUserName()
	testID := cloudtestutils.NewTestID()

	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     fmt.Sprintf("backup-test-%d", testID),
		RawQuery: q.Encode(),
	}

	testSettings := cluster.MakeTestingClusterSettings()

	cloudtestutils.CheckExportStore(
		t, u.String(), false, user, nil /* db */, testSettings,
	)
}

func TestS3DisallowCustomEndpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dest := cloudpb.ExternalStorage{S3Config: &cloudpb.ExternalStorage_S3{Endpoint: "http://do.not.go.there/"}}
	s3, err := MakeS3Storage(context.Background(),
		cloud.ExternalStorageContext{
			IOConf: base.ExternalIODirConfig{DisableHTTP: true},
		},
		dest,
	)
	require.Nil(t, s3)
	require.Error(t, err)
}

func TestS3DisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dest := cloudpb.ExternalStorage{S3Config: &cloudpb.ExternalStorage_S3{Endpoint: "http://do-not-go-there", Auth: cloud.AuthParamImplicit}}

	testSettings := cluster.MakeTestingClusterSettings()

	s3, err := MakeS3Storage(context.Background(),
		cloud.ExternalStorageContext{
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
	orig          error
	code, message string
}

var _ awserr.Error = awserror{}

func (a awserror) Code() string {
	return a.code
}

func (a awserror) Message() string {
	return a.message
}

func (a awserror) OrigErr() error {
	return a.orig
}

func TestInterpretAWSCode(t *testing.T) {
	{
		// with code
		input := awserror{
			error: errors.New("hello"),
			code:  s3.ErrCodeBucketAlreadyOwnedByYou,
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected tryAWSCode to recognize an awserr.Error type")
		require.False(t, errors.Is(got, cloud.ErrFileDoesNotExist), "should not include cloud.ErrFileDoesNotExist in the error chain")
		require.True(t, strings.Contains(got.Error(), s3.ErrCodeBucketAlreadyOwnedByYou), "aws error code should be in the error chain")
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
		input := awserror{
			error: errors.New("hello"),
			code:  s3.ErrCodeNoSuchBucket,
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected tryAWSCode to regognize awserr.Error")
		require.True(t, errors.Is(got, cloud.ErrFileDoesNotExist), "expected cloud.ErrFileDoesNotExist in the error chain")
		require.True(t, strings.Contains(got.Error(), s3.ErrCodeNoSuchBucket), "aws error code should be in the error chain")
	}

	{
		// with keywords and code
		input := awserror{
			error: errors.New("‹AccessDenied: User: arn:aws:sts::12345:assumed-role/12345 is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::12345›"),
			code:  s3.ErrCodeObjectAlreadyInActiveTierError,
		}
		got := interpretAWSError(input)
		require.NotNil(t, got, "expected interpretAWSError to recognize keywords")
		require.True(t, strings.Contains(got.Error(), "AccessDenied"), "expected to see AccessDenied in error chain")
		require.True(t, strings.Contains(got.Error(), "AssumeRole"), "expected to see AssumeRole in error chain")
		require.True(t, strings.Contains(got.Error(), s3.ErrCodeObjectAlreadyInActiveTierError), "aws error code should be in the error chain")
		require.True(t, strings.Contains(got.Error(), "12345"), "SDK error should appear in the error chain")

		// the keywords and code should come through while the original got redacted
		redacted := errors.Redact(got)
		require.True(t, strings.Contains(got.Error(), "AccessDenied"), "expected to see AccessDenied in error chain after redaction")
		require.True(t, strings.Contains(got.Error(), "AssumeRole"), "expected to see AssumeRole in error chain after redaction")
		require.True(t, strings.Contains(got.Error(), s3.ErrCodeObjectAlreadyInActiveTierError), "aws error code should be in the error chain after redaction")
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

	testSettings := cluster.MakeTestingClusterSettings()

	credentialsProvider := credentials.SharedCredentialsProvider{}
	_, err := credentialsProvider.Retrieve()
	if err != nil {
		skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
			"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
	}
	q := make(url.Values)
	q.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	q.Add(S3RegionParam, "us-east-1")

	bucket := "invalid-bucket"
	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	ctx := context.Background()
	user := username.RootUserName()

	conf, err := cloud.ExternalStorageConfFromURI(u.String(), user)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
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
	require.Error(t, err, "")
	require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist), "error is not cloud.ErrFileDoesNotExist: %v", err)
}

func TestAntagonisticS3Read(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Check if we can create aws session with implicit credentials.
	_, err := session.NewSession()
	if err != nil {
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

	_, err := session.NewSession()
	if err != nil {
		skip.IgnoreLint(t, "No AWS credentials")
	}

	testSettings := cluster.MakeTestingClusterSettings()
	ctx := context.Background()
	cfg := s3ClientConfig{
		bucket: "bucket-does-not-exist-v1i3m",
		auth:   cloud.AuthParamImplicit,
	}
	_, _, err = newClient(ctx, cfg, testSettings)
	require.Regexp(t, "could not find s3 bucket's region", err)
}

// TestReadFileAtReturnsSize tests that ReadFileAt returns
// a cloud.ResumingReader that contains the size of the file.
func TestReadFileAtReturnsSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
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
	args := cloud.ExternalStorageContext{
		IOConf:          base.ExternalIODirConfig{},
		Settings:        testSettings,
		DB:              nil,
		Options:         nil,
		Limiters:        nil,
		MetricsRecorder: cloud.NilMetrics,
	}
	s, err := MakeS3Storage(ctx, args, conf)
	require.NoError(t, err)

	w, err := s.Writer(ctx, file)
	require.NoError(t, err)

	_, err = w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	reader, _, err := s.ReadFile(ctx, file, cloud.ReadOptions{NoFileSize: true})
	require.NoError(t, err)

	rr, ok := reader.(*cloud.ResumingReader)
	require.True(t, ok)
	require.Equal(t, int64(len(data)), rr.Size)
}
