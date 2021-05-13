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

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeS3Storage(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	conf, err := cloud.ExternalStorageConfFromURI(uri, user)
	if err != nil {
		return nil, err
	}
	testSettings := cluster.MakeTestingClusterSettings()

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		clientFactory, nil, nil)
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
	user := security.RootUserName()
	t.Run("auth-empty-no-cred", func(t *testing.T) {
		_, err := cloud.ExternalStorageFromURI(ctx, fmt.Sprintf("s3://%s/%s", bucket,
			"backup-test-default"), base.ExternalIODirConfig{}, testSettings,
			blobs.TestEmptyBlobClientFactory, user, nil, nil)
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
			"s3://%s/%s?%s=%s",
			bucket, "backup-test-default",
			cloud.AuthParam, cloud.AuthParamImplicit,
		), false, user, nil, nil, testSettings)
	})

	t.Run("auth-specified", func(t *testing.T) {
		uri := S3URI(bucket, "backup-test",
			&roachpb.ExternalStorage_S3{AccessKey: creds.AccessKeyID, Secret: creds.SecretAccessKey, Region: "us-east-1"},
		)
		cloudtestutils.CheckExportStore(t, uri, false, user, nil, nil, testSettings)
		cloudtestutils.CheckListFiles(t, uri, user, nil, nil, testSettings)
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
			"s3://%s/%s?%s=%s&%s=%s",
			bucket, "backup-test-sse-256",
			cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
			"AES256",
		), false, user, nil, nil, testSettings)

		v := os.Getenv("AWS_KMS_KEY_ARN_A")
		if v == "" {
			skip.IgnoreLint(t, "AWS_KMS_KEY_ARN_A env var must be set")
		}
		cloudtestutils.CheckExportStore(t, fmt.Sprintf(
			"s3://%s/%s?%s=%s&%s=%s&%s=%s",
			bucket, "backup-test-sse-kms",
			cloud.AuthParam, cloud.AuthParamImplicit, AWSServerSideEncryptionMode,
			"aws:kms", AWSServerSideEncryptionKMSID, v,
		), false, user, nil, nil, testSettings)
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

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        AWSEndpointParam,
		"AWS_S3_ENDPOINT_KEY":    AWSAccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": AWSSecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	bucket := os.Getenv("AWS_S3_ENDPOINT_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_ENDPOINT_BUCKET env var must be set")
	}
	user := security.RootUserName()

	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	testSettings := cluster.MakeTestingClusterSettings()

	cloudtestutils.CheckExportStore(t, u.String(), false, user, nil, nil, testSettings)
}

func TestS3DisallowCustomEndpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dest := roachpb.ExternalStorage{S3Config: &roachpb.ExternalStorage_S3{Endpoint: "http://do.not.go.there/"}}
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
	dest := roachpb.ExternalStorage{S3Config: &roachpb.ExternalStorage_S3{Endpoint: "http://do-not-go-there", Auth: cloud.AuthParamImplicit}}

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

// S3 has two "does not exist" errors - ErrCodeNoSuchBucket and ErrCodeNoSuchKey.
// ErrCodeNoSuchKey is tested via the general test in external_storage_test.go.
// This test attempts to ReadFile from a bucket which does not exist.
func TestS3BucketDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testSettings := cluster.MakeTestingClusterSettings()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        AWSEndpointParam,
		"AWS_S3_ENDPOINT_KEY":    AWSAccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": AWSSecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}

	bucket := "invalid-bucket"
	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	ctx := context.Background()
	user := security.RootUserName()

	conf, err := cloud.ExternalStorageConfFromURI(u.String(), user)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := cloud.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
		clientFactory, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if readConf := s.Conf(); readConf != conf {
		t.Fatalf("conf does not roundtrip: started with %+v, got back %+v", conf, readConf)
	}

	_, err = s.ReadFile(ctx, "")
	require.Error(t, err, "")
	require.True(t, errors.Is(err, cloud.ErrFileDoesNotExist))
}

func TestAntagonisticS3Read(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Check if we can create aws session with implicit crendentials.
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
	conf, err := cloud.ExternalStorageConfFromURI(s3file, security.RootUserName())
	require.NoError(t, err)

	cloudtestutils.CheckAntagonisticRead(t, conf, testSettings)
}
