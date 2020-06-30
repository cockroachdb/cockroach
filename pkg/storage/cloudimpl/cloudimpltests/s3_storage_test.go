// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Skip("No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	ctx := context.Background()
	user := security.RootUser
	t.Run("auth-empty-no-cred", func(t *testing.T) {
		_, err := cloudimpl.ExternalStorageFromURI(ctx, fmt.Sprintf("s3://%s/%s", bucket,
			"backup-test-default"), base.ExternalIODirConfig{}, testSettings,
			blobs.TestEmptyBlobClientFactory, user, nil, nil)
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			cloudimpl.AuthParam,
			cloudimpl.AuthParamSpecified,
			cloudimpl.S3AccessKeyParam,
		))
	})
	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			t.Skip(err)
		}

		testExportStore(t, fmt.Sprintf(
			"s3://%s/%s?%s=%s",
			bucket, "backup-test-default",
			cloudimpl.AuthParam, cloudimpl.AuthParamImplicit,
		), false, user, nil, nil)
	})

	t.Run("auth-specified", func(t *testing.T) {
		testExportStore(t, fmt.Sprintf(
			"s3://%s/%s?%s=%s&%s=%s",
			bucket, "backup-test",
			cloudimpl.S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
			cloudimpl.S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
		), false, user, nil, nil)
		testListFiles(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "listing-test",
				cloudimpl.S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				cloudimpl.S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
			user, nil, nil,
		)
	})
}

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        cloudimpl.S3EndpointParam,
		"AWS_S3_ENDPOINT_KEY":    cloudimpl.S3AccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": cloudimpl.S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": cloudimpl.S3SecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			t.Skipf("%s env var must be set", env)
		}
		q.Add(param, v)
	}

	bucket := os.Getenv("AWS_S3_ENDPOINT_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_ENDPOINT_BUCKET env var must be set")
	}
	user := security.RootUser

	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	testExportStore(t, u.String(), false, user, nil, nil)
}

func TestS3DisallowCustomEndpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s3, err := cloudimpl.MakeS3Storage(context.Background(),
		base.ExternalIODirConfig{DisableHTTP: true},
		&roachpb.ExternalStorage_S3{Endpoint: "http://do.not.go.there/"}, nil,
	)
	require.Nil(t, s3)
	require.Error(t, err)
}

func TestS3DisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s3, err := cloudimpl.MakeS3Storage(context.Background(),
		base.ExternalIODirConfig{DisableImplicitCredentials: true},
		&roachpb.ExternalStorage_S3{
			Endpoint: "http://do-not-go-there",
			Auth:     cloudimpl.AuthParamImplicit,
		}, testSettings,
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

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        cloudimpl.S3EndpointParam,
		"AWS_S3_ENDPOINT_KEY":    cloudimpl.S3AccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": cloudimpl.S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": cloudimpl.S3SecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			t.Skipf("%s env var must be set", env)
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
	user := security.RootUser

	conf, err := cloudimpl.ExternalStorageConfFromURI(u.String(), user)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := cloudimpl.MakeExternalStorage(ctx, conf, base.ExternalIODirConfig{}, testSettings,
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
	require.True(t, errors.Is(err, cloudimpl.ErrFileDoesNotExist))
}
