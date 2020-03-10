// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

	ctx := context.TODO()
	t.Run("auth-empty-no-cred", func(t *testing.T) {
		_, err := ExternalStorageFromURI(
			ctx, fmt.Sprintf("s3://%s/%s", bucket, "backup-test-default"),
			base.ExternalIOConfig{}, testSettings, blobs.TestEmptyBlobClientFactory,
		)
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			AuthParam,
			authParamSpecified,
			S3AccessKeyParam,
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

		testExportStore(
			t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s",
				bucket, "backup-test-default",
				AuthParam, authParamImplicit,
			),
			false,
		)
	})

	t.Run("auth-specified", func(t *testing.T) {
		testExportStore(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "backup-test",
				S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
			false,
		)
		testListFiles(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "listing-test",
				S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
		)
	})
}

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        S3EndpointParam,
		"AWS_S3_ENDPOINT_KEY":    S3AccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": S3SecretParam,
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

	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	testExportStore(t, u.String(), false)
}

func TestS3DisallowCustomEndpoints(t *testing.T) {
	s3, err := makeS3Storage(context.TODO(),
		base.ExternalIOConfig{DisableHTTP: true},
		&roachpb.ExternalStorage_S3{Endpoint: "http://do.not.go.there/"}, nil,
	)
	require.Nil(t, s3)
	require.Error(t, err)
}

func TestS3DisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s3, err := makeS3Storage(context.TODO(),
		base.ExternalIOConfig{DisableImplicitCredentials: true},
		&roachpb.ExternalStorage_S3{
			Endpoint: "http://do-not-go-there",
			Auth:     authParamImplicit,
		}, testSettings,
	)
	require.Nil(t, s3)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "implicit"))
}
