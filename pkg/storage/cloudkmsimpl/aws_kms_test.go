// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cloudkmsimpl

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudkms"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestEncryptDecryptAWS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all AWS KMS tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Skip("No AWS credentials")
	}

	q := make(url.Values)
	expect := map[string]string{
		"AWS_REGION": cloudkms.KMSRegionParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			t.Skipf("%s env var must be set", env)
		}
		q.Add(param, v)
	}

	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access AWS KMS
		// in the AWS console, then set it up locally.
		// https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			t.Skip(err)
		}

		// Set AUTH to implicit
		q.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		// Get AWS Key ARN from env variable.
		// TODO(adityamaru): Check if there is a way to specify this in the default
		// role and if we can derive it from there instead?
		v := os.Getenv("AWS_KEY_ARN")
		if v == "" {
			t.Skipf("AWS_KEY_ARN env var must be set")
		}

		uri := fmt.Sprintf("aws:///%s?%s", v, q.Encode())
		fmt.Println(uri)

		testEncryptDecrypt(t, uri)
	})
}
