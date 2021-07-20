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
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/errors"
)

const awsScheme = "aws"

type awsKMS struct {
	kms                 *kms.KMS
	customerMasterKeyID string
}

var _ cloud.KMS = &awsKMS{}

func init() {
	cloud.RegisterKMSFromURIFactory(MakeAWSKMS, awsScheme)
}

type kmsURIParams struct {
	accessKey string
	secret    string
	tempToken string
	endpoint  string
	region    string
	auth      string
}

func resolveKMSURIParams(kmsURI url.URL) kmsURIParams {
	params := kmsURIParams{
		accessKey: kmsURI.Query().Get(AWSAccessKeyParam),
		secret:    kmsURI.Query().Get(AWSSecretParam),
		tempToken: kmsURI.Query().Get(AWSTempTokenParam),
		endpoint:  kmsURI.Query().Get(AWSEndpointParam),
		region:    kmsURI.Query().Get(KMSRegionParam),
		auth:      kmsURI.Query().Get(cloud.AuthParam),
	}

	// AWS secrets often contain + characters, which must be escaped when
	// included in a query string; otherwise, they represent a space character.
	// More than a few users have been bitten by this.
	//
	// Luckily, AWS secrets are base64-encoded data and thus will never actually
	// contain spaces. We can convert any space characters we see to +
	// characters to recover the original secret.
	params.secret = strings.Replace(params.secret, " ", "+", -1)
	return params
}

// MakeAWSKMS is the factory method which returns a configured, ready-to-use
// AWS KMS object.
func MakeAWSKMS(uri string, env cloud.KMSEnv) (cloud.KMS, error) {
	if env.KMSConfig().DisableOutbound {
		return nil, errors.New("external IO must be enabled to use AWS KMS")
	}
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}

	// Extract the URI parameters required to setup the AWS KMS session.
	kmsURIParams := resolveKMSURIParams(*kmsURI)
	region := kmsURIParams.region
	awsConfig := &aws.Config{
		Credentials: credentials.NewStaticCredentials(kmsURIParams.accessKey,
			kmsURIParams.secret, kmsURIParams.tempToken),
	}
	if kmsURIParams.endpoint != "" {
		if env.KMSConfig().DisableHTTP {
			return nil, errors.New(
				"custom endpoints disallowed for aws kms due to --aws-kms-disable-http flag")
		}
		awsConfig.Endpoint = &kmsURIParams.endpoint
		if region == "" {
			// TODO(adityamaru): Think about what the correct way to handle this
			// situation is.
			region = "default-region"
		}
		client, err := cloud.MakeHTTPClient(env.ClusterSettings())
		if err != nil {
			return nil, err
		}
		awsConfig.HTTPClient = client
	}

	// "specified": use credentials provided in URI params; error if not present.
	// "implicit": enable SharedConfig, which loads in credentials from environment.
	//             Detailed in https://docs.aws.amazon.com/sdk-for-go/api/aws/session/
	// "": default to `specified`.
	opts := session.Options{}
	switch kmsURIParams.auth {
	case "", cloud.AuthParamSpecified:
		if kmsURIParams.accessKey == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSAccessKeyParam,
			)
		}
		if kmsURIParams.secret == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSSecretParam,
			)
		}
		opts.Config.MergeIn(awsConfig)
	case cloud.AuthParamImplicit:
		if env.KMSConfig().DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for s3 due to --external-io-implicit-credentials flag")
		}
		opts.SharedConfigState = session.SharedConfigEnable
	default:
		return nil, errors.Errorf("unsupported value %s for %s", kmsURIParams.auth, cloud.AuthParam)
	}

	sess, err := session.NewSessionWithOptions(opts)
	if err != nil {
		return nil, errors.Wrap(err, "new aws session")
	}
	if region == "" {
		// TODO(adityamaru): Maybe use the KeyID to get the region, similar to how
		// we infer the region from the bucket for s3_storage.
		return nil, errors.New("could not find the aws kms region")
	}
	sess.Config.Region = aws.String(region)
	return &awsKMS{
		kms:                 kms.New(sess),
		customerMasterKeyID: strings.TrimPrefix(kmsURI.Path, "/"),
	}, nil
}

// MasterKeyID implements the KMS interface.
func (k *awsKMS) MasterKeyID() (string, error) {
	return k.customerMasterKeyID, nil
}

// Encrypt implements the KMS interface.
func (k *awsKMS) Encrypt(ctx context.Context, data []byte) ([]byte, error) {
	encryptInput := &kms.EncryptInput{
		KeyId:     &k.customerMasterKeyID,
		Plaintext: data,
	}

	encryptOutput, err := k.kms.Encrypt(encryptInput)
	if err != nil {
		return nil, err
	}

	return encryptOutput.CiphertextBlob, nil
}

// Decrypt implements the KMS interface.
func (k *awsKMS) Decrypt(ctx context.Context, data []byte) ([]byte, error) {
	decryptInput := &kms.DecryptInput{
		KeyId:          &k.customerMasterKeyID,
		CiphertextBlob: data,
	}

	decryptOutput, err := k.kms.Decrypt(decryptInput)
	if err != nil {
		return nil, err
	}

	return decryptOutput.Plaintext, nil
}

// Close implements the KMS interface.
func (k *awsKMS) Close() error {
	return nil
}
