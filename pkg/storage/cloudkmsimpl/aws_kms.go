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
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudkms"
	"github.com/pkg/errors"
)

const awsScheme = "aws"

type awsKMS struct {
	conf *roachpb.KMS_AWSKMS
	kms  *kms.KMS
}

var _ cloudkms.KMS = &awsKMS{}

func init() {
	cloudkms.RegisterKMSFromURIFactory(awsKMSFromURIFactory, awsScheme)
}

func awsKMSFromURIFactory(
	ctx context.Context, uri string, env cloudkms.KMSEnv,
) (cloudkms.KMS, error) {
	conf, err := kmsConfFromURI(uri)
	if err != nil {
		return nil, err
	}

	awsConf := conf.AWSKMSConfig
	if awsConf == nil {
		return nil, errors.Errorf("aws kms requested but info missing")
	}
	region := awsConf.Region
	config := awsConf.Keys()
	if awsConf.Endpoint != "" {
		if env.KMSConfig().DisableHTTP {
			return nil, errors.New(
				"custom endpoints disallowed for aws kms due to --aws-kms-disable-http flag")
		}
		config.Endpoint = &awsConf.Endpoint
		if awsConf.Region == "" {
			// TODO(adityamaru): Think about what the correct way to handle this
			// situation is.
			return nil, errors.New("could not find the aws kms region")
		}
		client, err := cloud.MakeHTTPClient(env.ClusterSettings())
		if err != nil {
			return nil, err
		}
		config.HTTPClient = client
	}

	// "specified": use credentials provided in URI params; error if not present.
	// "implicit": enable SharedConfig, which loads in credentials from environment.
	//             Detailed in https://docs.aws.amazon.com/sdk-for-go/api/aws/session/
	// "": default to `specified`.
	opts := session.Options{}
	switch awsConf.Auth {
	case "", cloud.AuthParamSpecified:
		if awsConf.AccessKey == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				cloud.S3AccessKeyParam,
			)
		}
		if awsConf.Secret == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				cloud.S3SecretParam,
			)
		}
		opts.Config.MergeIn(config)
	case cloud.AuthParamImplicit:
		if env.KMSConfig().DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for s3 due to --external-io-implicit-credentials flag")
		}
		opts.SharedConfigState = session.SharedConfigEnable
	default:
		return nil, errors.Errorf("unsupported value %s for %s", awsConf.Auth, cloud.AuthParam)
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
	if awsConf.Endpoint != "" {
		sess.Config.S3ForcePathStyle = aws.Bool(true)
	}
	return &awsKMS{
		conf: awsConf,
		kms:  kms.New(sess),
	}, nil
}

// ID implements the KMS interface.
func (k *awsKMS) ID() (string error) {
	return nil
}

// Encrypt implements the KMS interface.
func (k *awsKMS) Encrypt(data []byte) ([]byte, error) {
	encryptInput := &kms.EncryptInput{
		KeyId:     &k.conf.KeyID,
		Plaintext: data,
	}

	encryptOutput, err := k.kms.Encrypt(encryptInput)
	if err != nil {
		return nil, err
	}

	return encryptOutput.CiphertextBlob, nil
}

// Decrypt implements the KMS interface.
func (k *awsKMS) Decrypt(data []byte) ([]byte, error) {
	decryptInput := &kms.DecryptInput{
		KeyId:          &k.conf.KeyID,
		CiphertextBlob: data,
	}

	decryptOutput, err := k.kms.Decrypt(decryptInput)
	if err != nil {
		return nil, err
	}

	return decryptOutput.Plaintext, nil
}

func (k *awsKMS) Conf() roachpb.KMS {
	return roachpb.KMS{
		roachpb.KMSProvider_AWSKMS,
		k.conf,
	}
}
