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
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon/amazonparams"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	awsScheme    = "aws"
	awsKMSScheme = "aws-kms"
)

type awsKMS struct {
	kms                 *kms.KMS
	customerMasterKeyID string
}

var _ cloud.KMS = &awsKMS{}

func init() {
	cloud.RegisterKMSFromURIFactory(MakeAWSKMS, awsScheme, awsKMSScheme)
}

type kmsURIParams struct {
	accessKey             string
	secret                string
	tempToken             string
	endpoint              string
	region                string
	auth                  string
	roleProvider          roleProvider
	delegateRoleProviders []roleProvider
}

func resolveKMSURIParams(kmsURI cloud.ConsumeURL) (kmsURIParams, error) {
	assumeRoleProto, delegateRoleProtos := cloud.ParseRoleProvidersString(kmsURI.ConsumeParam(amazonparams.AssumeRole))
	assumeRoleProvider := makeRoleProvider(assumeRoleProto)
	delegateProviders := make([]roleProvider, len(delegateRoleProtos))
	for i := range delegateRoleProtos {
		delegateProviders[i] = makeRoleProvider(delegateRoleProtos[i])
	}

	params := kmsURIParams{
		accessKey:             kmsURI.ConsumeParam(amazonparams.AWSAccessKey),
		secret:                kmsURI.ConsumeParam(amazonparams.AWSSecret),
		tempToken:             kmsURI.ConsumeParam(amazonparams.AWSTempToken),
		endpoint:              kmsURI.ConsumeParam(amazonparams.AWSEndpoint),
		region:                kmsURI.ConsumeParam(amazonparams.KMSRegion),
		auth:                  kmsURI.ConsumeParam(cloud.AuthParam),
		roleProvider:          assumeRoleProvider,
		delegateRoleProviders: delegateProviders,
	}

	// Validate that all the passed in parameters are supported.
	if unknownParams := kmsURI.RemainingQueryParams(); len(unknownParams) > 0 {
		return kmsURIParams{}, errors.Errorf(
			`unknown KMS query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	// AWS secrets often contain + characters, which must be escaped when
	// included in a query string; otherwise, they represent a space character.
	// More than a few users have been bitten by this.
	//
	// Luckily, AWS secrets are base64-encoded data and thus will never actually
	// contain spaces. We can convert any space characters we see to +
	// characters to recover the original secret.
	params.secret = strings.Replace(params.secret, " ", "+", -1)
	return params, nil
}

// MakeAWSKMS is the factory method which returns a configured, ready-to-use
// AWS KMS object.
func MakeAWSKMS(ctx context.Context, uri string, env cloud.KMSEnv) (cloud.KMS, error) {
	if env.KMSConfig().DisableOutbound {
		return nil, errors.New("external IO must be enabled to use AWS KMS")
	}
	kmsURI, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}

	// Extract the URI parameters required to setup the AWS KMS session.
	kmsConsumeURL := cloud.ConsumeURL{URL: kmsURI}
	kmsURIParams, err := resolveKMSURIParams(kmsConsumeURL)
	if err != nil {
		return nil, err
	}
	region := kmsURIParams.region
	awsConfig := &aws.Config{
		Credentials: credentials.NewStaticCredentials(kmsURIParams.accessKey,
			kmsURIParams.secret, kmsURIParams.tempToken),
	}
	awsConfig.Logger = newLogAdapter(ctx)
	if log.V(2) {
		awsConfig.LogLevel = awsVerboseLogging
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
				amazonparams.AWSAccessKey,
			)
		}
		if kmsURIParams.secret == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				amazonparams.AWSSecret,
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

	if kmsURIParams.roleProvider != (roleProvider{}) {
		if !env.ClusterSettings().Version.IsActive(ctx, clusterversion.V22_2SupportAssumeRoleAuth) {
			return nil, errors.New("cannot authenticate to KMS via assume role until cluster has fully upgraded to 22.2")
		}

		// If there are delegate roles in the assume-role chain, we create a session
		// for each role in order for it to fetch the credentials from the next role
		// in the chain.
		for _, delegateProvider := range kmsURIParams.delegateRoleProviders {
			intermediateCreds := stscreds.NewCredentials(sess, delegateProvider.roleARN, withExternalID(delegateProvider.externalID))
			opts.Config.Credentials = intermediateCreds

			sess, err = session.NewSessionWithOptions(opts)
			if err != nil {
				return nil, errors.Wrap(err, "session with intermediate credentials")
			}
		}

		creds := stscreds.NewCredentials(sess, kmsURIParams.roleProvider.roleARN, withExternalID(kmsURIParams.roleProvider.externalID))
		opts.Config.Credentials = creds
		sess, err = session.NewSessionWithOptions(opts)
		if err != nil {
			return nil, errors.Wrap(err, "session with assume role credentials")
		}
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
