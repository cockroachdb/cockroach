// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package amazon

import (
	"context"
	"net/url"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	awsScheme    = "aws"
	awsKMSScheme = "aws-kms"
)

type awsKMS struct {
	kms                 *kms.Client
	customerMasterKeyID string
}

var _ cloud.KMS = &awsKMS{}

func init() {
	cloud.RegisterKMSFromURIFactory(MakeAWSKMS, awsScheme, awsKMSScheme)
}

type kmsURIParams struct {
	customerMasterKeyID   string
	accessKey             string
	secret                string
	tempToken             string
	endpoint              string
	region                string
	auth                  string
	roleProvider          roleProvider
	delegateRoleProviders []roleProvider
	verbose               bool
}

var reuseKMSSession = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cloudstorage.aws.reuse_kms_session.enabled",
	"persist the last opened AWS KMS session and reuse it when opening a new session with the same arguments",
	metamorphic.ConstantWithTestBool("aws-reuse-kms", true),
)

var kmsClientCache struct {
	syncutil.Mutex
	key kmsURIParams
	kms *awsKMS
}

func resolveKMSURIParams(kmsURI cloud.ConsumeURL) (kmsURIParams, error) {
	assumeRoleProto, delegateRoleProtos := cloud.ParseRoleProvidersString(kmsURI.ConsumeParam(AssumeRoleParam))
	assumeRoleProvider := makeRoleProvider(assumeRoleProto)
	delegateProviders := make([]roleProvider, len(delegateRoleProtos))
	for i := range delegateRoleProtos {
		delegateProviders[i] = makeRoleProvider(delegateRoleProtos[i])
	}

	params := kmsURIParams{
		customerMasterKeyID:   strings.TrimPrefix(kmsURI.Path, "/"),
		accessKey:             kmsURI.ConsumeParam(AWSAccessKeyParam),
		secret:                kmsURI.ConsumeParam(AWSSecretParam),
		tempToken:             kmsURI.ConsumeParam(AWSTempTokenParam),
		endpoint:              kmsURI.ConsumeParam(AWSEndpointParam),
		region:                kmsURI.ConsumeParam(KMSRegionParam),
		auth:                  kmsURI.ConsumeParam(cloud.AuthParam),
		roleProvider:          assumeRoleProvider,
		delegateRoleProviders: delegateProviders,
		verbose:               log.V(2),
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
	if region == "" {
		// TODO(adityamaru): Maybe use the KeyID to get the region, similar to how
		// we infer the region from the bucket for s3_storage.
		return nil, errors.New("aws kms REGION parameter not specified")
	}
	var loadOptions []func(options *config.LoadOptions) error
	addLoadOption := func(option config.LoadOptionsFunc) {
		loadOptions = append(loadOptions, option)
	}
	addLoadOption(config.WithLogger(newLogAdapter(ctx)))
	if kmsURIParams.verbose {
		addLoadOption(config.WithClientLogMode(awsVerboseLogging))
	}

	var endpointURI string
	if kmsURIParams.endpoint != "" {
		if env.KMSConfig().DisableHTTP {
			return nil, errors.New(
				"custom endpoints disallowed for aws kms due to --aws-kms-disable-http flag")
		}
		client, err := cloud.MakeHTTPClient(env.ClusterSettings(), cloud.NilMetrics, "aws", "KMS", "")
		if err != nil {
			return nil, err
		}
		addLoadOption(config.WithHTTPClient(client))
		endpointURI, err = constructEndpointURI(kmsURIParams.endpoint)
		if err != nil {
			return nil, err
		}
	}

	// "specified": use credentials provided in URI params; error if not present.
	// "implicit": enable SharedConfig, which loads in credentials from environment.
	//             Detailed in https://docs.aws.amazon.com/sdk-for-go/api/aws/session/
	// "": default to `specified`.
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
		addLoadOption(config.WithCredentialsProvider(aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(kmsURIParams.accessKey,
				kmsURIParams.secret, kmsURIParams.tempToken))))
	case cloud.AuthParamImplicit:
		if env.KMSConfig().DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for s3 due to --external-io-disable-implicit-credentials flag")
		}
	default:
		return nil, errors.Errorf("unsupported value %s for %s", kmsURIParams.auth, cloud.AuthParam)
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, cloud.KMSInaccessible(errors.Wrap(err, "could not initialize an aws config"))
	}
	cfg.Region = region

	if kmsURIParams.roleProvider != (roleProvider{}) {
		// If there are delegate roles in the assume-role chain, we create a session
		// for each role in order for it to fetch the credentials from the next role
		// in the chain.
		for _, delegateProvider := range kmsURIParams.delegateRoleProviders {
			client := sts.NewFromConfig(cfg, func(options *sts.Options) {
				if endpointURI != "" {
					options.BaseEndpoint = aws.String(endpointURI)
				}
			})
			intermediateCreds := stscreds.NewAssumeRoleProvider(client, delegateProvider.roleARN, withExternalID(delegateProvider.externalID))
			cfg.Credentials = intermediateCreds
		}

		client := sts.NewFromConfig(cfg, func(options *sts.Options) {
			if endpointURI != "" {
				options.BaseEndpoint = aws.String(endpointURI)
			}
		})
		creds := stscreds.NewAssumeRoleProvider(client, kmsURIParams.roleProvider.roleARN, withExternalID(kmsURIParams.roleProvider.externalID))
		cfg.Credentials = creds
	}

	reuse := reuseKMSSession.Get(&env.ClusterSettings().SV)
	if reuse {
		kmsClientCache.Lock()
		defer kmsClientCache.Unlock()

		if reflect.DeepEqual(kmsClientCache.key, kmsURIParams) {
			return kmsClientCache.kms, nil
		}
	}

	kms := &awsKMS{
		kms: kms.NewFromConfig(cfg, func(options *kms.Options) {
			if endpointURI != "" {
				options.BaseEndpoint = aws.String(endpointURI)
			}
		}),
		customerMasterKeyID: kmsURIParams.customerMasterKeyID,
	}

	if reuse {
		// We already have the cache lock from reading the cached client.
		kmsClientCache.key = kmsURIParams
		kmsClientCache.kms = kms
	}
	return kms, nil
}

// MasterKeyID implements the KMS interface.
func (k *awsKMS) MasterKeyID() string {
	return k.customerMasterKeyID
}

// Encrypt implements the KMS interface.
func (k *awsKMS) Encrypt(ctx context.Context, data []byte) ([]byte, error) {
	encryptInput := &kms.EncryptInput{
		KeyId:     &k.customerMasterKeyID,
		Plaintext: data,
	}

	encryptOutput, err := k.kms.Encrypt(ctx, encryptInput)
	if err != nil {
		return nil, cloud.KMSInaccessible(err)
	}

	return encryptOutput.CiphertextBlob, nil
}

// Decrypt implements the KMS interface.
func (k *awsKMS) Decrypt(ctx context.Context, data []byte) ([]byte, error) {
	decryptInput := &kms.DecryptInput{
		KeyId:          &k.customerMasterKeyID,
		CiphertextBlob: data,
	}

	decryptOutput, err := k.kms.Decrypt(ctx, decryptInput)
	if err != nil {
		return nil, cloud.KMSInaccessible(err)
	}

	return decryptOutput.Plaintext, nil
}

// Close implements the KMS interface.
func (k *awsKMS) Close() error {
	return nil
}
