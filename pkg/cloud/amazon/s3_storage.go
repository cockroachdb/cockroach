// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package amazon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// AWSAccessKeyParam is the query parameter for access_key in an AWS URI.
	AWSAccessKeyParam = "AWS_ACCESS_KEY_ID"
	// AWSSecretParam is the query parameter for the 'secret' in an AWS URI.
	AWSSecretParam = "AWS_SECRET_ACCESS_KEY"
	// AWSTempTokenParam is the query parameter for session_token in an AWS URI.
	AWSTempTokenParam = "AWS_SESSION_TOKEN"
	// AWSEndpointParam is the query parameter for the 'endpoint' in an AWS URI.
	AWSEndpointParam = "AWS_ENDPOINT"
	// AWSEndpointParam is the query parameter for UsePathStyle in S3 options.
	AWSUsePathStyle = "AWS_USE_PATH_STYLE"

	// AWSServerSideEncryptionMode is the query parameter in an AWS URI, for the
	// mode to be used for server side encryption. It can either be AES256 or
	// aws:kms.
	AWSServerSideEncryptionMode = "AWS_SERVER_ENC_MODE"

	// AWSServerSideEncryptionKMSID is the query parameter in an AWS URI, for the
	// KMS ID to be used for server side encryption.
	AWSServerSideEncryptionKMSID = "AWS_SERVER_KMS_ID"

	// S3StorageClassParam is the query parameter used in S3 URIs to configure the
	// storage class for written objects.
	S3StorageClassParam = "S3_STORAGE_CLASS"

	// S3RegionParam is the query parameter for the 'endpoint' in an S3 URI.
	S3RegionParam = "AWS_REGION"

	// KMSRegionParam is the query parameter for the 'region' in every KMS URI.
	KMSRegionParam = "REGION"

	// AssumeRoleParam is the query parameter for the chain of AWS Role ARNs to
	// assume.
	AssumeRoleParam = "ASSUME_ROLE"

	// scheme component of an S3 URI.
	scheme = "s3"

	checksumAlgorithm = types.ChecksumAlgorithmSha256
)

// NightlyEnvVarS3Params maps param keys that get added to an S3
// URI to the environment variables hard coded on the VM
// running the nightly cloud unit tests.
var NightlyEnvVarS3Params = map[string]string{
	AWSEndpointParam:  "AWS_S3_ENDPOINT",
	AWSAccessKeyParam: "AWS_ACCESS_KEY_ID",
	S3RegionParam:     "AWS_DEFAULT_REGION",
	AWSSecretParam:    "AWS_SECRET_ACCESS_KEY",
}

// NightlyEnvVarKMSParams maps param keys that get added to a KMS
// URI to the environment variables hard coded on the VM
// running the nightly cloud unit tests.
var NightlyEnvVarKMSParams = map[string]string{
	AWSEndpointParam: "AWS_KMS_ENDPOINT",
	KMSRegionParam:   "AWS_KMS_REGION",
}

type s3Storage struct {
	bucket         *string
	conf           *cloudpb.ExternalStorage_S3
	ioConf         base.ExternalIODirConfig
	settings       *cluster.Settings
	prefix         string
	metrics        *cloud.Metrics
	storageOptions cloud.ExternalStorageOptions

	opts   s3ClientConfig
	cached *s3Client
}

// customRetryer implements the `request.Retryer` interface and allows for
// customization of the retry behaviour of an AWS client.
type customRetryer struct{}

// isErrReadConnectionReset returns true if the underlying error is a read
// connection reset error.
//
// NB: A read connection reset error is thrown when the SDK is unable to read
// the response of an underlying API request due to a connection reset. The
// DefaultRetryer in the AWS SDK does not treat this error as a retryable error
// since the SDK does not have knowledge about the idempotence of the request,
// and whether it is safe to retry -
// https://github.com/aws/aws-sdk-go/pull/2926#issuecomment-553637658.
//
// In CRDB all operations with s3 (read, write, list) are considered idempotent,
// and so we can treat the read connection reset error as retryable too.
func isErrReadConnectionReset(err error) bool {
	// The error string must match the one in
	// github.com/aws/aws-sdk-go/aws/request/connection_reset_error.go. This is
	// unfortunate but the only solution until the SDK exposes a specialized error
	// code or type for this class of errors.
	return err != nil && strings.Contains(err.Error(), "read: connection reset")
}

// IsErrorRetryable implements the retry.IsErrorRetryable interface.
func (sr *customRetryer) IsErrorRetryable(e error) aws.Ternary {
	return aws.BoolTernary(isErrReadConnectionReset(e))
}

var _ retry.IsErrorRetryable = &customRetryer{}

// s3Client wraps an SDK client and uploader for a given session.
type s3Client struct {
	client   *s3.Client
	uploader *manager.Uploader
}

var reuseSession = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cloudstorage.s3.session_reuse.enabled",
	"persist the last opened s3 session and re-use it when opening a new session with the same arguments",
	true,
)

var usePutObject = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cloudstorage.s3.buffer_and_put_uploads.enabled",
	"construct files in memory before uploading via PutObject (may cause crashes due to memory usage)",
	false,
)

// roleProvider contains fields about the role that needs to be assumed
// in order to access the external storage.
type roleProvider struct {
	// roleARN, if non-empty, is the ARN of the AWS Role being assumed.
	roleARN string

	// externalID, if non-empty, is the external ID that must be passed along
	// with the role in order to assume it. Some additional information about
	// the issues that external IDs can address can be found on the AWS docs:
	// https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
	externalID string
}

func makeRoleProvider(provider cloudpb.ExternalStorage_AssumeRoleProvider) roleProvider {
	return roleProvider{
		roleARN:    provider.Role,
		externalID: provider.ExternalID,
	}
}

// s3ClientConfig is the immutable config used to initialize an s3 session.
// It contains values copied from corresponding fields in ExternalStorage_S3
// which are used by the session (but not those that are only used by individual
// requests).
type s3ClientConfig struct {
	// copied from ExternalStorage_S3.
	endpoint, region, bucket, accessKey, secret, tempToken, auth string
	usePathStyle                                                 bool
	assumeRoleProvider                                           roleProvider
	delegateRoleProviders                                        []roleProvider

	// log.V(2) decides session init params so include it in key.
	verbose bool
}

func clientConfig(conf *cloudpb.ExternalStorage_S3) s3ClientConfig {
	var assumeRoleProvider roleProvider
	var delegateRoleProviders []roleProvider

	// In order to maintain backwards compatibility, parse both fields where roles
	// are stored in ExternalStorage, preferring the provider fields.
	if conf.AssumeRoleProvider.Role == "" && conf.RoleARN != "" {
		assumeRoleProvider = roleProvider{
			roleARN: conf.RoleARN,
		}

		delegateRoleProviders = make([]roleProvider, len(conf.DelegateRoleARNs))
		for i := range conf.DelegateRoleARNs {
			delegateRoleProviders[i] = roleProvider{
				roleARN: conf.DelegateRoleARNs[i],
			}
		}
	} else {
		assumeRoleProvider = makeRoleProvider(conf.AssumeRoleProvider)
		delegateRoleProviders = make([]roleProvider, len(conf.DelegateRoleProviders))
		for i := range conf.DelegateRoleProviders {
			delegateRoleProviders[i] = makeRoleProvider(conf.DelegateRoleProviders[i])
		}
	}

	return s3ClientConfig{
		endpoint:              conf.Endpoint,
		usePathStyle:          conf.UsePathStyle,
		region:                conf.Region,
		bucket:                conf.Bucket,
		accessKey:             conf.AccessKey,
		secret:                conf.Secret,
		tempToken:             conf.TempToken,
		auth:                  conf.Auth,
		verbose:               log.V(2),
		assumeRoleProvider:    assumeRoleProvider,
		delegateRoleProviders: delegateRoleProviders,
	}
}

var s3ClientCache struct {
	syncutil.Mutex
	// TODO(dt): make this an >1 item cache e.g. add a FIFO ring.
	key    s3ClientConfig
	client *s3Client
}

var _ cloud.ExternalStorage = &s3Storage{}

type serverSideEncMode string

const (
	kmsEnc    serverSideEncMode = "aws:kms"
	aes256Enc serverSideEncMode = "AES256"
)

// S3URI returns the string URI for a given bucket and path.
func S3URI(bucket, path string, conf *cloudpb.ExternalStorage_S3) string {
	q := make(url.Values)
	setIf := func(key, value string) {
		if value != "" {
			q.Set(key, value)
		}
	}
	setIf(AWSAccessKeyParam, conf.AccessKey)
	setIf(AWSSecretParam, conf.Secret)
	setIf(AWSTempTokenParam, conf.TempToken)
	setIf(AWSEndpointParam, conf.Endpoint)
	setIf(S3RegionParam, conf.Region)
	setIf(cloud.AuthParam, conf.Auth)
	setIf(AWSServerSideEncryptionMode, conf.ServerEncMode)
	setIf(AWSServerSideEncryptionKMSID, conf.ServerKMSID)
	setIf(S3StorageClassParam, conf.StorageClass)
	if conf.UsePathStyle {
		q.Set(AWSUsePathStyle, "true")
	}
	if conf.AssumeRoleProvider.Role != "" {
		roleProviderStrings := make([]string, 0, len(conf.DelegateRoleProviders)+1)
		for _, p := range conf.DelegateRoleProviders {
			roleProviderStrings = append(roleProviderStrings, p.EncodeAsString())
		}
		roleProviderStrings = append(roleProviderStrings, conf.AssumeRoleProvider.EncodeAsString())
		q.Set(AssumeRoleParam, strings.Join(roleProviderStrings, ","))
	}

	s3URL := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     path,
		RawQuery: q.Encode(),
	}

	return s3URL.String()
}

func parseS3URL(uri *url.URL) (cloudpb.ExternalStorage, error) {
	s3URL := cloud.ConsumeURL{URL: uri}
	conf := cloudpb.ExternalStorage{}
	if s3URL.Host == "" {
		return conf, errors.New("empty host component; s3 URI must specify a target bucket")
	}

	conf.Provider = cloudpb.ExternalStorageProvider_s3

	// TODO(rui): currently the value of AssumeRoleParam is written into both of
	// the RoleARN fields and the RoleProvider fields in order to support a mixed
	// version cluster with nodes on 22.2.0 and 22.2.1+. The logic around the
	// RoleARN fields can be removed in 23.2.
	assumeRoleValue := s3URL.ConsumeParam(AssumeRoleParam)
	assumeRoleProvider, delegateRoleProviders := cloud.ParseRoleProvidersString(assumeRoleValue)
	assumeRole, delegateRoles := cloud.ParseRoleString(assumeRoleValue)

	pathStyleStr := s3URL.ConsumeParam(AWSUsePathStyle)
	pathStyleBool := false
	if pathStyleStr != "" {
		var err error
		pathStyleBool, err = strconv.ParseBool(pathStyleStr)
		if err != nil {
			return cloudpb.ExternalStorage{}, errors.Wrapf(err, "cannot parse %s as bool", AWSUsePathStyle)
		}
	}

	conf.S3Config = &cloudpb.ExternalStorage_S3{
		Bucket:                s3URL.Host,
		Prefix:                s3URL.Path,
		AccessKey:             s3URL.ConsumeParam(AWSAccessKeyParam),
		Secret:                s3URL.ConsumeParam(AWSSecretParam),
		TempToken:             s3URL.ConsumeParam(AWSTempTokenParam),
		Endpoint:              s3URL.ConsumeParam(AWSEndpointParam),
		UsePathStyle:          pathStyleBool,
		Region:                s3URL.ConsumeParam(S3RegionParam),
		Auth:                  s3URL.ConsumeParam(cloud.AuthParam),
		ServerEncMode:         s3URL.ConsumeParam(AWSServerSideEncryptionMode),
		ServerKMSID:           s3URL.ConsumeParam(AWSServerSideEncryptionKMSID),
		StorageClass:          s3URL.ConsumeParam(S3StorageClassParam),
		RoleARN:               assumeRole,
		DelegateRoleARNs:      delegateRoles,
		AssumeRoleProvider:    assumeRoleProvider,
		DelegateRoleProviders: delegateRoleProviders,
		/* NB: additions here should also update s3QueryParams() serializer */
	}
	conf.S3Config.Prefix = strings.TrimLeft(conf.S3Config.Prefix, "/")
	// AWS secrets often contain + characters, which must be escaped when
	// included in a query string; otherwise, they represent a space character.
	// More than a few users have been bitten by this.
	//
	// Luckily, AWS secrets are base64-encoded data and thus will never actually
	// contain spaces. We can convert any space characters we see to +
	// characters to recover the original secret.
	conf.S3Config.Secret = strings.Replace(conf.S3Config.Secret, " ", "+", -1)

	// Validate that all the passed in parameters are supported.
	if unknownParams := s3URL.RemainingQueryParams(); len(unknownParams) > 0 {
		return cloudpb.ExternalStorage{}, errors.Errorf(
			`unknown S3 query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	// Validate the authentication parameters are set correctly.
	switch conf.S3Config.Auth {
	case "", cloud.AuthParamSpecified:
		if conf.S3Config.AccessKey == "" {
			return cloudpb.ExternalStorage{}, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSAccessKeyParam,
			)
		}
		if conf.S3Config.Secret == "" {
			return cloudpb.ExternalStorage{}, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSSecretParam,
			)
		}
	case cloud.AuthParamImplicit:
	default:
		return cloudpb.ExternalStorage{}, errors.Errorf("unsupported value %s for %s",
			conf.S3Config.Auth, cloud.AuthParam)
	}

	// Ensure that a KMS ID is specified if server side encryption is set to use
	// KMS.
	if conf.S3Config.ServerEncMode != "" {
		switch conf.S3Config.ServerEncMode {
		case string(aes256Enc):
		case string(kmsEnc):
			if conf.S3Config.ServerKMSID == "" {
				return cloudpb.ExternalStorage{}, errors.New("AWS_SERVER_KMS_ID param must be set" +
					" when using aws:kms server side encryption mode.")
			}
		default:
			return cloudpb.ExternalStorage{}, errors.Newf("unsupported server encryption mode %s. "+
				"Supported values are `aws:kms` and `AES256`.", conf.S3Config.ServerEncMode)
		}
	}

	return conf, nil
}

// MakeS3Storage returns an instance of S3 ExternalStorage.
func MakeS3Storage(
	ctx context.Context, args cloud.EarlyBootExternalStorageContext, dest cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.s3")
	conf := dest.S3Config
	if conf == nil {
		return nil, errors.Errorf("s3 upload requested but info missing")
	}

	if conf.Endpoint != "" {
		if args.IOConf.DisableHTTP {
			return nil, errors.New(
				"custom endpoints disallowed for s3 due to --external-io-disable-http flag")
		}
	}

	switch conf.Auth {
	case "", cloud.AuthParamSpecified:
		if conf.AccessKey == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSAccessKeyParam,
			)
		}
		if conf.Secret == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				cloud.AuthParam,
				cloud.AuthParamSpecified,
				AWSSecretParam,
			)
		}
	case cloud.AuthParamImplicit:
		if args.IOConf.DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for s3 due to --external-io-disable-implicit-credentials flag")
		}
	default:
		return nil, errors.Errorf("unsupported value %s for %s", conf.Auth, cloud.AuthParam)
	}

	// Ensure that a KMS ID is specified if server side encryption is set to use
	// KMS.
	if conf.ServerEncMode != "" {
		switch conf.ServerEncMode {
		case string(aes256Enc):
		case string(kmsEnc):
			if conf.ServerKMSID == "" {
				return nil, errors.New("AWS_SERVER_KMS_ID param must be set" +
					" when using aws:kms server side encryption mode.")
			}
		default:
			return nil, errors.Newf("unsupported server encryption mode %s. "+
				"Supported values are `aws:kms` and `AES256`.", conf.ServerEncMode)
		}
	}

	s := &s3Storage{
		bucket:         aws.String(conf.Bucket),
		conf:           conf,
		ioConf:         args.IOConf,
		prefix:         conf.Prefix,
		metrics:        args.MetricsRecorder,
		settings:       args.Settings,
		opts:           clientConfig(conf),
		storageOptions: args.ExternalStorageOptions(),
	}

	reuse := reuseSession.Get(&args.Settings.SV)
	if !reuse {
		return s, nil
	}

	s3ClientCache.Lock()
	defer s3ClientCache.Unlock()

	if reflect.DeepEqual(s3ClientCache.key, s.opts) {
		s.cached = s3ClientCache.client
		return s, nil
	}

	// Make the client and cache it *while holding the lock*. We want to keep
	// other callers from making clients in the meantime, not just to avoid making
	// duplicate clients in a race but also because making clients concurrently
	// can fail if the AWS metadata server hits its rate limit.
	client, _, err := s.newClient(ctx)
	if err != nil {
		return nil, err
	}
	s.cached = &client
	s3ClientCache.key = s.opts
	s3ClientCache.client = &client
	return s, nil
}

type awsLogAdapter struct {
	ctx context.Context
}

func (l *awsLogAdapter) Logf(_ logging.Classification, format string, v ...interface{}) {
	log.Infof(l.ctx, format, v...)
}

func newLogAdapter(ctx context.Context) *awsLogAdapter {
	return &awsLogAdapter{
		ctx: logtags.AddTags(context.Background(), logtags.FromContext(ctx)),
	}
}

var awsVerboseLogging = aws.LogRequestEventMessage | aws.LogResponseEventMessage | aws.LogRetries | aws.LogSigning

func constructEndpointURI(endpoint string) (string, error) {
	parsedURL, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Wrap(err, "error parsing URL")
	}

	if parsedURL.Scheme != "" {
		return parsedURL.String(), nil
	}
	// Input URL doesn't have a scheme, construct a new URL with a default
	// scheme.
	u := &url.URL{
		Scheme: "https", // Default scheme
		Host:   endpoint,
	}

	return u.String(), nil
}

// newClient creates a client from the passed s3ClientConfig and if the passed
// config's region is empty, used the passed bucket to determine a region and
// configures the client with it as well as returning it (so the caller can
// remember it for future calls).
func (s *s3Storage) newClient(ctx context.Context) (s3Client, string, error) {

	// Open a span if client creation will do IO/RPCs to find creds/bucket region.
	if s.opts.region == "" || s.opts.auth == cloud.AuthParamImplicit {
		var sp *tracing.Span
		ctx, sp = tracing.ChildSpan(ctx, "s3.newClient")
		defer sp.Finish()
	}

	var loadOptions []func(options *config.LoadOptions) error
	addLoadOption := func(option config.LoadOptionsFunc) {
		loadOptions = append(loadOptions, option)
	}

	client, err := cloud.MakeHTTPClient(s.settings, s.metrics, "aws", s.opts.bucket, s.storageOptions.ClientName)
	if err != nil {
		return s3Client{}, "", err
	}
	addLoadOption(config.WithHTTPClient(client))

	// TODO(yevgeniy): Revisit retry logic.  Retrying 10 times seems arbitrary.
	retryMaxAttempts := 10
	addLoadOption(config.WithRetryMaxAttempts(retryMaxAttempts))

	addLoadOption(config.WithLogger(newLogAdapter(ctx)))
	if s.opts.verbose {
		addLoadOption(config.WithClientLogMode(awsVerboseLogging))
	}

	config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(opts *retry.StandardOptions) {
			opts.MaxAttempts = retryMaxAttempts
			opts.Retryables = append(opts.Retryables, &customRetryer{})
		})
	})

	switch s.opts.auth {
	case "", cloud.AuthParamSpecified:
		addLoadOption(config.WithCredentialsProvider(
			aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(s.opts.accessKey, s.opts.secret, s.opts.tempToken))))
	case cloud.AuthParamImplicit:
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return s3Client{}, "", errors.Wrap(err, "could not initialize an aws config")
	}

	var endpointURI string
	if s.opts.endpoint != "" {
		var err error
		endpointURI, err = constructEndpointURI(s.opts.endpoint)
		if err != nil {
			return s3Client{}, "", err
		}
	}

	if s.opts.assumeRoleProvider.roleARN != "" {
		for _, delegateProvider := range s.opts.delegateRoleProviders {
			client := sts.NewFromConfig(cfg, func(options *sts.Options) {
				if endpointURI != "" {
					options.BaseEndpoint = aws.String(endpointURI)
				}
			})
			intermediateCreds := stscreds.NewAssumeRoleProvider(client, delegateProvider.roleARN, withExternalID(delegateProvider.externalID))
			cfg.Credentials = aws.NewCredentialsCache(intermediateCreds)
		}

		client := sts.NewFromConfig(cfg, func(options *sts.Options) {
			if endpointURI != "" {
				options.BaseEndpoint = aws.String(endpointURI)
			}
		})

		creds := stscreds.NewAssumeRoleProvider(client, s.opts.assumeRoleProvider.roleARN, withExternalID(s.opts.assumeRoleProvider.externalID))
		cfg.Credentials = creds
	}

	region := s.opts.region
	if region == "" {
		// Set a hint because we have no region specified, we will override this
		// below once we get the actual bucket region.
		cfg.Region = "us-east-1"
		if err := cloud.DelayedRetry(ctx, "s3manager.GetBucketRegion", s3ErrDelay, func() error {
			region, err = manager.GetBucketRegion(ctx, s3.NewFromConfig(cfg, func(options *s3.Options) {
				if endpointURI != "" {
					options.BaseEndpoint = aws.String(endpointURI)
				}
				if s.opts.usePathStyle {
					options.UsePathStyle = true
				}
			}), s.opts.bucket)
			return err
		}); err != nil {
			return s3Client{}, "", errors.Wrap(err, "could not find s3 bucket's region")
		}
	}
	cfg.Region = region

	c := s3.NewFromConfig(cfg, func(options *s3.Options) {
		if endpointURI != "" {
			options.BaseEndpoint = aws.String(endpointURI)
		}
		if s.opts.usePathStyle {
			options.UsePathStyle = true
		}
	})
	u := manager.NewUploader(c, func(uploader *manager.Uploader) {
		uploader.PartSize = cloud.WriteChunkSize.Get(&s.settings.SV)
	})
	return s3Client{client: c, uploader: u}, region, nil
}

func (s *s3Storage) getClient(ctx context.Context) (*s3.Client, error) {
	if s.cached != nil {
		return s.cached.client, nil
	}
	client, region, err := s.newClient(ctx)
	if err != nil {
		return nil, err
	}
	if s.opts.region == "" {
		s.opts.region = region
	}
	return client.client, nil
}

func (s *s3Storage) getUploader(ctx context.Context) (*manager.Uploader, error) {
	if s.cached != nil {
		return s.cached.uploader, nil
	}
	client, region, err := s.newClient(ctx)
	if err != nil {
		return nil, err
	}
	if s.opts.region == "" {
		s.opts.region = region
	}
	return client.uploader, nil
}

func (s *s3Storage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider: cloudpb.ExternalStorageProvider_s3,
		S3Config: s.conf,
	}
}

func (s *s3Storage) ExternalIOConf() base.ExternalIODirConfig {
	return s.ioConf
}

func (s *s3Storage) RequiresExternalIOAccounting() bool { return true }

func (s *s3Storage) Settings() *cluster.Settings {
	return s.settings
}

type putUploader struct {
	b      *bytes.Buffer
	client *s3.Client
	input  *s3.PutObjectInput
}

func (u *putUploader) Write(p []byte) (int, error) {
	return u.b.Write(p)
}

func (u *putUploader) Close() error {
	u.input.Body = bytes.NewReader(u.b.Bytes())
	// TODO(adityamaru): plumb a ctx through to close.
	_, err := u.client.PutObject(context.Background(), u.input)
	return err
}

func (s *s3Storage) putUploader(ctx context.Context, basename string) (io.WriteCloser, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(make([]byte, 0, 4<<20))

	return &putUploader{
		b: buf,
		input: &s3.PutObjectInput{
			Bucket:               s.bucket,
			Key:                  aws.String(path.Join(s.prefix, basename)),
			ServerSideEncryption: types.ServerSideEncryption(s.conf.ServerEncMode),
			SSEKMSKeyId:          nilIfEmpty(s.conf.ServerKMSID),
			StorageClass:         types.StorageClass(s.conf.StorageClass),
			ChecksumAlgorithm:    checksumAlgorithm,
		},
		client: client,
	}, nil
}

func (s *s3Storage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	if usePutObject.Get(&s.settings.SV) {
		return s.putUploader(ctx, basename)
	}

	uploader, err := s.getUploader(ctx)
	if err != nil {
		return nil, err
	}

	ctx, sp := tracing.ChildSpan(ctx, "s3.Writer")
	sp.SetTag("path", attribute.StringValue(path.Join(s.prefix, basename)))
	return cloud.BackgroundPipe(ctx, func(ctx context.Context, r io.Reader) error {
		defer sp.Finish()
		// Upload the file to S3.
		// TODO(dt): test and tune the uploader parameters.
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:               s.bucket,
			Key:                  aws.String(path.Join(s.prefix, basename)),
			Body:                 r,
			ServerSideEncryption: types.ServerSideEncryption(s.conf.ServerEncMode),
			SSEKMSKeyId:          nilIfEmpty(s.conf.ServerKMSID),
			StorageClass:         types.StorageClass(s.conf.StorageClass),
			ChecksumAlgorithm:    checksumAlgorithm,
		})
		err = interpretAWSError(err)
		err = errors.Wrap(err, "upload failed")
		// Mark with ctx's error for upstream code to not interpret this as
		// corruption.
		if ctx.Err() != nil {
			err = errors.Mark(err, ctx.Err())
		}
		return err
	}), nil
}

// openStreamAt opens a stream of object data, starting at offset <pos>.
// If endPos is non-zero, returns data up to that offset (exclusive).
func (s *s3Storage) openStreamAt(
	ctx context.Context, basename string, pos int64, endPos int64,
) (*s3.GetObjectOutput, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}
	req := &s3.GetObjectInput{Bucket: s.bucket, Key: aws.String(path.Join(s.prefix, basename))}
	if endPos != 0 {
		if pos >= endPos {
			return nil, io.EOF
		}
		// Range header end position is inclusive.
		req.Range = aws.String(fmt.Sprintf("bytes=%d-%d", pos, endPos-1))
	} else if pos != 0 {
		req.Range = aws.String(fmt.Sprintf("bytes=%d-", pos))
	}

	out, err := client.GetObject(ctx, req)
	if err != nil {
		err = interpretAWSError(err)
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			// keep this string in case anyone is depending on it
			err = errors.Wrap(err, "s3 object does not exist")
		}
		err = errors.Wrap(err, "failed to get s3 object")
		// Mark with ctx's error for upstream code to not interpret this as
		// corruption.
		if ctx.Err() != nil {
			err = errors.Mark(err, ctx.Err())
		}
		return nil, err
	}
	return out, nil
}

// ReadFile is part of the cloud.ExternalStorage interface.
func (s *s3Storage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (_ ioctx.ReadCloserCtx, fileSize int64, _ error) {
	ctx, sp := tracing.ChildSpan(ctx, "s3.ReadFile")
	defer sp.Finish()

	path := path.Join(s.prefix, basename)
	sp.SetTag("path", attribute.StringValue(path))
	endOffset := int64(0)
	if opts.LengthHint != 0 {
		endOffset = opts.Offset + opts.LengthHint
	}

	stream, err := s.openStreamAt(ctx, basename, opts.Offset, endOffset)
	if err != nil {
		return nil, 0, err
	}
	if !opts.NoFileSize {
		if opts.Offset != 0 {
			if stream.ContentRange == nil {
				return nil, 0, errors.New("expected content range for read at offset")
			}
			fileSize, err = cloud.CheckHTTPContentRangeHeader(*stream.ContentRange, opts.Offset)
			if err != nil {
				return nil, 0, err
			}
		} else {
			if stream.ContentLength == nil {
				log.Warningf(ctx, "Content length missing from S3 GetObject (is this actually s3?); attempting to lookup size with separate call...")
				// Some not-actually-s3 services may not set it, or set it in a way the
				// official SDK finds it (e.g. if they don't use the expected checksummer)
				// so try a Size() request.
				x, err := s.Size(ctx, basename)
				if err != nil {
					return nil, 0, errors.Wrap(err, "content-length missing from GetObject and Size() failed")
				}
				fileSize = x
			} else {
				fileSize = *stream.ContentLength
			}
		}
	}
	opener := func(ctx context.Context, pos int64) (io.ReadCloser, int64, error) {
		s, err := s.openStreamAt(ctx, basename, pos, endOffset)
		if err != nil {
			return nil, 0, err
		}
		return s.Body, fileSize, nil
	}
	return cloud.NewResumingReader(ctx, opener, stream.Body, opts.Offset, fileSize, path,
		cloud.ResumingReaderRetryOnErrFnForSettings(ctx, s.settings), s3ErrDelay), fileSize, nil
}

func (s *s3Storage) List(ctx context.Context, prefix, delim string, fn cloud.ListingFn) error {
	ctx, sp := tracing.ChildSpan(ctx, "s3.List")
	defer sp.Finish()

	dest := cloud.JoinPathPreservingTrailingSlash(s.prefix, prefix)
	sp.SetTag("path", attribute.StringValue(dest))

	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}

	var s3Input *s3.ListObjectsV2Input
	// Add an environment variable toggle for s3 storage to list prefixes with a
	// paging marker that's the prefix with an additional /. This allows certain
	// s3 clones which return s3://<prefix>/ as the first result of listing
	// s3://<prefix> to exclude that result.
	if envutil.EnvOrDefaultBool("COCKROACH_S3_LIST_WITH_PREFIX_SLASH_MARKER", false) {
		s3Input = &s3.ListObjectsV2Input{Bucket: s.bucket, Prefix: aws.String(dest), Delimiter: nilIfEmpty(delim), StartAfter: aws.String(dest + "/")}
	} else {
		s3Input = &s3.ListObjectsV2Input{Bucket: s.bucket, Prefix: aws.String(dest), Delimiter: nilIfEmpty(delim)}
	}

	paginator := s3.NewListObjectsV2Paginator(client, s3Input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			err = interpretAWSError(err)
			err = errors.Wrap(err, `failed to list s3 bucket`)
			// Mark with ctx's error for upstream code to not interpret this as
			// corruption.
			if ctx.Err() != nil {
				err = errors.Mark(err, ctx.Err())
			}
			return err
		}

		for _, x := range page.CommonPrefixes {
			if err := fn(strings.TrimPrefix(*x.Prefix, dest)); err != nil {
				return err
			}
		}

		for _, fileObject := range page.Contents {
			if err := fn(strings.TrimPrefix(*fileObject.Key, dest)); err != nil {
				return err
			}
		}
	}
	return nil
}

// interpretAWSError attempts to surface safe information that otherwise would be redacted.
//
// We could mark the err with the Context.Err() if aerr.Code() is
// request.CanceledErrorCode, instead of doing it in the caller. But this
// requires knowing something about the SDK implementation (that the request.*
// error codes are relevant, in addition to the s3.* error codes).
func interpretAWSError(err error) error {
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "AssumeRole") {
		err = errors.Wrap(err, "AssumeRole")
	}

	if strings.Contains(err.Error(), "AccessDenied") {
		err = errors.Wrap(err, "AccessDenied")
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()

		if code != "" {
			// nolint:errwrap
			err = errors.Wrapf(err, "%v", code)

			noSuchBucket := types.NoSuchBucket{}
			noSuchKey := types.NoSuchKey{}
			switch code {
			// Relevant 404 errors reported by AWS.
			case noSuchBucket.ErrorCode(), noSuchKey.ErrorCode():
				// nolint:errwrap
				err = errors.Wrapf(
					errors.Wrap(cloud.ErrFileDoesNotExist, "s3 object does not exist"),
					"%v",
					err.Error(),
				)
			}
		}
	}

	return err
}

func (s *s3Storage) Delete(ctx context.Context, basename string) error {
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}
	// TODO(sumeer): the timeout error could be interpreted as corruption in
	// upstream CockroachDB code that is transitively using this for Pebble's
	// disaggregated storage. Have a different implementation for that code path
	// that only uses ctx cancellation.
	return timeutil.RunWithTimeout(ctx, "delete s3 object",
		cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
			})
			return err
		})
}

func (s *s3Storage) Size(ctx context.Context, basename string) (int64, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return 0, err
	}
	var out *s3.HeadObjectOutput
	// TODO(sumeer): the timeout error could be interpreted as corruption in
	// upstream CockroachDB code that is transitively using this for Pebble's
	// disaggregated storage. Have a different implementation for that code path
	// that only uses ctx cancellation.
	err = timeutil.RunWithTimeout(ctx, "get s3 object header",
		cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			var err error
			out, err = client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
			})
			return err
		})
	if err != nil {
		err = interpretAWSError(err)
		err = errors.Wrap(err, "failed to get s3 object headers")
		// Mark with ctx's error for upstream code to not interpret this as
		// corruption.
		if ctx.Err() != nil {
			err = errors.Mark(err, ctx.Err())
		}
		return 0, err
	}
	return *out.ContentLength, nil
}

func (s *s3Storage) Close() error {
	return nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

func s3ErrDelay(err error) time.Duration {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		// A 503 error could mean we need to reduce our request rate. Impose an
		// arbitrary slowdown in that case.
		// See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
		if re.HTTPStatusCode() == 503 {
			return time.Second * 5
		}
	}
	return 0
}

func withExternalID(externalID string) func(p *stscreds.AssumeRoleOptions) {
	return func(p *stscreds.AssumeRoleOptions) {
		if externalID != "" {
			p.ExternalID = aws.String(externalID)
		}
	}
}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_s3,
		cloud.RegisteredProvider{
			EarlyBootConstructFn: MakeS3Storage,
			EarlyBootParseFn:     parseS3URL,

			RedactedParams: cloud.RedactedParams(AWSSecretParam, AWSTempTokenParam),
			Schemes:        []string{scheme},
		})
}
