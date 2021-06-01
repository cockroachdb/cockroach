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
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	// AWSServerSideEncryptionMode is the query parameter in an AWS URI, for the
	// mode to be used for server side encryption. It can either be AES256 or
	// aws:kms.
	AWSServerSideEncryptionMode = "AWS_SERVER_ENC_MODE"

	// AWSServerSideEncryptionKMSID is the query parameter in an AWS URI, for the
	// KMS ID to be used for server side encryption.
	AWSServerSideEncryptionKMSID = "AWS_SERVER_KMS_ID"

	// S3RegionParam is the query parameter for the 'endpoint' in an S3 URI.
	S3RegionParam = "AWS_REGION"

	// KMSRegionParam is the query parameter for the 'region' in every KMS URI.
	KMSRegionParam = "REGION"
)

type s3Storage struct {
	bucket   *string
	conf     *roachpb.ExternalStorage_S3
	ioConf   base.ExternalIODirConfig
	prefix   string
	opts     session.Options
	settings *cluster.Settings
}

var _ cloud.ExternalStorage = &s3Storage{}

type serverSideEncMode string

const (
	kmsEnc    serverSideEncMode = "aws:kms"
	aes256Enc serverSideEncMode = "AES256"
)

// S3URI returns the string URI for a given bucket and path.
func S3URI(bucket, path string, conf *roachpb.ExternalStorage_S3) string {
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

	s3URL := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     path,
		RawQuery: q.Encode(),
	}

	return s3URL.String()
}

func parseS3URL(_ cloud.ExternalStorageURIContext, uri *url.URL) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	conf.Provider = roachpb.ExternalStorageProvider_s3
	conf.S3Config = &roachpb.ExternalStorage_S3{
		Bucket:        uri.Host,
		Prefix:        uri.Path,
		AccessKey:     uri.Query().Get(AWSAccessKeyParam),
		Secret:        uri.Query().Get(AWSSecretParam),
		TempToken:     uri.Query().Get(AWSTempTokenParam),
		Endpoint:      uri.Query().Get(AWSEndpointParam),
		Region:        uri.Query().Get(S3RegionParam),
		Auth:          uri.Query().Get(cloud.AuthParam),
		ServerEncMode: uri.Query().Get(AWSServerSideEncryptionMode),
		ServerKMSID:   uri.Query().Get(AWSServerSideEncryptionKMSID),
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
	return conf, nil
}

// MakeS3Storage returns an instance of S3 ExternalStorage.
func MakeS3Storage(
	ctx context.Context, args cloud.ExternalStorageContext, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.s3")
	conf := dest.S3Config
	if conf == nil {
		return nil, errors.Errorf("s3 upload requested but info missing")
	}
	config := conf.Keys()
	if conf.Endpoint != "" {
		if args.IOConf.DisableHTTP {
			return nil, errors.New(
				"custom endpoints disallowed for s3 due to --external-io-disable-http flag")
		}
		config.Endpoint = &conf.Endpoint
		if conf.Region == "" {
			conf.Region = "default-region"
		}
		client, err := cloud.MakeHTTPClient(args.Settings)
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
		opts.Config.MergeIn(config)
	case cloud.AuthParamImplicit:
		if args.IOConf.DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for s3 due to --external-io-implicit-credentials flag")
		}
		opts.SharedConfigState = session.SharedConfigEnable
	default:
		return nil, errors.Errorf("unsupported value %s for %s", conf.Auth, cloud.AuthParam)
	}

	// TODO(yevgeniy): Revisit retry logic.  Retrying 10 times seems arbitrary.
	maxRetries := 10
	opts.Config.MaxRetries = &maxRetries

	if conf.Endpoint != "" {
		opts.Config.S3ForcePathStyle = aws.Bool(true)
	}
	if log.V(2) {
		opts.Config.LogLevel = aws.LogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
		opts.Config.CredentialsChainVerboseErrors = aws.Bool(true)
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

	return &s3Storage{
		bucket:   aws.String(conf.Bucket),
		conf:     conf,
		ioConf:   args.IOConf,
		prefix:   conf.Prefix,
		opts:     opts,
		settings: args.Settings,
	}, nil
}

func s3ErrDelay(err error) time.Duration {
	var s3err s3.RequestFailure
	if errors.As(err, &s3err) {
		// A 503 error could mean we need to reduce our request rate. Impose an
		// arbitrary slowdown in that case.
		// See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
		if s3err.StatusCode() == 503 {
			return time.Second * 5
		}
	}
	return 0
}

func (s *s3Storage) newSession(ctx context.Context) (*session.Session, error) {
	sess, err := session.NewSessionWithOptions(s.opts)
	if err != nil {
		return nil, errors.Wrap(err, "new aws session")
	}
	if s.conf.Region == "" {
		if err := cloud.DelayedRetry(ctx, s3ErrDelay, func() error {
			var err error
			s.conf.Region, err = s3manager.GetBucketRegion(ctx, sess, s.conf.Bucket, "us-east-1")
			return err
		}); err != nil {
			return nil, errors.Wrap(err, "could not find s3 bucket's region")
		}
	}
	sess.Config.Region = aws.String(s.conf.Region)
	return sess, err
}

func (s *s3Storage) newS3Client(ctx context.Context) (*s3.S3, error) {
	sess, err := s.newSession(ctx)
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func (s *s3Storage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider: roachpb.ExternalStorageProvider_s3,
		S3Config: s.conf,
	}
}

func (s *s3Storage) ExternalIOConf() base.ExternalIODirConfig {
	return s.ioConf
}

func (s *s3Storage) Settings() *cluster.Settings {
	return s.settings
}

func (s *s3Storage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	sess, err := s.newSession(ctx)
	if err != nil {
		return nil, err
	}
	uploader := s3manager.NewUploader(sess)

	return cloud.BackgroundPipe(ctx, func(ctx context.Context, r io.Reader) error {
		// Upload the file to S3.
		// TODO(dt): test and tune the uploader parameters.
		_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket:               s.bucket,
			Key:                  aws.String(path.Join(s.prefix, basename)),
			Body:                 r,
			ServerSideEncryption: nilIfEmpty(s.conf.ServerEncMode),
			SSEKMSKeyId:          nilIfEmpty(s.conf.ServerKMSID),
		})
		return errors.Wrap(err, "upload failed")
	}), nil
}

func (s *s3Storage) openStreamAt(
	ctx context.Context, basename string, pos int64,
) (*s3.GetObjectOutput, error) {
	client, err := s.newS3Client(ctx)
	if err != nil {
		return nil, err
	}
	req := &s3.GetObjectInput{Bucket: s.bucket, Key: aws.String(path.Join(s.prefix, basename))}
	if pos != 0 {
		req.Range = aws.String(fmt.Sprintf("bytes=%d-", pos))
	}

	out, err := client.GetObjectWithContext(ctx, req)
	if err != nil {
		if aerr := (awserr.Error)(nil); errors.As(err, &aerr) {
			switch aerr.Code() {
			// Relevant 404 errors reported by AWS.
			case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey:
				return nil, errors.Wrapf(cloud.ErrFileDoesNotExist, "s3 object does not exist: %s", err.Error())
			}
		}
		return nil, errors.Wrap(err, "failed to get s3 object")
	}
	return out, nil
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (s *s3Storage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	reader, _, err := s.ReadFileAt(ctx, basename, 0)
	return reader, err
}

// ReadFileAt opens a reader at the requested offset.
func (s *s3Storage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (io.ReadCloser, int64, error) {
	stream, err := s.openStreamAt(ctx, basename, offset)
	if err != nil {
		return nil, 0, err
	}
	var size int64
	if offset != 0 {
		if stream.ContentRange == nil {
			return nil, 0, errors.New("expected content range for read at offset")
		}
		size, err = cloud.CheckHTTPContentRangeHeader(*stream.ContentRange, offset)
		if err != nil {
			return nil, 0, err
		}
	} else {
		if stream.ContentLength == nil {
			return nil, 0, errors.New("expected content length")
		}
		size = *stream.ContentLength
	}

	return &cloud.ResumingReader{
		Ctx: ctx,
		Opener: func(ctx context.Context, pos int64) (io.ReadCloser, error) {
			s, err := s.openStreamAt(ctx, basename, pos)
			if err != nil {
				return nil, err
			}
			return s.Body, nil
		},
		Reader:       stream.Body,
		Pos:          offset,
		RetryOnErrFn: cloud.IsResumableHTTPError,
		ErrFn:        s3ErrDelay,
	}, size, nil
}

func (s *s3Storage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	var fileList []string

	pattern := s.prefix
	if patternSuffix != "" {
		if cloud.ContainsGlob(s.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = path.Join(pattern, patternSuffix)
	}
	client, err := s.newS3Client(ctx)
	if err != nil {
		return nil, err
	}

	var matchErr error
	err = client.ListObjectsPagesWithContext(
		ctx,
		&s3.ListObjectsInput{
			Bucket: s.bucket,
			Prefix: aws.String(cloud.GetPrefixBeforeWildcard(s.prefix)),
		},
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, fileObject := range page.Contents {
				matches, err := path.Match(pattern, *fileObject.Key)
				if err != nil {
					matchErr = err
					return false
				}
				if matches {
					if patternSuffix != "" {
						if !strings.HasPrefix(*fileObject.Key, s.prefix) {
							// TODO(dt): return a nice rel-path instead of erroring out.
							matchErr = errors.New("pattern matched file outside of path")
							return false
						}
						fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(*fileObject.Key, s.prefix), "/"))
					} else {

						fileList = append(fileList, S3URI(*s.bucket, *fileObject.Key, s.conf))
					}
				}
			}
			return !lastPage
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, `failed to list s3 bucket`)
	}
	if matchErr != nil {
		return nil, errors.Wrap(matchErr, `failed to list s3 bucket`)
	}

	return fileList, nil
}

func (s *s3Storage) Delete(ctx context.Context, basename string) error {
	client, err := s.newS3Client(ctx)
	if err != nil {
		return err
	}
	return contextutil.RunWithTimeout(ctx, "delete s3 object",
		cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
			})
			return err
		})
}

func (s *s3Storage) Size(ctx context.Context, basename string) (int64, error) {
	client, err := s.newS3Client(ctx)
	if err != nil {
		return 0, err
	}
	var out *s3.HeadObjectOutput
	err = contextutil.RunWithTimeout(ctx, "get s3 object header",
		cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			var err error
			out, err = client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
			})
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "failed to get s3 object headers")
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

func init() {
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_s3,
		parseS3URL, MakeS3Storage, cloud.RedactedParams(AWSSecretParam, AWSTempTokenParam), "s3")
}
