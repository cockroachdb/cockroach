// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpl

import (
	"context"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
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

	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"

	// GoogleBillingProjectParam is the query parameter for the billing project
	// in a gs URI.
	GoogleBillingProjectParam = "GOOGLE_BILLING_PROJECT"

	// AuthParam is the query parameter for the cluster settings named
	// key in a URI.
	AuthParam = "AUTH"
	// AuthParamImplicit is the query parameter for the implicit authentication
	// mode in a URI.
	AuthParamImplicit = "implicit"
	// AuthParamDefault is the query parameter for the default authentication
	// mode in a URI.
	AuthParamDefault = "default"
	// AuthParamSpecified is the query parameter for the specified authentication
	// mode in a URI.
	AuthParamSpecified = "specified"

	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"

	cloudstoragePrefix = "cloudstorage"
	cloudstorageGS     = cloudstoragePrefix + ".gs"
	cloudstorageHTTP   = cloudstoragePrefix + ".http"

	cloudstorageDefault = ".default"
	cloudstorageKey     = ".key"

	cloudstorageGSDefault = cloudstorageGS + cloudstorageDefault
	// CloudstorageGSDefaultKey is the setting whose value is the JSON key to use
	// during Google Cloud Storage operations.
	CloudstorageGSDefaultKey = cloudstorageGSDefault + cloudstorageKey

	// CloudstorageHTTPCASetting is the setting whose value is the custom root CA
	// (appended to system's default CAs) for verifying certificates when
	// interacting with HTTPS storage.
	CloudstorageHTTPCASetting = cloudstorageHTTP + ".custom_ca"

	cloudStorageTimeout = cloudstoragePrefix + ".timeout"
)

// See SanitizeExternalStorageURI.
var redactedQueryParams = map[string]struct{}{
	AWSSecretParam:       {},
	AWSTempTokenParam:    {},
	AzureAccountKeyParam: {},
	CredentialsParam:     {},
}

// ErrListingUnsupported is a marker for indicating listing is unsupported.
var ErrListingUnsupported = errors.New("listing is not supported")

// ErrFileDoesNotExist is a sentinel error for indicating that a specified
// bucket/object/key/file (depending on storage terminology) does not exist.
// This error is raised by the ReadFile method.
var ErrFileDoesNotExist = errors.New("external_storage: file doesn't exist")

func init() {
	cloud.AccessIsWithExplicitAuth = AccessIsWithExplicitAuth
}

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(
	path string, user security.SQLUsername,
) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	uri, err := url.Parse(path)
	if err != nil {
		return conf, err
	}
	switch uri.Scheme {
	case "s3":
		conf.Provider = roachpb.ExternalStorageProvider_S3
		conf.S3Config = &roachpb.ExternalStorage_S3{
			Bucket:        uri.Host,
			Prefix:        uri.Path,
			AccessKey:     uri.Query().Get(AWSAccessKeyParam),
			Secret:        uri.Query().Get(AWSSecretParam),
			TempToken:     uri.Query().Get(AWSTempTokenParam),
			Endpoint:      uri.Query().Get(AWSEndpointParam),
			Region:        uri.Query().Get(S3RegionParam),
			Auth:          uri.Query().Get(AuthParam),
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
	case "gs":
		conf.Provider = roachpb.ExternalStorageProvider_GoogleCloud
		conf.GoogleCloudConfig = &roachpb.ExternalStorage_GCS{
			Bucket:         uri.Host,
			Prefix:         uri.Path,
			Auth:           uri.Query().Get(AuthParam),
			BillingProject: uri.Query().Get(GoogleBillingProjectParam),
			Credentials:    uri.Query().Get(CredentialsParam),
			/* NB: additions here should also update gcsQueryParams() serializer */
		}
		conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	case "azure":
		conf.Provider = roachpb.ExternalStorageProvider_Azure
		conf.AzureConfig = &roachpb.ExternalStorage_Azure{
			Container:   uri.Host,
			Prefix:      uri.Path,
			AccountName: uri.Query().Get(AzureAccountNameParam),
			AccountKey:  uri.Query().Get(AzureAccountKeyParam),
			/* NB: additions here should also update azureQueryParams() serializer */
		}
		if conf.AzureConfig.AccountName == "" {
			return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountNameParam)
		}
		if conf.AzureConfig.AccountKey == "" {
			return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountKeyParam)
		}
		conf.AzureConfig.Prefix = strings.TrimLeft(conf.AzureConfig.Prefix, "/")
	case "http", "https":
		conf.Provider = roachpb.ExternalStorageProvider_Http
		conf.HttpPath.BaseUri = path
	case "nodelocal":
		if uri.Host == "" {
			return conf, errors.Errorf(
				"host component of nodelocal URI must be a node ID ("+
					"use 'self' to specify each node should access its own local filesystem): %s",
				path,
			)
		} else if uri.Host == "self" {
			uri.Host = "0"
		}

		nodeID, err := strconv.Atoi(uri.Host)
		if err != nil {
			return conf, errors.Errorf("host component of nodelocal URI must be a node ID: %s", path)
		}
		conf.Provider = roachpb.ExternalStorageProvider_LocalFile
		conf.LocalFile.Path = uri.Path
		conf.LocalFile.NodeID = roachpb.NodeID(nodeID)
	case "experimental-workload", "workload":
		conf.Provider = roachpb.ExternalStorageProvider_Workload
		if conf.WorkloadConfig, err = ParseWorkloadConfig(uri); err != nil {
			return conf, err
		}
	case "userfile":
		qualifiedTableName := uri.Host
		if user.Undefined() {
			return conf, errors.Errorf("user creating the FileTable ExternalStorage must be specified")
		}

		// If the import statement does not specify a qualified table name then use
		// the default to attempt to locate the file(s).
		if qualifiedTableName == "" {
			composedTableName := security.MakeSQLUsernameFromPreNormalizedString(
				DefaultQualifiedNamePrefix + user.Normalized())
			qualifiedTableName = DefaultQualifiedNamespace +
				// Escape special identifiers as needed.
				composedTableName.SQLIdentifier()
		}

		conf.Provider = roachpb.ExternalStorageProvider_FileTable
		conf.FileTableConfig.User = user.Normalized()
		conf.FileTableConfig.QualifiedTableName = qualifiedTableName
		conf.FileTableConfig.Path = uri.Path
	default:
		// TODO(adityamaru): Link dedicated ExternalStorage scheme docs once ready.
		return conf, errors.Errorf("unsupported storage scheme: %q - refer to docs to find supported"+
			" storage schemes", uri.Scheme)
	}
	return conf, nil
}

// ExternalStorageFromURI returns an ExternalStorage for the given URI.
func ExternalStorageFromURI(
	ctx context.Context,
	uri string,
	externalConfig base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	user security.SQLUsername,
	ie *sql.InternalExecutor,
	kvDB *kv.DB,
) (cloud.ExternalStorage, error) {
	conf, err := ExternalStorageConfFromURI(uri, user)
	if err != nil {
		return nil, err
	}
	return MakeExternalStorage(ctx, conf, externalConfig, settings, blobClientFactory, ie, kvDB)
}

// SanitizeExternalStorageURI returns the external storage URI with with some
// secrets redacted, for use when showing these URIs in the UI, to provide some
// protection from shoulder-surfing. The param is still present -- just
// redacted -- to make it clearer that that value is indeed persisted interally.
// extraParams which should be scrubbed -- for params beyond those that the
// various clound-storage URIs supported by this package know about -- can be
// passed allowing this function to be used to scrub other URIs too (such as
// non-cloudstorage changefeed sinks).
func SanitizeExternalStorageURI(path string, extraParams []string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "experimental-workload" || uri.Scheme == "workload" {
		return path, nil
	}

	params := uri.Query()
	for param := range params {
		if _, ok := redactedQueryParams[param]; ok {
			params.Set(param, "redacted")
		} else {
			for _, p := range extraParams {
				if param == p {
					params.Set(param, "redacted")
				}
			}
		}
	}

	uri.RawQuery = params.Encode()
	return uri.String(), nil
}

// MakeExternalStorage creates an ExternalStorage from the given config.
func MakeExternalStorage(
	ctx context.Context,
	dest roachpb.ExternalStorage,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	ie *sql.InternalExecutor,
	kvDB *kv.DB,
) (cloud.ExternalStorage, error) {
	if conf.DisableOutbound && dest.Provider != roachpb.ExternalStorageProvider_FileTable {
		return nil, errors.New("external network access is disabled")
	}
	switch dest.Provider {
	case roachpb.ExternalStorageProvider_LocalFile:
		telemetry.Count("external-io.nodelocal")
		if blobClientFactory == nil {
			return nil, errors.New("nodelocal storage is not available")
		}
		return makeLocalStorage(ctx, dest.LocalFile, settings, blobClientFactory, conf)
	case roachpb.ExternalStorageProvider_Http:
		if conf.DisableHTTP {
			return nil, errors.New("external http access disabled")
		}
		telemetry.Count("external-io.http")
		return MakeHTTPStorage(dest.HttpPath.BaseUri, settings, conf)
	case roachpb.ExternalStorageProvider_S3:
		telemetry.Count("external-io.s3")
		return MakeS3Storage(ctx, conf, dest.S3Config, settings)
	case roachpb.ExternalStorageProvider_GoogleCloud:
		telemetry.Count("external-io.google_cloud")
		return makeGCSStorage(ctx, conf, dest.GoogleCloudConfig, settings)
	case roachpb.ExternalStorageProvider_Azure:
		telemetry.Count("external-io.azure")
		return makeAzureStorage(dest.AzureConfig, settings, conf)
	case roachpb.ExternalStorageProvider_Workload:
		telemetry.Count("external-io.workload")
		return makeWorkloadStorage(dest.WorkloadConfig, settings, conf)
	case roachpb.ExternalStorageProvider_FileTable:
		telemetry.Count("external-io.filetable")
		return makeFileTableStorage(ctx, dest.FileTableConfig, ie, kvDB, settings, conf)
	}
	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}

// URINeedsGlobExpansion checks if URI can be expanded by checking if it contains wildcard characters.
// This should be used before passing a URI into ListFiles().
func URINeedsGlobExpansion(uri string) bool {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return false
	}
	// We don't support listing files for workload and http.
	unsupported := []string{"workload", "http", "https", "experimental-workload"}
	for _, str := range unsupported {
		if parsedURI.Scheme == str {
			return false
		}
	}

	return containsGlob(parsedURI.Path)
}

// AccessIsWithExplicitAuth checks if the provided ExternalStorage URI has
// explicit authentication i.e does not rely on implicit machine credentials to
// access the resource.
// The following scenarios are considered implicit access:
//
// - implicit AUTH: access will use the node's machine account and only a
// super user should have the authority to use these credentials.
//
// - HTTP/HTTPS/Custom endpoint: requests are made by the server, in the
// server's network, potentially behind a firewall and only a super user should
// be able to do this.
//
// - nodelocal: this is the node's shared filesystem and so only a super user
// should be able to interact with it.
func AccessIsWithExplicitAuth(path string) (bool, string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return false, "", err
	}
	hasExplicitAuth := false
	switch uri.Scheme {
	case "s3":
		auth := uri.Query().Get(AuthParam)
		hasExplicitAuth = auth == AuthParamSpecified || auth == ""

		// If a custom endpoint has been specified in the S3 URI then this is no
		// longer an explicit AUTH.
		hasExplicitAuth = hasExplicitAuth && uri.Query().Get(AWSEndpointParam) == ""
	case "gs":
		auth := uri.Query().Get(AuthParam)
		hasExplicitAuth = auth == AuthParamSpecified
	case "azure":
		// Azure does not support implicit authentication i.e. all credentials have
		// to be specified as part of the URI.
		hasExplicitAuth = true
	case "http", "https", "nodelocal":
		hasExplicitAuth = false
	case "experimental-workload", "workload", "userfile":
		hasExplicitAuth = true
	default:
		return hasExplicitAuth, "", nil
	}
	return hasExplicitAuth, uri.Scheme, nil
}

func containsGlob(str string) bool {
	return strings.ContainsAny(str, "*?[")
}

var (
	// GcsDefault is the setting which defines the JSON key to use during GCS
	// operations.
	GcsDefault = settings.RegisterStringSetting(
		CloudstorageGSDefaultKey,
		"[deprecated] if set, JSON key to use during Google Cloud Storage operations. "+
			"This setting will be removed in "+
			"21.2, as we will no longer support the `default` AUTH mode for GCS operations.",
		"",
	).WithPublic()
	httpCustomCA = settings.RegisterStringSetting(
		CloudstorageHTTPCASetting,
		"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
		"",
	).WithPublic()
	timeoutSetting = settings.RegisterDurationSetting(
		cloudStorageTimeout,
		"the timeout for import/export storage operations",
		10*time.Minute,
	).WithPublic()
)

// delayedRetry runs fn and re-runs it a limited number of times if it
// fails. It knows about specific kinds of errors that need longer retry
// delays than normal.
func delayedRetry(ctx context.Context, fn func() error) error {
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), MaxDelayedRetryAttempts, func() error {
		err := fn()
		if err == nil {
			return nil
		}
		var s3err s3.RequestFailure
		if errors.As(err, &s3err) {
			// A 503 error could mean we need to reduce our request rate. Impose an
			// arbitrary slowdown in that case.
			// See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
			if s3err.StatusCode() == 503 {
				select {
				case <-time.After(time.Second * 5):
				case <-ctx.Done():
				}
			}
		}
		// See https:github.com/GoogleCloudPlatform/google-cloudimpl-go/issues/1012#issuecomment-393606797
		// which suggests this GCE error message could be due to auth quota limits
		// being reached.
		if strings.Contains(err.Error(), "net/http: timeout awaiting response headers") {
			select {
			case <-time.After(time.Second * 5):
			case <-ctx.Done():
			}
		}
		return err
	})
}

// isResumableHTTPError returns true if we can
// resume download after receiving an error 'err'.
// We can attempt to resume download if the error is ErrUnexpectedEOF.
// In particular, we should not worry about a case when error is io.EOF.
// The reason for this is two-fold:
//   1. The underlying http library converts io.EOF to io.ErrUnexpectedEOF
//   if the number of bytes transferred is less than the number of
//   bytes advertised in the Content-Length header.  So if we see
//   io.ErrUnexpectedEOF we can simply request the next range.
//   2. If the server did *not* advertise Content-Length, then
//   there is really nothing we can do: http standard says that
//   the stream ends when the server terminates connection.
// In addition, we treat connection reset by peer errors (which can
// happen if we didn't read from the connection too long due to e.g. load),
// the same as unexpected eof errors.
func isResumableHTTPError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		sysutil.IsErrConnectionReset(err) ||
		sysutil.IsErrConnectionRefused(err)
}

func getPrefixBeforeWildcard(p string) string {
	globIndex := strings.IndexAny(p, "*?[")
	if globIndex < 0 {
		return p
	}
	return path.Dir(p[:globIndex])
}

// MaxDelayedRetryAttempts is the number of times the delayedRetry method will
// re-run the provided function.
const MaxDelayedRetryAttempts = 3

// Maximum number of times we can attempt to retry reading from external storage,
// without making any progress.
const maxNoProgressReads = 3

type openStreamAt func(ctx context.Context, pos int64) (io.ReadCloser, error)

// resumingReader is a reader which retries reads in case of a transient errors.
type resumingReader struct {
	ctx    context.Context // Reader context
	opener openStreamAt    // Get additional content
	reader io.ReadCloser   // Currently opened reader
	pos    int64           // How much data was received so far
}

var _ io.ReadCloser = &resumingReader{}

func (r *resumingReader) openStream() error {
	return delayedRetry(r.ctx, func() error {
		var readErr error
		r.reader, readErr = r.opener(r.ctx, r.pos)
		return readErr
	})
}

func (r *resumingReader) Read(p []byte) (int, error) {
	var lastErr error
	for retries := 0; lastErr == nil; retries++ {
		if r.reader == nil {
			lastErr = r.openStream()
		}

		if lastErr == nil {
			n, readErr := r.reader.Read(p)
			if readErr == nil || readErr == io.EOF {
				r.pos += int64(n)
				return n, readErr
			}
			lastErr = readErr
		}

		if !errors.IsAny(lastErr, io.EOF, io.ErrUnexpectedEOF) {
			log.Errorf(r.ctx, "Read err: %s", lastErr)
		}

		if isResumableHTTPError(lastErr) {
			if retries >= maxNoProgressReads {
				return 0, errors.Wrap(lastErr, "multiple Read calls return no data")
			}
			log.Errorf(r.ctx, "Retry IO: error %s", lastErr)
			lastErr = nil
			r.reader = nil
		}
	}

	// NB: Go says Read() callers need to expect n > 0 *and* non-nil error, and do
	// something with what was read before the error, but this mostly applies to
	// err = EOF case which we handle above, so likely OK that we're discarding n
	// here and pretending it was zero.
	return 0, lastErr
}

func (r *resumingReader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}

// JoinPathPreservingTrailingSlash wraps path.Join but preserves the trailing
// slash if there was one in the suffix.
//
// This is particularly important when the joined path is used as a prefix for
// listing. When listing, the suffix *after the listed prefix* of each file name
// is what is returned and, importantly, what is used when grouping using a
// delimiter. E.g. when using `/` as a delimiter to find what might be called the
// immediate children in a directory, we pass that directory's path *with a
// trailing slash* as the prefix, so that the children do not start with a slash
// and get grouped into nothing. Thus it is important that if we use path.Join
// to construct the prefix, we always preserve the trailing slash.
func JoinPathPreservingTrailingSlash(prefix, suffix string) string {
	out := path.Join(prefix, suffix)
	// path.Clean removes trailing slashes, so put it back if needed.
	if strings.HasSuffix(suffix, "/") {
		out += "/"
	}
	return out
}
