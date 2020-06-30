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
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

const (
	// S3AccessKeyParam is the query parameter for access_key in an S3 URI.
	S3AccessKeyParam = "AWS_ACCESS_KEY_ID"
	// S3SecretParam is the query parameter for the 'secret' in an S3 URI.
	S3SecretParam = "AWS_SECRET_ACCESS_KEY"
	// S3TempTokenParam is the query parameter for session_token in an S3 URI.
	S3TempTokenParam = "AWS_SESSION_TOKEN"
	// S3EndpointParam is the query parameter for the 'endpoint' in an S3 URI.
	S3EndpointParam = "AWS_ENDPOINT"
	// S3RegionParam is the query parameter for the 'endpoint' in an S3 URI.
	S3RegionParam = "AWS_REGION"

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
	S3SecretParam:        {},
	S3TempTokenParam:     {},
	AzureAccountKeyParam: {},
	CredentialsParam:     {},
}

// ErrListingUnsupported is a marker for indicating listing is unsupported.
var ErrListingUnsupported = errors.New("listing is not supported")

// ErrFileDoesNotExist is a sentinel error for indicating that a specified
// bucket/object/key/file (depending on storage terminology) does not exist.
// This error is raised by the ReadFile method.
var ErrFileDoesNotExist = errors.New("external_storage: file doesn't exist")

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(path, user string) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	uri, err := url.Parse(path)
	if err != nil {
		return conf, err
	}
	switch uri.Scheme {
	case "s3":
		conf.Provider = roachpb.ExternalStorageProvider_S3
		conf.S3Config = &roachpb.ExternalStorage_S3{
			Bucket:    uri.Host,
			Prefix:    uri.Path,
			AccessKey: uri.Query().Get(S3AccessKeyParam),
			Secret:    uri.Query().Get(S3SecretParam),
			TempToken: uri.Query().Get(S3TempTokenParam),
			Endpoint:  uri.Query().Get(S3EndpointParam),
			Region:    uri.Query().Get(S3RegionParam),
			Auth:      uri.Query().Get(AuthParam),
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
				"host component of nodelocal URI must be a node ID (" +
					"use 'self' to specify each node should access its own local filesystem)",
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
		if qualifiedTableName == "" {
			return conf, errors.Errorf("host component of userfile URI must be a qualified table name")
		}

		if user == "" {
			return conf, errors.Errorf("user creating the FileTable ExternalStorage must be specified")
		}

		conf.Provider = roachpb.ExternalStorageProvider_FileTable
		conf.FileTableConfig.User = user
		conf.FileTableConfig.QualifiedTableName = qualifiedTableName
		conf.FileTableConfig.Path = uri.Path
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
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
	user string,
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
	switch dest.Provider {
	case roachpb.ExternalStorageProvider_LocalFile:
		telemetry.Count("external-io.nodelocal")
		return makeLocalStorage(ctx, dest.LocalFile, settings, blobClientFactory)
	case roachpb.ExternalStorageProvider_Http:
		if conf.DisableHTTP {
			return nil, errors.New("external http access disabled")
		}
		telemetry.Count("external-io.http")
		return MakeHTTPStorage(dest.HttpPath.BaseUri, settings)
	case roachpb.ExternalStorageProvider_S3:
		telemetry.Count("external-io.s3")
		return MakeS3Storage(ctx, conf, dest.S3Config, settings)
	case roachpb.ExternalStorageProvider_GoogleCloud:
		telemetry.Count("external-io.google_cloud")
		return makeGCSStorage(ctx, conf, dest.GoogleCloudConfig, settings)
	case roachpb.ExternalStorageProvider_Azure:
		telemetry.Count("external-io.azure")
		return makeAzureStorage(dest.AzureConfig, settings)
	case roachpb.ExternalStorageProvider_Workload:
		telemetry.Count("external-io.workload")
		return makeWorkloadStorage(dest.WorkloadConfig)
	case roachpb.ExternalStorageProvider_FileTable:
		telemetry.Count("external-io.filetable")
		return makeFileTableStorage(ctx, dest.FileTableConfig, ie, kvDB)
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

func containsGlob(str string) bool {
	return strings.ContainsAny(str, "*?[")
}

var (
	// GcsDefault is the setting which defines the JSON key to use during GCS
	// operations.
	GcsDefault = settings.RegisterPublicStringSetting(
		CloudstorageGSDefaultKey,
		"if set, JSON key to use during Google Cloud Storage operations",
		"",
	)
	httpCustomCA = settings.RegisterPublicStringSetting(
		CloudstorageHTTPCASetting,
		"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
		"",
	)
	timeoutSetting = settings.RegisterPublicDurationSetting(
		cloudStorageTimeout,
		"the timeout for import/export storage operations",
		10*time.Minute)
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
