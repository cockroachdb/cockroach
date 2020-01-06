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
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
	AuthParam          = "AUTH"
	authParamImplicit  = "implicit"
	authParamDefault   = "default"
	authParamSpecified = "specified"

	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"

	cloudstoragePrefix = "cloudstorage"
	cloudstorageGS     = cloudstoragePrefix + ".gs"
	cloudstorageHTTP   = cloudstoragePrefix + ".http"

	cloudstorageDefault = ".default"
	cloudstorageKey     = ".key"

	cloudstorageGSDefault    = cloudstorageGS + cloudstorageDefault
	cloudstorageGSDefaultKey = cloudstorageGSDefault + cloudstorageKey

	cloudstorageHTTPCASetting = cloudstorageHTTP + ".custom_ca"

	cloudStorageTimeout = cloudstoragePrefix + ".timeout"
)

// ExternalStorageFactory describes a factory function for ExternalStorage.
type ExternalStorageFactory func(ctx context.Context, dest roachpb.ExternalStorage) (ExternalStorage, error)

// ExternalStorageFromURIFactory describes a factory function for ExternalStorage given a URI.
type ExternalStorageFromURIFactory func(ctx context.Context, uri string) (ExternalStorage, error)

// ExternalStorage provides functions to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
// Generally an implementation is instantiated pointing to some base path or
// prefix and then gets and puts files using the various methods to interact
// with individual files contained within that path or prefix.
// However, implementations must also allow callers to provide the full path to
// a given file as the "base" path, and then read or write it with the methods
// below by simply passing an empty filename. Implementations that use stdlib's
// `filepath.Join` to concatenate their base path with the provided filename will
// find its semantics well suited to this -- it elides empty components and does
// not append surplus slashes.
type ExternalStorage interface {
	io.Closer

	// Conf should return the serializable configuration required to reconstruct
	// this ExternalStorage implementation.
	Conf() roachpb.ExternalStorage

	// ReadFile should return a Reader for requested name.
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)

	// WriteFile should write the content to requested name.
	WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error

	// ListFiles should treat the ExternalStorage URI as a glob
	// pattern, and return a list of files that match the pattern.
	ListFiles(ctx context.Context) ([]string, error)

	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error

	// Size returns the length of the named file in bytes.
	Size(ctx context.Context, basename string) (int64, error)
}

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(path string) (roachpb.ExternalStorage, error) {
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
		}
		conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	case "azure":
		conf.Provider = roachpb.ExternalStorageProvider_Azure
		conf.AzureConfig = &roachpb.ExternalStorage_Azure{
			Container:   uri.Host,
			Prefix:      uri.Path,
			AccountName: uri.Query().Get(AzureAccountNameParam),
			AccountKey:  uri.Query().Get(AzureAccountKeyParam),
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
		nodeID, err := strconv.Atoi(uri.Host)
		if err != nil && uri.Host != "" {
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
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
}

// ExternalStorageFromURI returns an ExternalStorage for the given URI.
func ExternalStorageFromURI(
	ctx context.Context,
	uri string,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (ExternalStorage, error) {
	conf, err := ExternalStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	return MakeExternalStorage(ctx, conf, settings, blobClientFactory)
}

// SanitizeExternalStorageURI returns the external storage URI with sensitive
// credentials stripped.
func SanitizeExternalStorageURI(path string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "experimental-workload" || uri.Scheme == "workload" {
		return path, nil
	}
	// All current external storage providers store credentials in the query string,
	// if they store it in the URI at all.
	uri.RawQuery = ""
	return uri.String(), nil
}

// MakeExternalStorage creates an ExternalStorage from the given config.
func MakeExternalStorage(
	ctx context.Context,
	dest roachpb.ExternalStorage,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (ExternalStorage, error) {
	switch dest.Provider {
	case roachpb.ExternalStorageProvider_LocalFile:
		telemetry.Count("external-io.nodelocal")
		return makeLocalStorage(ctx, dest.LocalFile, settings, blobClientFactory)
	case roachpb.ExternalStorageProvider_Http:
		telemetry.Count("external-io.http")
		return makeHTTPStorage(dest.HttpPath.BaseUri, settings)
	case roachpb.ExternalStorageProvider_S3:
		telemetry.Count("external-io.s3")
		return makeS3Storage(ctx, dest.S3Config, settings)
	case roachpb.ExternalStorageProvider_GoogleCloud:
		telemetry.Count("external-io.google_cloud")
		return makeGCSStorage(ctx, dest.GoogleCloudConfig, settings)
	case roachpb.ExternalStorageProvider_Azure:
		telemetry.Count("external-io.azure")
		return makeAzureStorage(dest.AzureConfig, settings)
	case roachpb.ExternalStorageProvider_Workload:
		telemetry.Count("external-io.workload")
		return makeWorkloadStorage(dest.WorkloadConfig)
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

	return strings.ContainsAny(parsedURI.Path, "*?[")
}

var (
	gcsDefault = settings.RegisterPublicStringSetting(
		cloudstorageGSDefaultKey,
		"if set, JSON key to use during Google Cloud Storage operations",
		"",
	)
	httpCustomCA = settings.RegisterPublicStringSetting(
		cloudstorageHTTPCASetting,
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
	const maxAttempts = 3
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		err := fn()
		if err == nil {
			return nil
		}
		if s3err, ok := err.(s3.RequestFailure); ok {
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
		// See https:github.com/GoogleCloudPlatform/google-cloud-go/issues/1012#issuecomment-393606797
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
