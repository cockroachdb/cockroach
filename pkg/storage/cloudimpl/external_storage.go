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
	"fmt"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
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
	AuthParamImplicit = roachpb.ExternalStorageAuthImplicit
	// AuthParamSpecified is the query parameter for the specified authentication
	// mode in a URI.
	AuthParamSpecified = roachpb.ExternalStorageAuthSpecified

	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"
)

var redactedQueryParams = map[string]struct{}{}
var confParsers = map[string]ExternalStorageURIParser{}
var implementations = map[roachpb.ExternalStorageProvider]ExternalStorageConstructor{}

// RegisterExternalStorageProvider registers an external storage provider for a
// given URI scheme and provider type.
func RegisterExternalStorageProvider(
	providerType roachpb.ExternalStorageProvider,
	parseFn ExternalStorageURIParser,
	constructFn ExternalStorageConstructor,
	redactedParams map[string]struct{},
	schemes ...string,
) {
	for _, scheme := range schemes {
		if _, ok := confParsers[scheme]; ok {
			panic(fmt.Sprintf("external storage provider already registered for %s", scheme))
		}
		confParsers[scheme] = parseFn
		for param := range redactedParams {
			redactedQueryParams[param] = struct{}{}
		}
	}
	if _, ok := implementations[providerType]; ok {
		panic(fmt.Sprintf("external storage provider already registered for %s", providerType.String()))
	}
	implementations[providerType] = constructFn
}

// RedactedParams is a helper for making a set of param names to redact in URIs.
func RedactedParams(strs ...string) map[string]struct{} {
	if len(strs) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(strs))
	for i := range strs {
		m[strs[i]] = struct{}{}
	}
	return m
}

func init() {
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_azure,
		parseAzureURL, makeAzureStorage, RedactedParams(AzureAccountKeyParam), "azure")
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_gs,
		parseGSURL, makeGCSStorage, RedactedParams(CredentialsParam), "gs")
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_http,
		parseHTTPURL, MakeHTTPStorage, RedactedParams(), "http", "https")
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_nodelocal,
		parseNodelocalURL, makeLocalStorage, RedactedParams(), "nodelocal")
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_null,
		parseNullURL, makeNullSinkStorage, RedactedParams(), "null")
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_s3,
		parseS3URL, MakeS3Storage, RedactedParams(AWSSecretParam, AWSTempTokenParam), "s3")
	RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_userfile,
		parseUserfileURL, makeFileTableStorage, RedactedParams(), "userfile")
}

// ExternalStorageURIContext contains arguments needed to parse external storage
// URIs.
type ExternalStorageURIContext struct {
	CurrentUser security.SQLUsername
}

// ExternalStorageURIParser functions parses a URL into a structured
// ExternalStorage configuration.
type ExternalStorageURIParser func(ExternalStorageURIContext, *url.URL) (roachpb.ExternalStorage, error)

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(
	path string, user security.SQLUsername,
) (roachpb.ExternalStorage, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return roachpb.ExternalStorage{}, err
	}
	if fn, ok := confParsers[uri.Scheme]; ok {
		return fn(ExternalStorageURIContext{CurrentUser: user}, uri)
	}
	// TODO(adityamaru): Link dedicated ExternalStorage scheme docs once ready.
	return roachpb.ExternalStorage{}, errors.Errorf("unsupported storage scheme: %q - refer to docs to find supported"+
		" storage schemes", uri.Scheme)
}

// ExternalStorageFromURI returns an ExternalStorage for the given URI.
func ExternalStorageFromURI(
	ctx context.Context,
	uri string,
	externalConfig base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	user security.SQLUsername,
	ie sqlutil.InternalExecutor,
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
	if uri.Scheme == "experimental-workload" || uri.Scheme == "workload" || uri.Scheme == "null" {
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

// ExternalStorageContext contains the dependencies passed to external storage
// implementations during creation.
type ExternalStorageContext struct {
	IOConf            base.ExternalIODirConfig
	Settings          *cluster.Settings
	BlobClientFactory blobs.BlobClientFactory
	InternalExecutor  sqlutil.InternalExecutor
	DB                *kv.DB
}

// ExternalStorageConstructor is a function registered to create instances
// of a given external storage implamentation.
type ExternalStorageConstructor func(
	context.Context, ExternalStorageContext, roachpb.ExternalStorage,
) (cloud.ExternalStorage, error)

// MakeExternalStorage creates an ExternalStorage from the given config.
func MakeExternalStorage(
	ctx context.Context,
	dest roachpb.ExternalStorage,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	ie sqlutil.InternalExecutor,
	kvDB *kv.DB,
) (cloud.ExternalStorage, error) {
	args := ExternalStorageContext{
		IOConf:            conf,
		Settings:          settings,
		BlobClientFactory: blobClientFactory,
		InternalExecutor:  ie,
		DB:                kvDB,
	}
	if conf.DisableOutbound && dest.Provider != roachpb.ExternalStorageProvider_userfile {
		return nil, errors.New("external network access is disabled")
	}
	if fn, ok := implementations[dest.Provider]; ok {
		return fn(ctx, args, dest)
	}
	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}
