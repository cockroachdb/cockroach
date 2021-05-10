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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
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

const (
	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"
)

const (

	// GoogleBillingProjectParam is the query parameter for the billing project
	// in a gs URI.
	GoogleBillingProjectParam = "GOOGLE_BILLING_PROJECT"
	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"
)

const (
	// AuthParam is the query parameter for the cluster settings named
	// key in a URI.
	AuthParam = "AUTH"
	// AuthParamImplicit is the query parameter for the implicit authentication
	// mode in a URI.
	AuthParamImplicit = roachpb.ExternalStorageAuthImplicit
	// AuthParamSpecified is the query parameter for the specified authentication
	// mode in a URI.
	AuthParamSpecified = roachpb.ExternalStorageAuthSpecified
)

func init() {
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_azure,
		parseAzureURL, makeAzureStorage, cloud.RedactedParams(AzureAccountKeyParam), "azure")
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_gs,
		parseGSURL, makeGCSStorage, cloud.RedactedParams(CredentialsParam), "gs")
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_http,
		parseHTTPURL, MakeHTTPStorage, cloud.RedactedParams(), "http", "https")
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_nodelocal,
		parseNodelocalURL, makeLocalStorage, cloud.RedactedParams(), "nodelocal")
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_null,
		parseNullURL, makeNullSinkStorage, cloud.RedactedParams(), "null")
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_s3,
		parseS3URL, MakeS3Storage, cloud.RedactedParams(AWSSecretParam, AWSTempTokenParam), "s3")
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_userfile,
		parseUserfileURL, makeFileTableStorage, cloud.RedactedParams(), "userfile")
}
