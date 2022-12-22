// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package amazonparams

const (

	// AWSAccessKey is the query parameter for access_key in an AWS URI.
	AWSAccessKey = "AWS_ACCESS_KEY_ID"
	// AWSSecret is the query parameter for the 'secret' in an AWS URI.
	AWSSecret = "AWS_SECRET_ACCESS_KEY"
	// AWSTempToken is the query parameter for session_token in an AWS URI.
	AWSTempToken = "AWS_SESSION_TOKEN"
	// AWSEndpoint is the query parameter for the 'endpoint' in an AWS URI.
	AWSEndpoint = "AWS_ENDPOINT"

	// AWSServerSideEncryptionMode is the query parameter in an AWS URI, for the
	// mode to be used for server side encryption. It can either be AES256 or
	// aws:kms.
	AWSServerSideEncryptionMode = "AWS_SERVER_ENC_MODE"

	// AWSServerSideEncryptionKMSID is the query parameter in an AWS URI, for the
	// KMS ID to be used for server side encryption.
	AWSServerSideEncryptionKMSID = "AWS_SERVER_KMS_ID"

	// S3StorageClass is the query parameter used in S3 URIs to configure the
	// storage class for written objects.
	S3StorageClass = "S3_STORAGE_CLASS"

	// S3Region is the query parameter for the 'endpoint' in an S3 URI.
	S3Region = "AWS_REGION"

	// KMSRegion is the query parameter for the 'region' in every KMS URI.
	KMSRegion = "REGION"

	// AssumeRole is the query parameter for the chain of AWS Role ARNs to
	// assume.
	AssumeRole = "ASSUME_ROLE"
)
