// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcpparams

const (

	// GoogleBillingProject is the query parameter for the billing project
	// in a gs URI.
	GoogleBillingProject = "GOOGLE_BILLING_PROJECT"
	// Credentials is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	Credentials = "CREDENTIALS"
	// AssumeRole is the query parameter for the chain of service account
	// email addresses to assume.
	AssumeRole = "ASSUME_ROLE"

	// BearerToken is the query parameter for a temporary bearer token. There
	// is no refresh mechanism associated with this token, so it is up to the user
	// to ensure that its TTL is longer than the duration of the job or query that
	// is using the token. The job or query may irrecoverably fail if one of its
	// tokens expire before completion.
	BearerToken = "BEARER_TOKEN"
)
