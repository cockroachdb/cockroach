// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudpb

const (
	// ExternalStorageAuthImplicit is used by ExternalStorage instances to
	// indicate access via a node's "implicit" authorization (e.g. machine acct).
	ExternalStorageAuthImplicit = "implicit"

	// ExternalStorageAuthSpecified is used by ExternalStorage instances to
	// indicate access is via explicitly provided credentials.
	ExternalStorageAuthSpecified = "specified"
)

// AccessIsWithExplicitAuth returns true if the external storage config carries
// its own explicit credentials to use for access (or does not require them), as
// opposed to using something about the node to gain implicit access, such as a
// VM's machine account, network access, file system, etc.
func (m *ExternalStorage) AccessIsWithExplicitAuth() bool {
	switch m.Provider {
	case ExternalStorageProvider_s3:
		// custom endpoints could be a network resource only accessible via this
		// node's network context and thus have an element of implicit auth.
		if m.S3Config.Endpoint != "" {
			return false
		}
		return m.S3Config.Auth != ExternalStorageAuthImplicit
	case ExternalStorageProvider_gs:
		return m.GoogleCloudConfig.Auth == ExternalStorageAuthSpecified
	case ExternalStorageProvider_azure:
		// Azure storage only uses explicitly supplied credentials.
		return true
	case ExternalStorageProvider_userfile:
		// userfile always checks the user performing the action has grants on the
		// table used.
		return true
	case ExternalStorageProvider_null:
		return true
	case ExternalStorageProvider_http:
		// Arbitrary network endpoints may be accessible only via the node and thus
		// make use of its implicit access to them.
		return false
	case ExternalStorageProvider_nodelocal:
		// The node's local filesystem is obviously accessed implicitly as the node.
		return false
	case ExternalStorageProvider_external:
		// External Connections have a `USAGE` privilege that determines if a user
		// has the appropriate privileges to use the underlying resource.
		return true
	default:
		return false
	}
}
