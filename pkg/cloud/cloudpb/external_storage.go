// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudpb

import (
	"fmt"
	"strings"
)

const (
	// ExternalStorageAuthImplicit is used by ExternalStorage instances to
	// indicate access via a node's "implicit" authorization (e.g. machine acct).
	ExternalStorageAuthImplicit = "implicit"

	// ExternalStorageAuthSpecified is used by ExternalStorage instances to
	// indicate access is via explicitly provided credentials.
	ExternalStorageAuthSpecified = "specified"
)

// ExternalDestination describes a target of external I/O for the purpose of
// privilege checks, without requiring the target to be an external storage. It
// lets the destination privilege check reason uniformly about external storage
// (e.g. S3) and about destinations that have no ExternalStorage representation
// at all (e.g. kafka changefeed sinks).
type ExternalDestination interface {
	// AccessIsWithExplicitAuth reports whether access to the destination uses
	// credentials the statement supplied, what EXTERNALIOIMPLICITACCESS gates.
	AccessIsWithExplicitAuth() bool
	// Kind names the destination for user-facing error messages.
	// It is a label, not a stable identifier, and callers must not switch on it.
	Kind() string
}

// ImplicitAuthDestination returns an ExternalDestination for a direct
// (non-external-connection) target that the node reaches using its own implicit
// credentials or network context, and therefore always requires
// EXTERNALIOIMPLICITACCESS. scheme is used only as the user-facing label for
// error messages.
func ImplicitAuthDestination(scheme string) ExternalDestination {
	return implicitAuthDestination{scheme: scheme}
}

type implicitAuthDestination struct{ scheme string }

func (d implicitAuthDestination) AccessIsWithExplicitAuth() bool { return false }
func (d implicitAuthDestination) Kind() string                   { return d.scheme }

var _ ExternalDestination = implicitAuthDestination{}

// KMSDestination returns an ExternalDestination for a KMS URI. A KMS URI has no
// ExternalStorage representation, but access to it still needs to be classified
// as explicit or implicit for privilege checks. endpoint is the URI's custom
// endpoint (e.g. AWS_ENDPOINT for aws-kms), or empty if none was specified.
func KMSDestination(scheme, auth, endpoint string) ExternalDestination {
	return kmsDestination{scheme: scheme, auth: auth, endpoint: endpoint}
}

type kmsDestination struct {
	// scheme is used only as the user-facing label for error messages.
	scheme string
	// auth is the value of the URI's AUTH parameter.
	auth string
	// endpoint is the URI's custom endpoint, if any.
	endpoint string
}

func (d kmsDestination) AccessIsWithExplicitAuth() bool {
	// As with S3 storage, a custom endpoint could be a network resource only
	// accessible via this node's network context and thus has an element of
	// implicit auth, regardless of the credentials supplied.
	if d.endpoint != "" {
		return false
	}
	return d.auth != ExternalStorageAuthImplicit
}
func (d kmsDestination) Kind() string { return d.scheme }

var _ ExternalDestination = kmsDestination{}

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
		return m.AzureConfig.Auth == AzureAuth_LEGACY || m.AzureConfig.Auth == AzureAuth_EXPLICIT
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

// Kind implements the ExternalDestination interface.
func (m *ExternalStorage) Kind() string {
	return m.Provider.String()
}

var _ ExternalDestination = (*ExternalStorage)(nil)

const assumeRoleProviderExternalIDParam = "external_id"

// EncodeAsString returns the string representation of the provider to be used
// in URIs.
func (p ExternalStorage_AssumeRoleProvider) EncodeAsString() string {
	if p.Role == "" {
		return ""
	}

	if p.ExternalID == "" {
		return p.Role
	}

	return fmt.Sprintf("%s;%s=%s", p.Role, assumeRoleProviderExternalIDParam, p.ExternalID)

}

// DecodeRoleProviderString decodes a string into an
// ExternalStorage_AssumeRoleProvider.
func DecodeRoleProviderString(roleProviderString string) (p ExternalStorage_AssumeRoleProvider) {
	parts := strings.Split(roleProviderString, ";")
	p.Role = parts[0]

	for pidx := 1; pidx < len(parts); pidx++ {
		key, value, _ := strings.Cut(parts[pidx], "=")
		if key == assumeRoleProviderExternalIDParam {
			p.ExternalID = value
		}
	}
	return p
}
