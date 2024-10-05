// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package connectionpb

import "github.com/cockroachdb/errors"

// Type returns the ConnectionType of the receiver.
func (d *ConnectionDetails) Type() ConnectionType {
	switch d.Provider {
	case ConnectionProvider_nodelocal, ConnectionProvider_s3, ConnectionProvider_userfile,
		ConnectionProvider_gs, ConnectionProvider_azure_storage:
		return TypeStorage
	case ConnectionProvider_gcp_kms, ConnectionProvider_aws_kms, ConnectionProvider_azure_kms:
		return TypeKMS
	case ConnectionProvider_kafka, ConnectionProvider_http, ConnectionProvider_https,
		ConnectionProvider_webhookhttp, ConnectionProvider_webhookhttps, ConnectionProvider_gcpubsub:
		// Changefeed sink providers are TypeStorage for now because they overlap with backup storage providers.
		return TypeStorage
	case ConnectionProvider_sql:
		return TypeForeignData
	default:
		panic(errors.AssertionFailedf("ConnectionDetails.Type called on a details with an unknown type: %s", d.Provider.String()))
	}
}

// UnredactedURI returns the unredacted URI of the resource represented by the
// External Connection.
func (d *ConnectionDetails) UnredactedURI() string {
	switch c := d.Details.(type) {
	case *ConnectionDetails_SimpleURI:
		return c.SimpleURI.URI
	default:
		panic(errors.AssertionFailedf("ConnectionDetails.UnredactedURI called on details with an unknown type: %s", d.Provider.String()))
	}
}
