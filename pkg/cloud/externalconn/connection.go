// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalconn

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/errors"
)

// ExternalConnection is the interface to the external resource represented by
// an External Connection object. This interface should expose read-only
// methods that are required to interact with the External Connection object.
type ExternalConnection interface {
	// UnredactedConnectionStatement returns a `CREATE EXTERNAL CONNECTION`
	// statement that is functionally equivalent to the statement that created the
	// external connection in the first place.
	//
	// NB: The returned string will contain unredacted secrets and should not be
	// persisted.
	UnredactedConnectionStatement() string
	// ConnectionName returns the label of the connection.
	ConnectionName() string
	// ConnectionType returns the type of the connection.
	ConnectionType() connectionpb.ConnectionType
	// ConnectionProto returns an in-memory representation of the
	// ConnectionDetails that describe the underlying resource. These details
	// should not be persisted directly, refer to
	// `externalconn.NewMutableExternalConnection` for mutating an External
	// Connection object.
	ConnectionProto() *connectionpb.ConnectionDetails
	// RedactedConnectionURI returns the connection URI with sensitive information
	// redacted.
	RedactedConnectionURI() string
}

// Materialize converts a potentially modified external connection uri to the underlying uri with those
// modifications applied. passing uri == nil will just return the underlying uri with no modifications
func Materialize(ec ExternalConnection, uri *url.URL) (*url.URL, error) {
	if uri != nil {
		if uri.Scheme != Scheme {
			return nil, errors.Newf("invalid input scheme '%s'", uri.Scheme)

		}
		connectionName := ec.ConnectionName()
		if uri.Host != connectionName {
			return nil, errors.Newf("input host %s does not match connection name %s", uri.Host, connectionName)
		}
	}

	connDetails := ec.ConnectionProto()
	ecUri, ok := connDetails.Details.(*connectionpb.ConnectionDetails_SimpleURI)
	if !ok {
		return nil, errors.Newf("external connection is not a URI")
	}
	materialized, err := url.Parse(ecUri.SimpleURI.URI)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse external connection URI")
	}

	if uri != nil {
		query := materialized.Query()
		for key, vals := range uri.Query() {
			for _, val := range vals {
				query.Add(key, val)
			}
		}
		materialized.RawQuery = query.Encode()

		materialized = materialized.JoinPath(uri.Path)
	}

	return materialized, nil
}

// connectionParserFactory is the factory method that takes in an endpoint URI
// for an external resource, and returns the ExternalConnection representation
// of that URI.
type connectionParserFactory func(ctx context.Context, env ExternalConnEnv, url *url.URL) (ExternalConnection, error)
