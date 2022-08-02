// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package externalconn

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
)

// ExternalConnection is the interface to the external resource represented by
// an External Connection object. This interface should expose read-only
// methods that are required to interact with the External Connection object.
type ExternalConnection interface {
	// ConnectionType returns the type of the connection.
	ConnectionType() connectionpb.ConnectionType
	// ConnectionProto returns an in-memory representation of the
	// ConnectionDetails that describe the underlying resource. These details
	// should not be persisted directly, refer to
	// `externalconn.NewMutableExternalConnection` for mutating an External
	// Connection object.
	ConnectionProto() *connectionpb.ConnectionDetails
}

// connectionParserFactory is the factory method that takes in an endpoint URI
// for an external resource, and returns the ExternalConnection representation
// of that URI.
type connectionParserFactory func(ctx context.Context, uri *url.URL) (ExternalConnection, error)
