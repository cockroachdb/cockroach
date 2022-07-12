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

// ConnectionDetails is the interface to the external resource represented by an
// External Connection object.
type ConnectionDetails interface {
	// ConnectionProto prepares the ConnectionDetails for serialization.
	ConnectionProto() *connectionpb.ConnectionDetails
	// ConnectionType returns the type of the connection.
	ConnectionType() connectionpb.ConnectionType
}

// ConnectionDetailsFromURIFactory is the factory method that takes in an
// endpoint URI for an external resource, and returns a ConnectionDetails
// interface to interact with it.
type ConnectionDetailsFromURIFactory func(ctx context.Context, uri *url.URL) (ConnectionDetails, error)
