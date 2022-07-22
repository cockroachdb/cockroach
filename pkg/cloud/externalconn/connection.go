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

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
)

// Connection is a marker interface for objects that support interaction with an
// external resource.
type Connection interface{}

// ConnectionDetails is the interface to the external resource represented by an
// External Connection object.
type ConnectionDetails interface {
	// Dial establishes a connection to the external resource.
	//
	// A non-empty subdir results in a `Connection` to the joined path of the
	// endpoint represented by the external connection and subdir.
	Dial(ctx context.Context, args cloud.ExternalStorageContext, subdir string) (Connection, error)
	// ConnectionProto prepares the ConnectionDetails for serialization.
	ConnectionProto() *connectionpb.ConnectionDetails
	// ConnectionType returns the type of the connection.
	ConnectionType() connectionpb.ConnectionType
}

// connectionParserFactory is the factory method that takes in an endpoint URI
// for an external resource, and returns the ConnectionDetails proto
// representation of that URI.
type connectionParserFactory func(ctx context.Context,
	uri *url.URL) (connectionpb.ConnectionDetails, error)

// connectionDetailsFactory is the factory method that returns a
// ConnectionDetails interface to interact with the underlying External
// Connection object.
type connectionDetailsFactory func(ctx context.Context,
	details connectionpb.ConnectionDetails) ConnectionDetails
