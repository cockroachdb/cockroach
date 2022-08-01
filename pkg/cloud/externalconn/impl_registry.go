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
	"fmt"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/errors"
)

// parseAndValidateFns maps a URI scheme to a constructor of instances of that
// external connection.
var parseAndValidateFns = map[string]connectionParserFactory{}

// constructFns maps a connectionpb.ConnectionProvider to a constructor of
// instances of that external connection.
var constructFns = map[connectionpb.ConnectionProvider]connectionDetailsFactory{}

// RegisterConnectionDetailsFromURIFactory is used by every concrete
// implementation to register its factory method.
func RegisterConnectionDetailsFromURIFactory(
	provider connectionpb.ConnectionProvider,
	providerScheme string,
	parseAndValidateFn connectionParserFactory,
	constructFn connectionDetailsFactory,
) {
	if _, ok := parseAndValidateFns[providerScheme]; ok {
		panic(fmt.Sprintf("parse function already registered for %s", providerScheme))
	}
	parseAndValidateFns[providerScheme] = parseAndValidateFn

	if _, ok := constructFns[provider]; ok {
		panic(fmt.Sprintf("construct function already registered for %s", provider.String()))
	}
	constructFns[provider] = constructFn
}

// ConnectionDetailsFromURI returns a ConnectionDetails for the given URI.
func ConnectionDetailsFromURI(ctx context.Context, uri string) (ConnectionDetails, error) {
	externalConnectionURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	// Find the parseFn method for the ExternalConnection provider.
	parseFn, registered := parseAndValidateFns[externalConnectionURI.Scheme]
	if !registered {
		return nil, errors.Newf("no parseFn found for external connection provider %s", externalConnectionURI.Scheme)
	}

	conn, err := parseFn(ctx, externalConnectionURI)
	if err != nil {
		return nil, err
	}
	return MakeConnectionDetails(ctx, conn)
}

// MakeConnectionDetails constructs a ConnectionDetails from the passed in
// config.
func MakeConnectionDetails(
	ctx context.Context, details connectionpb.ConnectionDetails,
) (ConnectionDetails, error) {
	// Find the factory method for the ExternalConnection provider.
	factory, registered := constructFns[details.Provider]
	if !registered {
		return nil, errors.Newf("no factory method found for external connection provider %s", details.Provider.String())
	}

	return factory(ctx, details), nil
}
