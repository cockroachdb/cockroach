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

	"github.com/cockroachdb/errors"
)

// parseAndValidateFns maps a URI scheme to a constructor of instances of that external
// connection.
var parseAndValidateFns = map[string]connectionParserFactory{}

// RegisterConnectionDetailsFromURIFactory is used by every concrete
// implementation to register its factory method.
func RegisterConnectionDetailsFromURIFactory(
	providerScheme string, parseAndValidateFn connectionParserFactory,
) {
	if _, ok := parseAndValidateFns[providerScheme]; ok {
		panic(fmt.Sprintf("parse function already registered for %s", providerScheme))
	}
	parseAndValidateFns[providerScheme] = parseAndValidateFn
}

// ExternalConnectionFromURI returns a ExternalConnection for the given URI.
func ExternalConnectionFromURI(ctx context.Context, uri string) (ExternalConnection, error) {
	externalConnectionURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	// Find the parseFn method for the ExternalConnection provider.
	parseFn, registered := parseAndValidateFns[externalConnectionURI.Scheme]
	if !registered {
		return nil, errors.Newf("no parseFn found for external connection provider %s", externalConnectionURI.Scheme)
	}

	return parseFn(ctx, externalConnectionURI)
}
