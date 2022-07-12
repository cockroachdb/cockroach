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

// implementations maps an ExternalStorageProvider enum value to a constructor
// of instances of that external storage.
var implementations = map[string]ConnectionDetailsFromURIFactory{}

// RegisterConnectionDetailsFromURIFactory is used by every concrete
// implementation to register its factory method.
func RegisterConnectionDetailsFromURIFactory(
	providerScheme string, factoryFn ConnectionDetailsFromURIFactory,
) {
	if _, ok := implementations[providerScheme]; ok {
		panic(fmt.Sprintf("external connection provider already registered for %s", providerScheme))
	}
	implementations[providerScheme] = factoryFn
}

// ConnectionDetailsFromURI returns a ConnectionDetails for the given URI.
func ConnectionDetailsFromURI(ctx context.Context, uri string) (ConnectionDetails, error) {
	externalConnectionURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	// Find the factory method for the ExternalConnection provider.
	factory, registered := implementations[externalConnectionURI.Scheme]
	if !registered {
		return nil, errors.Newf("no factory method found for external connection provider %s", externalConnectionURI.Scheme)
	}

	return factory(ctx, externalConnectionURI)
}
