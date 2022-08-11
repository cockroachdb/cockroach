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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

type schemeRegistration struct {
	FactoryType
	connectionpb.ConnectionProvider
	connectionParserFactory
	validations map[string]ValidationFn
}

// parseAndValidateFns maps a URI scheme to a constructor of instances of that external
// connection.
var parseAndValidateFns = map[string]schemeRegistration{}

// FactoryType identifies the expected parsing logic used to create an external connection.
// The only one currently supported is SimpleURIFactory.
type FactoryType int

const (
	SimpleURIFactory FactoryType = iota
)

var factoryFactories = map[FactoryType]func(connectionpb.ConnectionProvider) connectionParserFactory{
	SimpleURIFactory: makeSimpleURIFactory,
}

// RegisterConnectionDetailsFromURIFactory is used by every concrete
// implementation to register its factory method.
func RegisterConnectionDetailsFromURIFactory(
	providerScheme string, provider connectionpb.ConnectionProvider, factoryType FactoryType,
) {
	if existing, ok := parseAndValidateFns[providerScheme]; ok {
		if existing.FactoryType != factoryType || existing.ConnectionProvider != provider {
			panic(fmt.Sprintf("different parse function already registered for %s", providerScheme))
		}
	} else {
		parseAndValidateFns[providerScheme] = schemeRegistration{
			FactoryType:             factoryType,
			ConnectionProvider:      provider,
			connectionParserFactory: factoryFactories[factoryType](provider),
			validations:             make(map[string]ValidationFn),
		}
	}
}

const defaultValidation = `default`

// RegisterDefaultValidation sets the default validation for external connections using this scheme.
func RegisterDefaultValidation(providerScheme string, validation ValidationFn) {
	RegisterNamedValidation(providerScheme, defaultValidation, validation)
}

// RegisterNamedValidation adds a custom validation for external connections using this scheme.
// It will be the default if no default is provided.
func RegisterNamedValidation(providerScheme string, name string, validation ValidationFn) {
	existing, ok := parseAndValidateFns[providerScheme]
	if !ok {
		panic(fmt.Sprintf("no parse function registered yet for %s", providerScheme))
	}
	existing.validations[name] = validation
	if _, ok = existing.validations[defaultValidation]; !ok {
		existing.validations[defaultValidation] = validation
	}
	parseAndValidateFns[providerScheme] = existing
}

func makeSimpleURIFactory(provider connectionpb.ConnectionProvider) connectionParserFactory {
	return func(ctx context.Context, execCfg interface{}, user username.SQLUsername, uri *url.URL) (ExternalConnection, error) {
		connDetails := connectionpb.ConnectionDetails{
			Provider: provider,
			Details: &connectionpb.ConnectionDetails_SimpleURI{
				SimpleURI: &connectionpb.SimpleURI{
					URI: uri.String(),
				},
			},
		}
		return NewExternalConnection(connDetails), nil
	}
}

// ExternalConnectionFromURI returns a ExternalConnection for the given URI.
func ExternalConnectionFromURI(
	ctx context.Context, execCfg interface{}, user username.SQLUsername, uri string,
) (ExternalConnection, error) {
	externalConnectionURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	// Find the parseAndValidateFn method for the ExternalConnection provider.
	parseAndValidateFn, registered := parseAndValidateFns[externalConnectionURI.Scheme]
	if !registered {
		return nil, errors.Newf("no parseAndValidateFn found for external connection provider %s", externalConnectionURI.Scheme)
	}

	return parseAndValidateFn.parseAndValidateURI(ctx, execCfg, user, externalConnectionURI, defaultValidation)
}

// ValidationFn is the type taken by Register*Validation. Validation functions can ping the external connection.
type ValidationFn func(ctx context.Context, execCfg interface{}, user username.SQLUsername, uri string) error

func (s *schemeRegistration) parseAndValidateURI(
	ctx context.Context,
	execCfg interface{},
	user username.SQLUsername,
	uri *url.URL,
	validations ...string,
) (ExternalConnection, error) {
	conn, err := s.connectionParserFactory(ctx, execCfg, user, uri)
	if err != nil {
		return nil, err
	}
	for _, v := range validations {
		if validationFn, ok := s.validations[v]; ok {
			err = errors.CombineErrors(err, validationFn(ctx, execCfg, user, uri.String()))
		}
	}
	return conn, err
}

// TestingKnobs provide fine-grained control over the external connection
// components for testing.
type TestingKnobs struct {
	// SkipCheckingExternalStorageConnection returns whether `CREATE EXTERNAL
	// CONNECTION` should skip the step that writes, lists and reads a sentinel
	// file from the underlying ExternalStorage.
	SkipCheckingExternalStorageConnection func() bool
	// SkipCheckingKMSConnection returns whether `CREATE EXTERNAL CONNECTION`
	// should skip the step that encrypts and decrypts a sentinel file from the
	// underlying KMS.
	SkipCheckingKMSConnection func() bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
