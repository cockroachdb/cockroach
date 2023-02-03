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
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/errors"
)

// FactoryType identifies the expected parsing logic used to create an external connection.
// The only one currently supported is SimpleURIFactory.
type FactoryType int

const (
	// SimpleURIFactory just stores URI.String().
	SimpleURIFactory FactoryType = iota
)

var factoryFactories = map[FactoryType]func(connectionpb.ConnectionProvider) connectionParserFactory{
	SimpleURIFactory: makeSimpleURIFactory,
}

// parseAndValidateFns maps a URI scheme to a constructor of instances of that external
// connection.
var parseAndValidateFns = map[string]schemeRegistration{}

const defaultValidation = `default`

type schemeRegistration struct {
	FactoryType
	connectionpb.ConnectionProvider
	// connectionParserFactory is the function that will be called
	// to create an ExternalConnection from a URI of a given scheme.
	connectionParserFactory
	// validationsWithCloud contains all registered validations for the cloud service providers
	// specific to this scheme, indexed by name or the defaultValidation magic constant.
	validationsWithCloud map[string]ValidationFnWithCloud
	// validationsWithSinkURI contains the validations from the changefeedccl
	// side. We want to distinguish it from the one from the cloud provider side.
	validationsWithSinkURI map[string]ValidationFnWithSinkURI
}

func (s *schemeRegistration) parseAndValidateURI(
	ctx context.Context, env ExternalConnEnv, uri *url.URL, requestedValidations ...string,
) (ExternalConnection, error) {
	conn, err := s.connectionParserFactory(ctx, env, uri)
	if err != nil {
		return nil, err
	}
	for _, v := range requestedValidations {
		if validationFn, ok := s.validationsWithCloud[v]; ok {
			err = errors.CombineErrors(err, validationFn(ctx, cloud.KMSEnv(&env), uri.String()))
		}
		if validationFn, ok := s.validationsWithSinkURI[v]; ok {
			err = errors.CombineErrors(err, validationFn(ctx, env, uri.String()))
		}
	}
	return conn, err
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
			validationsWithCloud:    make(map[string]ValidationFnWithCloud),
			validationsWithSinkURI:  make(map[string]ValidationFnWithSinkURI),
		}
	}
}

// RegisterDefaultValidation sets the default validation for external connections using this scheme.
func RegisterDefaultValidation(providerScheme string, validation ValidationFnWithCloud) {
	RegisterNamedValidationForCloud(providerScheme, defaultValidation, validation)
}

// RegisterNamedValidationForCloud adds a custom validation for external connections using this scheme.
// It will be the default if no default is provided.
func RegisterNamedValidationForCloud(
	providerScheme string, name string, validation ValidationFnWithCloud,
) {
	existing, ok := parseAndValidateFns[providerScheme]
	if !ok {
		panic(fmt.Sprintf("no parse function registered yet for %s", providerScheme))
	}
	existing.validationsWithCloud[name] = validation
	if _, ok = existing.validationsWithCloud[defaultValidation]; !ok {
		existing.validationsWithCloud[defaultValidation] = validation
	}
	parseAndValidateFns[providerScheme] = existing
}

// RegisterNamedValidationForSink adds a custom validation for external
// connections Sink URI using this scheme.
// It will be the default if no default is provided.
func RegisterNamedValidationForSink(
	providerScheme string, name string, validation ValidationFnWithSinkURI,
) {
	existing, ok := parseAndValidateFns[providerScheme]
	if !ok {
		panic(fmt.Sprintf("no parse function registered yet for %s", providerScheme))
	}
	existing.validationsWithSinkURI[name] = validation
	if _, ok = existing.validationsWithSinkURI[defaultValidation]; !ok {
		existing.validationsWithSinkURI[defaultValidation] = validation
	}
	parseAndValidateFns[providerScheme] = existing
}

func makeSimpleURIFactory(provider connectionpb.ConnectionProvider) connectionParserFactory {
	return func(ctx context.Context, env ExternalConnEnv, uri *url.URL) (ExternalConnection, error) {
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

// ExternalConnectionFromURI returns an ExternalConnection for the given URI and runs the registered
// default validation, which can involve making external calls.
func ExternalConnectionFromURI(
	ctx context.Context, env ExternalConnEnv, uri string,
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

	return parseAndValidateFn.parseAndValidateURI(ctx, env, externalConnectionURI, defaultValidation)
}

type ExternalConnEnv struct {
	// Settings refers to the cluster settings that apply to the BackupKMSEnv.
	Settings *cluster.Settings
	// Conf represents the ExternalIODirConfig that applies to the BackupKMSEnv.
	Conf *base.ExternalIODirConfig
	// DB is the database handle that applies to the BackupKMSEnv.
	Isqldb isql.DB
	// Username is the user that applies to the BackupKMSEnv.
	Username username.SQLUsername
	// ExternalConnectionTestingKnobs is used for testing.
	ExternalConnectionTestingKnobs cloud.ExternalConnTestingKnobs
	// ServerCfg is used to validate the external connection sink URI.
	ServerCfg *execinfra.ServerConfig
}

func MakeExternalConnEnv(
	settings *cluster.Settings,
	conf *base.ExternalIODirConfig,
	isqlDB isql.DB,
	username username.SQLUsername,
	externalConnTestingKnobs cloud.ExternalConnTestingKnobs,
	serverCfg *execinfra.ServerConfig,
) ExternalConnEnv {
	return ExternalConnEnv{
		Settings:                       settings,
		Conf:                           conf,
		Isqldb:                         isqlDB,
		ExternalConnectionTestingKnobs: externalConnTestingKnobs,
		Username:                       username,
		ServerCfg:                      serverCfg,
	}
}

var _ cloud.KMSEnv = &ExternalConnEnv{}

func (s *ExternalConnEnv) ClusterSettings() *cluster.Settings {
	return s.Settings
}

func (s *ExternalConnEnv) KMSConfig() *base.ExternalIODirConfig {
	return s.Conf
}

func (s *ExternalConnEnv) DBHandle() isql.DB {
	return s.Isqldb
}

func (s *ExternalConnEnv) User() username.SQLUsername {
	return s.Username
}

func (s *ExternalConnEnv) ExternalStorageFromURI() cloud.ExternalStorageFromURIFactory {
	return s.ServerCfg.ExternalStorageFromURI
}

func (s *ExternalConnEnv) ExternalConnTestingKnobs() cloud.ExternalConnTestingKnobs {
	return s.ExternalConnectionTestingKnobs
}

// ValidationFnWithCloud is the type taken by Register*ValidationWithCloud. Validation functions can ping the external connection.
type ValidationFnWithCloud func(ctx context.Context, env cloud.KMSEnv, uri string) error

// ValidationFnWithSinkURI is the type taken by Register*ValidationWithSinkURI. Validation functions can ping the external connection.
type ValidationFnWithSinkURI func(ctx context.Context, env ExternalConnEnv, uri string) error

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

// GetSkipCheckingExternalStorageConnection implements cloud.ExternalConnTestingKnobs.
func (t *TestingKnobs) GetSkipCheckingExternalStorageConnection() func() bool {
	return t.SkipCheckingExternalStorageConnection
}

// GetSkipCheckingKMSConnection implements cloud.ExternalConnTestingKnobs.
func (t *TestingKnobs) GetSkipCheckingKMSConnection() func() bool {
	return t.SkipCheckingKMSConnection
}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
