// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// validations contains all registered validations specific to this scheme,
	// indexed by name or the defaultValidation magic constant.
	validations map[string]ValidationFn
}

func (s *schemeRegistration) parseAndValidateURI(
	ctx context.Context, env ExternalConnEnv, uri *url.URL, requestedValidations ...string,
) (ExternalConnection, error) {
	conn, err := s.connectionParserFactory(ctx, env, uri)
	if err != nil {
		return nil, err
	}
	for _, v := range requestedValidations {
		if validationFn, ok := s.validations[v]; ok {
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
			validations:             make(map[string]ValidationFn),
		}
	}
}

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

// ProviderForURI returns the provider associated with the scheme of a given URI,
// or UNKNOWN if none found.
// This is useful for testing.
func ProviderForURI(uri string) connectionpb.ConnectionProvider {
	externalConnectionURI, err := url.Parse(uri)
	if err != nil {
		return connectionpb.ConnectionProvider_Unknown
	}

	// Find the parseAndValidateFn method for the ExternalConnection provider.
	parseAndValidateFn, registered := parseAndValidateFns[externalConnectionURI.Scheme]
	if !registered {
		return connectionpb.ConnectionProvider_Unknown
	}

	return parseAndValidateFn.ConnectionProvider
}

// ExternalConnEnv contains parameters to be used to validate an external
// connection.
type ExternalConnEnv struct {
	// Settings refers to the cluster settings that apply to the BackupKMSEnv.
	Settings *cluster.Settings
	// Conf represents the ExternalIODirConfig that applies to the BackupKMSEnv.
	Conf *base.ExternalIODirConfig
	// DB is the database handle that applies to the BackupKMSEnv.
	Isqldb isql.DB
	// Username is the user that applies to the BackupKMSEnv.
	Username username.SQLUsername

	ExternalStorageFromURIFactory cloud.ExternalStorageFromURIFactory

	SkipCheckingExternalStorageConnection bool
	SkipCheckingKMSConnection             bool
	// ServerCfg is used to validate the external connection sink URI.
	// It implements *execinfra.ServerConfig, and is declared as an interface
	// here to avoid importing `execinfra` package to the sub-packages in the
	// cloud pkg.
	ServerCfg interface{}
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

func MakeExternalConnEnv(
	settings *cluster.Settings,
	conf *base.ExternalIODirConfig,
	isqlDB isql.DB,
	username username.SQLUsername,
	externalStorageFromURIFactory cloud.ExternalStorageFromURIFactory,
	skipCheckingExternalStorageConnection bool,
	skipCheckingKMSConnection bool,
	serverCfg interface{},
) ExternalConnEnv {
	return ExternalConnEnv{
		Settings:                              settings,
		Conf:                                  conf,
		Isqldb:                                isqlDB,
		SkipCheckingExternalStorageConnection: skipCheckingExternalStorageConnection,
		SkipCheckingKMSConnection:             skipCheckingKMSConnection,
		ExternalStorageFromURIFactory:         externalStorageFromURIFactory,
		Username:                              username,
		ServerCfg:                             serverCfg,
	}
}

// ValidationFn is the type taken by Register*ValidationWithCloud. Validation functions can ping the external connection.
type ValidationFn func(ctx context.Context, env ExternalConnEnv, uri string) error

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
