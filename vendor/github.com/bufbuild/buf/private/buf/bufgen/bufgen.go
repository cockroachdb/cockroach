// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bufgen does configuration-based generation.
//
// It is used by the buf generate command.
package bufgen

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/gen/proto/apiclient/buf/alpha/registry/v1alpha1/registryv1alpha1apiclient"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	// ExternalConfigFilePath is the default external configuration file path.
	ExternalConfigFilePath = "buf.gen.yaml"
	// V1Version is the string used to identify the v1 version of the generate template.
	V1Version = "v1"
	// V1Beta1Version is the string used to identify the v1beta1 version of the generate template.
	V1Beta1Version = "v1beta1"
)

const (
	// StrategyDirectory is the strategy that says to generate per directory.
	//
	// This is the default value.
	StrategyDirectory Strategy = 1
	// StrategyAll is the strategy that says to generate with all files at once.
	StrategyAll Strategy = 2
)

// Strategy is a generation stategy.
type Strategy int

// ParseStrategy parses the Strategy.
//
// If the empty string is provided, this is interpreted as StrategyDirectory.
func ParseStrategy(s string) (Strategy, error) {
	switch s {
	case "", "directory":
		return StrategyDirectory, nil
	case "all":
		return StrategyAll, nil
	default:
		return 0, fmt.Errorf("unknown strategy: %s", s)
	}
}

// String implements fmt.Stringer.
func (s Strategy) String() string {
	switch s {
	case StrategyDirectory:
		return "directory"
	case StrategyAll:
		return "all"
	default:
		return strconv.Itoa(int(s))
	}
}

// Provider is a provider.
type Provider interface {
	// GetConfig gets the Config for the YAML data at ExternalConfigFilePath.
	//
	// If the data is of length 0, returns the default config.
	GetConfig(ctx context.Context, readBucket storage.ReadBucket) (*Config, error)
}

// NewProvider returns a new Provider.
func NewProvider(logger *zap.Logger) Provider {
	return newProvider(logger)
}

// Generator generates Protobuf stubs based on configurations.
type Generator interface {
	// Generate calls the generation logic.
	//
	// The config is assumed to be valid. If created by ReadConfig, it will
	// always be valid.
	Generate(
		ctx context.Context,
		container app.EnvStdioContainer,
		config *Config,
		image bufimage.Image,
		options ...GenerateOption,
	) error
}

// NewGenerator returns a new Generator.
func NewGenerator(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	registryProvider registryv1alpha1apiclient.Provider,
) Generator {
	return newGenerator(logger, storageosProvider, registryProvider)
}

// GenerateOption is an option for Generate.
type GenerateOption func(*generateOptions)

// GenerateWithBaseOutDirPath returns a new GenerateOption that uses the given
// base directory as the output directory.
//
// The default is to use the current directory.
func GenerateWithBaseOutDirPath(baseOutDirPath string) GenerateOption {
	return func(generateOptions *generateOptions) {
		generateOptions.baseOutDirPath = baseOutDirPath
	}
}

// GenerateWithIncludeImports says to also generate imports.
//
// Note that this does NOT result in the Well-Known Types being generated.
func GenerateWithIncludeImports() GenerateOption {
	return func(generateOptions *generateOptions) {
		generateOptions.includeImports = true
	}
}

// Config is a configuration.
type Config struct {
	// Required
	PluginConfigs []*PluginConfig
	// Optional
	ManagedConfig *ManagedConfig
}

// PluginConfig is a plugin configuration.
type PluginConfig struct {
	// One of Name and Remote is required
	Name   string
	Remote string
	// Required
	Out string
	// Optional
	Opt string
	// Optional, exclusive with Remote
	Path string
	// Required
	Strategy Strategy
}

// ManagedConfig is the Managed Mode configuration.
type ManagedConfig struct {
	CcEnableArenas        *bool
	JavaMultipleFiles     *bool
	JavaStringCheckUtf8   *bool
	JavaPackagePrefix     string
	OptimizeFor           *descriptorpb.FileOptions_OptimizeMode
	GoPackagePrefixConfig *GoPackagePrefixConfig
	Override              map[string]map[string]string
}

// GoPackagePrefixConfig is the go_package prefix configuration.
type GoPackagePrefixConfig struct {
	Default string
	Except  []bufmodule.ModuleIdentity
	// bufmodule.ModuleIdentity -> go_package prefix.
	Override map[bufmodule.ModuleIdentity]string
}

// ReadConfig reads the configuration from the OS or an override, if any.
//
// Only use in CLI tools.
func ReadConfig(
	ctx context.Context,
	provider Provider,
	readBucket storage.ReadBucket,
	options ...ReadConfigOption,
) (*Config, error) {
	return readConfig(
		ctx,
		provider,
		readBucket,
		options...,
	)
}

// ReadConfigOption is an option for ReadConfig.
type ReadConfigOption func(*readConfigOptions)

// ReadConfigWithOverride sets the override.
//
// If override is set, this will first check if the override ends in .json or .yaml, if so,
// this reads the file at this path and uses it. Otherwise, this assumes this is configuration
// data in either JSON or YAML format, and unmarshals it.
//
// If no override is set, this reads ExternalConfigFilePath in the bucket.
func ReadConfigWithOverride(override string) ReadConfigOption {
	return func(readConfigOptions *readConfigOptions) {
		readConfigOptions.override = override
	}
}

// ConfigExists checks if a generation configuration file exists.
func ConfigExists(ctx context.Context, readBucket storage.ReadBucket) (bool, error) {
	return storage.Exists(ctx, readBucket, ExternalConfigFilePath)
}

// ExternalConfigV1 is an external configuration.
type ExternalConfigV1 struct {
	Version string                   `json:"version,omitempty" yaml:"version,omitempty"`
	Plugins []ExternalPluginConfigV1 `json:"plugins,omitempty" yaml:"plugins,omitempty"`
	Managed ExternalManagedConfigV1  `json:"managed,omitempty" yaml:"managed,omitempty"`
}

// ExternalPluginConfigV1 is an external plugin configuration.
type ExternalPluginConfigV1 struct {
	Name     string      `json:"name,omitempty" yaml:"name,omitempty"`
	Remote   string      `json:"remote,omitempty" yaml:"remote,omitempty"`
	Out      string      `json:"out,omitempty" yaml:"out,omitempty"`
	Opt      interface{} `json:"opt,omitempty" yaml:"opt,omitempty"`
	Path     string      `json:"path,omitempty" yaml:"path,omitempty"`
	Strategy string      `json:"strategy,omitempty" yaml:"strategy,omitempty"`
}

// ExternalManagedConfigV1 is an external Managed Mode configuration.
//
// Only use outside of this package for testing.
type ExternalManagedConfigV1 struct {
	Enabled             bool                            `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	CcEnableArenas      *bool                           `json:"cc_enable_arenas,omitempty" yaml:"cc_enable_arenas,omitempty"`
	JavaMultipleFiles   *bool                           `json:"java_multiple_files,omitempty" yaml:"java_multiple_files,omitempty"`
	JavaStringCheckUtf8 *bool                           `json:"java_string_check_utf8,omitempty" yaml:"java_string_check_utf8,omitempty"`
	JavaPackagePrefix   string                          `json:"java_package_prefix,omitempty" yaml:"java_package_prefix,omitempty"`
	OptimizeFor         string                          `json:"optimize_for,omitempty" yaml:"optimize_for,omitempty"`
	GoPackagePrefix     ExternalGoPackagePrefixConfigV1 `json:"go_package_prefix,omitempty" yaml:"go_package_prefix,omitempty"`
	Override            map[string]map[string]string    `json:"override,omitempty" yaml:"override,omitempty"`
}

// IsEmpty returns true if the config is empty, excluding the 'Enabled' setting.
func (e ExternalManagedConfigV1) IsEmpty() bool {
	return e.CcEnableArenas == nil &&
		e.JavaMultipleFiles == nil &&
		e.JavaStringCheckUtf8 == nil &&
		e.JavaPackagePrefix == "" &&
		e.OptimizeFor == "" &&
		e.GoPackagePrefix.IsEmpty() &&
		len(e.Override) == 0
}

// ExternalGoPackagePrefixConfigV1 is the external go_package prefix configuration.
type ExternalGoPackagePrefixConfigV1 struct {
	Default  string            `json:"default,omitempty" yaml:"default,omitempty"`
	Except   []string          `json:"except,omitempty" yaml:"except,omitempty"`
	Override map[string]string `json:"override,omitempty" yaml:"override,omitempty"`
}

// IsEmpty returns true if the config is empty.
func (e ExternalGoPackagePrefixConfigV1) IsEmpty() bool {
	return e.Default == "" &&
		len(e.Except) == 0 &&
		len(e.Override) == 0
}

// ExternalConfigV1Beta1 is an external configuration.
type ExternalConfigV1Beta1 struct {
	Version string                        `json:"version,omitempty" yaml:"version,omitempty"`
	Managed bool                          `json:"managed,omitempty" yaml:"managed,omitempty"`
	Plugins []ExternalPluginConfigV1Beta1 `json:"plugins,omitempty" yaml:"plugins,omitempty"`
	Options ExternalOptionsConfigV1Beta1  `json:"options,omitempty" yaml:"options,omitempty"`
}

// ExternalPluginConfigV1Beta1 is an external plugin configuration.
type ExternalPluginConfigV1Beta1 struct {
	Name     string      `json:"name,omitempty" yaml:"name,omitempty"`
	Out      string      `json:"out,omitempty" yaml:"out,omitempty"`
	Opt      interface{} `json:"opt,omitempty" yaml:"opt,omitempty"`
	Path     string      `json:"path,omitempty" yaml:"path,omitempty"`
	Strategy string      `json:"strategy,omitempty" yaml:"strategy,omitempty"`
}

// ExternalOptionsConfigV1Beta1 is an external options configuration.
type ExternalOptionsConfigV1Beta1 struct {
	CcEnableArenas    *bool  `json:"cc_enable_arenas,omitempty" yaml:"cc_enable_arenas,omitempty"`
	JavaMultipleFiles *bool  `json:"java_multiple_files,omitempty" yaml:"java_multiple_files,omitempty"`
	OptimizeFor       string `json:"optimize_for,omitempty" yaml:"optimize_for,omitempty"`
}

// ExternalConfigVersion defines the subset of all config
// file versions that is used to determine the configuration version.
type ExternalConfigVersion struct {
	Version string `json:"version,omitempty" yaml:"version,omitempty"`
}
