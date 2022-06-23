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

package bufgen

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/bufpkg/bufplugin"
	"github.com/bufbuild/buf/private/pkg/encoding"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/storage"
	"google.golang.org/protobuf/types/descriptorpb"
)

func readConfig(
	ctx context.Context,
	provider Provider,
	readBucket storage.ReadBucket,
	options ...ReadConfigOption,
) (*Config, error) {
	readConfigOptions := newReadConfigOptions()
	for _, option := range options {
		option(readConfigOptions)
	}
	if override := readConfigOptions.override; override != "" {
		switch filepath.Ext(override) {
		case ".json":
			return getConfigJSONFile(override)
		case ".yaml", ".yml":
			return getConfigYAMLFile(override)
		default:
			return getConfigJSONOrYAMLData(override)
		}
	}
	return provider.GetConfig(ctx, readBucket)
}

func getConfigJSONFile(file string) (*Config, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("could not read file %s: %v", file, err)
	}
	return getConfig(
		encoding.UnmarshalJSONNonStrict,
		encoding.UnmarshalJSONStrict,
		data,
		file,
	)
}

func getConfigYAMLFile(file string) (*Config, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("could not read file %s: %v", file, err)
	}
	return getConfig(
		encoding.UnmarshalYAMLNonStrict,
		encoding.UnmarshalYAMLStrict,
		data,
		file,
	)
}

func getConfigJSONOrYAMLData(data string) (*Config, error) {
	return getConfig(
		encoding.UnmarshalJSONOrYAMLNonStrict,
		encoding.UnmarshalJSONOrYAMLStrict,
		[]byte(data),
		"Generate configuration data",
	)
}

func getConfig(
	unmarshalNonStrict func([]byte, interface{}) error,
	unmarshalStrict func([]byte, interface{}) error,
	data []byte,
	id string,
) (*Config, error) {
	var externalConfigVersion ExternalConfigVersion
	if err := unmarshalNonStrict(data, &externalConfigVersion); err != nil {
		return nil, err
	}
	switch externalConfigVersion.Version {
	case V1Beta1Version:
		var externalConfigV1Beta1 ExternalConfigV1Beta1
		if err := unmarshalStrict(data, &externalConfigV1Beta1); err != nil {
			return nil, err
		}
		if err := validateExternalConfigV1Beta1(externalConfigV1Beta1, id); err != nil {
			return nil, err
		}
		return newConfigV1Beta1(externalConfigV1Beta1, id)
	case V1Version:
		var externalConfigV1 ExternalConfigV1
		if err := unmarshalStrict(data, &externalConfigV1); err != nil {
			return nil, err
		}
		if err := validateExternalConfigV1(externalConfigV1, id); err != nil {
			return nil, err
		}
		return newConfigV1(externalConfigV1, id)
	default:
		return nil, fmt.Errorf(`%s has no version set. Please add "version: %s"`, id, V1Version)
	}
}

func newConfigV1(externalConfig ExternalConfigV1, id string) (*Config, error) {
	managedConfig, err := newManagedConfigV1(externalConfig.Managed)
	if err != nil {
		return nil, err
	}
	pluginConfigs := make([]*PluginConfig, 0, len(externalConfig.Plugins))
	for _, plugin := range externalConfig.Plugins {
		strategy, err := ParseStrategy(plugin.Strategy)
		if err != nil {
			return nil, err
		}
		if plugin.Remote != "" {
			// Always use StrategyAll for remote plugins
			strategy = StrategyAll
		}
		opt, err := encoding.InterfaceSliceOrStringToCommaSepString(plugin.Opt)
		if err != nil {
			return nil, err
		}
		pluginConfigs = append(
			pluginConfigs,
			&PluginConfig{
				Name:     plugin.Name,
				Remote:   plugin.Remote,
				Out:      plugin.Out,
				Opt:      opt,
				Path:     plugin.Path,
				Strategy: strategy,
			},
		)
	}
	return &Config{
		PluginConfigs: pluginConfigs,
		ManagedConfig: managedConfig,
	}, nil
}

func validateExternalConfigV1(externalConfig ExternalConfigV1, id string) error {
	if len(externalConfig.Plugins) == 0 {
		return fmt.Errorf("%s: no plugins set", id)
	}
	for _, plugin := range externalConfig.Plugins {
		if plugin.Name == "" && plugin.Remote == "" {
			return fmt.Errorf("%s: plugin name or remote plugin name is required", id)
		}
		if plugin.Name != "" && plugin.Remote != "" {
			return fmt.Errorf("%s: only one of plugin name and remote plugin name can be set", id)
		}
		if plugin.Out == "" {
			return fmt.Errorf("%s: plugin %s out is required", id, plugin.Name)
		}
		if plugin.Remote != "" {
			if _, _, _, _, err := bufplugin.ParsePluginVersionPath(plugin.Remote); err != nil {
				return fmt.Errorf("%s: invalid remote plugin name: %w", id, err)
			}
			// We limit the amount of validation done on the client side intentionally
			if plugin.Path != "" {
				return fmt.Errorf("%s: remote plugin %s cannot specify a path", id, plugin.Remote)
			}
			if plugin.Strategy != "" {
				return fmt.Errorf("%s: remote plugin %s cannot specify a strategy", id, plugin.Remote)
			}
			continue
		}
		// Check that the plugin name doesn't look like a remote plugin
		if _, _, _, _, err := bufplugin.ParsePluginVersionPath(plugin.Name); err == nil {
			return fmt.Errorf("%s: invalid plugin name, did you mean to use a remote plugin?", id)
		}
	}
	return nil
}

func newManagedConfigV1(externalManagedConfig ExternalManagedConfigV1) (*ManagedConfig, error) {
	if !externalManagedConfig.Enabled && !externalManagedConfig.IsEmpty() {
		return nil, errors.New("Managed Mode options are set, but 'managed.enabled: true' is not set")
	}
	if !externalManagedConfig.Enabled {
		return nil, nil
	}
	var optimizeFor *descriptorpb.FileOptions_OptimizeMode
	if externalManagedConfig.OptimizeFor != "" {
		value, ok := descriptorpb.FileOptions_OptimizeMode_value[externalManagedConfig.OptimizeFor]
		if !ok {
			return nil, fmt.Errorf(
				"invalid optimize_for value; expected one of %v",
				enumMapToStringSlice(descriptorpb.FileOptions_OptimizeMode_value),
			)
		}
		optimizeFor = optimizeModePtr(descriptorpb.FileOptions_OptimizeMode(value))
	}
	goPackagePrefixConfig, err := newGoPackagePrefixConfigV1(externalManagedConfig.GoPackagePrefix)
	if err != nil {
		return nil, err
	}
	override := externalManagedConfig.Override
	for overrideID, overrideValue := range override {
		for importPath := range overrideValue {
			normalizedImportPath, err := normalpath.NormalizeAndValidate(importPath)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to normalize import path: %s provided for override: %s",
					importPath,
					overrideID,
				)
			}
			if importPath != normalizedImportPath {
				return nil, fmt.Errorf(
					"override can only take normalized import paths, invalid import path: %s provided for override: %s",
					importPath,
					overrideID,
				)
			}
		}
	}
	return &ManagedConfig{
		CcEnableArenas:        externalManagedConfig.CcEnableArenas,
		JavaMultipleFiles:     externalManagedConfig.JavaMultipleFiles,
		JavaStringCheckUtf8:   externalManagedConfig.JavaStringCheckUtf8,
		JavaPackagePrefix:     externalManagedConfig.JavaPackagePrefix,
		OptimizeFor:           optimizeFor,
		GoPackagePrefixConfig: goPackagePrefixConfig,
		Override:              override,
	}, nil
}

func newGoPackagePrefixConfigV1(externalGoPackagePrefixConfig ExternalGoPackagePrefixConfigV1) (*GoPackagePrefixConfig, error) {
	if externalGoPackagePrefixConfig.IsEmpty() {
		return nil, nil
	}
	if externalGoPackagePrefixConfig.Default == "" {
		return nil, errors.New("go_package_prefix setting requires a default value")
	}
	defaultGoPackagePrefix, err := normalpath.NormalizeAndValidate(externalGoPackagePrefixConfig.Default)
	if err != nil {
		return nil, fmt.Errorf("invalid go_package_prefix default: %w", err)
	}
	seenModuleIdentities := make(map[string]struct{}, len(externalGoPackagePrefixConfig.Except))
	except := make([]bufmodule.ModuleIdentity, 0, len(externalGoPackagePrefixConfig.Except))
	for _, moduleName := range externalGoPackagePrefixConfig.Except {
		moduleIdentity, err := bufmodule.ModuleIdentityForString(moduleName)
		if err != nil {
			return nil, fmt.Errorf("invalid go_package_prefix except: %w", err)
		}
		if _, ok := seenModuleIdentities[moduleIdentity.IdentityString()]; ok {
			return nil, fmt.Errorf("invalid go_package_prefix except: %q is defined multiple times", moduleIdentity.IdentityString())
		}
		seenModuleIdentities[moduleIdentity.IdentityString()] = struct{}{}
		except = append(except, moduleIdentity)
	}
	override := make(map[bufmodule.ModuleIdentity]string, len(externalGoPackagePrefixConfig.Override))
	for moduleName, goPackagePrefix := range externalGoPackagePrefixConfig.Override {
		moduleIdentity, err := bufmodule.ModuleIdentityForString(moduleName)
		if err != nil {
			return nil, fmt.Errorf("invalid go_package_prefix override key: %w", err)
		}
		normalizedGoPackagePrefix, err := normalpath.NormalizeAndValidate(goPackagePrefix)
		if err != nil {
			return nil, fmt.Errorf("invalid go_package_prefix override value: %w", err)
		}
		if _, ok := seenModuleIdentities[moduleIdentity.IdentityString()]; ok {
			return nil, fmt.Errorf("invalid go_package_prefix override: %q is already defined as an except", moduleIdentity.IdentityString())
		}
		seenModuleIdentities[moduleIdentity.IdentityString()] = struct{}{}
		override[moduleIdentity] = normalizedGoPackagePrefix
	}
	return &GoPackagePrefixConfig{
		Default:  defaultGoPackagePrefix,
		Except:   except,
		Override: override,
	}, nil
}

func newConfigV1Beta1(externalConfig ExternalConfigV1Beta1, id string) (*Config, error) {
	managedConfig, err := newManagedConfigV1Beta1(externalConfig.Options, externalConfig.Managed)
	if err != nil {
		return nil, err
	}
	pluginConfigs := make([]*PluginConfig, 0, len(externalConfig.Plugins))
	for _, plugin := range externalConfig.Plugins {
		strategy, err := ParseStrategy(plugin.Strategy)
		if err != nil {
			return nil, err
		}
		opt, err := encoding.InterfaceSliceOrStringToCommaSepString(plugin.Opt)
		if err != nil {
			return nil, err
		}
		pluginConfigs = append(
			pluginConfigs,
			&PluginConfig{
				Name:     plugin.Name,
				Out:      plugin.Out,
				Opt:      opt,
				Path:     plugin.Path,
				Strategy: strategy,
			},
		)
	}
	return &Config{
		PluginConfigs: pluginConfigs,
		ManagedConfig: managedConfig,
	}, nil
}

func validateExternalConfigV1Beta1(externalConfig ExternalConfigV1Beta1, id string) error {
	if len(externalConfig.Plugins) == 0 {
		return fmt.Errorf("%s: no plugins set", id)
	}
	for _, plugin := range externalConfig.Plugins {
		if plugin.Name == "" {
			return fmt.Errorf("%s: plugin name is required", id)
		}
		if plugin.Out == "" {
			return fmt.Errorf("%s: plugin %s out is required", id, plugin.Name)
		}
	}
	return nil
}

func newManagedConfigV1Beta1(externalOptionsConfig ExternalOptionsConfigV1Beta1, enabled bool) (*ManagedConfig, error) {
	if !enabled || externalOptionsConfig == (ExternalOptionsConfigV1Beta1{}) {
		return nil, nil
	}
	var optimizeFor *descriptorpb.FileOptions_OptimizeMode
	if externalOptionsConfig.OptimizeFor != "" {
		value, ok := descriptorpb.FileOptions_OptimizeMode_value[externalOptionsConfig.OptimizeFor]
		if !ok {
			return nil, fmt.Errorf(
				"invalid optimize_for value; expected one of %v",
				enumMapToStringSlice(descriptorpb.FileOptions_OptimizeMode_value),
			)
		}
		optimizeFor = optimizeModePtr(descriptorpb.FileOptions_OptimizeMode(value))
	}
	return &ManagedConfig{
		CcEnableArenas:    externalOptionsConfig.CcEnableArenas,
		JavaMultipleFiles: externalOptionsConfig.JavaMultipleFiles,
		OptimizeFor:       optimizeFor,
	}, nil
}

// enumMapToStringSlice is a convenience function for mapping Protobuf enums
// into a slice of strings.
func enumMapToStringSlice(enums map[string]int32) []string {
	slice := make([]string, 0, len(enums))
	for enum := range enums {
		slice = append(slice, enum)
	}
	return slice
}

// optimizeModePtr is a convenience function for initializing the
// *descriptorpb.FileOptions_OptimizeMode type in-line. This is
// also useful in unit tests.
func optimizeModePtr(value descriptorpb.FileOptions_OptimizeMode) *descriptorpb.FileOptions_OptimizeMode {
	return &value
}

type readConfigOptions struct {
	override string
}

func newReadConfigOptions() *readConfigOptions {
	return &readConfigOptions{}
}
