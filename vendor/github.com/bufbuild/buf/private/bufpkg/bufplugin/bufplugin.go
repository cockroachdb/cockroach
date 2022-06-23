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

package bufplugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appproto"
	"github.com/bufbuild/buf/private/pkg/encoding"
	"google.golang.org/protobuf/types/pluginpb"
)

const (
	// PluginsPathName is the path prefix used to signify that
	// a name belongs to a plugin.
	PluginsPathName = "plugins"

	// TemplatesPathName is the path prefix used to signify that
	// a name belongs to a template.
	TemplatesPathName = "templates"

	v1Version = "v1"
)

// ParsePluginPath parses a string in the format <buf.build/owner/plugins/name>
// into remote, owner and name.
func ParsePluginPath(pluginPath string) (remote string, owner string, name string, _ error) {
	if pluginPath == "" {
		return "", "", "", appcmd.NewInvalidArgumentError("a plugin path must be specified")
	}
	components := strings.Split(pluginPath, "/")
	if len(components) != 4 || components[2] != PluginsPathName {
		return "", "", "", appcmd.NewInvalidArgumentErrorf("%s is not a valid plugin path", pluginPath)
	}
	return components[0], components[1], components[3], nil
}

// ParsePluginVersionPath parses a string in the format <buf.build/owner/plugins/name[:version]>
// into remote, owner, name and version. The version is empty if not specified.
func ParsePluginVersionPath(pluginVersionPath string) (remote string, owner string, name string, version string, _ error) {
	remote, owner, name, err := ParsePluginPath(pluginVersionPath)
	if err != nil {
		return "", "", "", "", err
	}
	components := strings.Split(name, ":")
	switch len(components) {
	case 2:
		return remote, owner, components[0], components[1], nil
	case 1:
		return remote, owner, name, "", nil
	default:
		return "", "", "", "", fmt.Errorf("invalid version: %q", name)
	}
}

// ParseTemplatePath parses a string in the format <buf.build/owner/templates/name>
// into remote, owner and name.
func ParseTemplatePath(templatePath string) (remote string, owner string, name string, _ error) {
	if templatePath == "" {
		return "", "", "", appcmd.NewInvalidArgumentError("a template path must be specified")
	}
	components := strings.Split(templatePath, "/")
	if len(components) != 4 || components[2] != TemplatesPathName {
		return "", "", "", appcmd.NewInvalidArgumentErrorf("%s is not a valid template path", templatePath)
	}
	return components[0], components[1], components[3], nil
}

// ParseTemplateVersionPath parses a string in the format <buf.build/owner/templates/name:version>
// into remote, owner, name and version.
func ParseTemplateVersionPath(templateVersionPath string) (remote string, owner string, name string, version string, _ error) {
	remote, owner, name, err := ParseTemplatePath(templateVersionPath)
	if err != nil {
		return "", "", "", "", err
	}
	components := strings.Split(name, ":")
	if len(components) != 2 {
		return "", "", "", "", fmt.Errorf("invalid version: %q", name)
	}
	return remote, owner, components[0], components[1], nil
}

// ValidateTemplateName validates the format of the template name.
// This is only used for client side validation and attempts to avoid
// validation constraints that we may want to change.
func ValidateTemplateName(templateName string) error {
	if templateName == "" {
		return errors.New("template name is required")
	}
	return nil
}

// TemplateConfig is the config used to describe the plugins
// of a new template.
type TemplateConfig struct {
	Plugins []PluginConfig
}

// TemplateConfigToProtoPluginConfigs converts the template config to a slice of proto plugin configs,
// suitable for use with the Plugin Service CreateTemplate RPC.
func TemplateConfigToProtoPluginConfigs(templateConfig *TemplateConfig) []*registryv1alpha1.PluginConfig {
	pluginConfigs := make([]*registryv1alpha1.PluginConfig, 0, len(templateConfig.Plugins))
	for _, plugin := range templateConfig.Plugins {
		pluginConfigs = append(
			pluginConfigs,
			&registryv1alpha1.PluginConfig{
				PluginOwner: plugin.Owner,
				PluginName:  plugin.Name,
				Parameters:  plugin.Parameters,
			},
		)
	}
	return pluginConfigs
}

// PluginConfig is the config used to describe a plugin in
// a new template.
type PluginConfig struct {
	Owner      string
	Name       string
	Parameters []string
}

// ParseTemplateConfig parses the input template config as a path or JSON/YAML literal.
func ParseTemplateConfig(config string) (*TemplateConfig, error) {
	var data []byte
	var err error
	switch filepath.Ext(config) {
	case ".json", ".yaml", ".yml":
		data, err = os.ReadFile(config)
		if err != nil {
			return nil, fmt.Errorf("could not read file: %v", err)
		}
	default:
		data = []byte(config)
	}
	var version externalTemplateConfigVersion
	if err := encoding.UnmarshalJSONOrYAMLNonStrict(data, &version); err != nil {
		return nil, fmt.Errorf("failed to determine version of template config: %w", err)
	}
	switch version.Version {
	case "":
		return nil, errors.New("template config version is required")
	case v1Version:
	default:
		return nil, fmt.Errorf("unknown template config version: %q", version.Version)
	}
	var externalConfig externalTemplateConfig
	if err := encoding.UnmarshalJSONOrYAMLStrict(data, &externalConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal template config: %w", err)
	}
	templateConfig := &TemplateConfig{
		Plugins: make([]PluginConfig, 0, len(externalConfig.Plugins)),
	}
	for _, plugin := range externalConfig.Plugins {
		templatePlugin := PluginConfig{
			Owner: plugin.Owner,
			Name:  plugin.Name,
		}
		parameterString, err := encoding.InterfaceSliceOrStringToCommaSepString(plugin.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to parse options: %w", err)
		}
		if parameterString != "" {
			templatePlugin.Parameters = strings.Split(parameterString, ",")
		}
		templateConfig.Plugins = append(templateConfig.Plugins, templatePlugin)
	}
	return templateConfig, nil
}

// TemplateVersionConfig is the config used to describe the plugin
// version of a new template version.
type TemplateVersionConfig struct {
	PluginVersions []PluginVersion
}

// TemplateVersionConfigToProtoPluginVersionMappings converts the template version config to a
// slice of Plugin version mappings, suitable for use with the Plugin Service CreateTemplateVersion RPC.
func TemplateVersionConfigToProtoPluginVersionMappings(
	templateVersionConfig *TemplateVersionConfig,
) []*registryv1alpha1.PluginVersionMapping {
	pluginVersions := make([]*registryv1alpha1.PluginVersionMapping, 0, len(templateVersionConfig.PluginVersions))
	for _, pluginVersion := range templateVersionConfig.PluginVersions {
		pluginVersions = append(
			pluginVersions,
			&registryv1alpha1.PluginVersionMapping{
				PluginOwner: pluginVersion.Owner,
				PluginName:  pluginVersion.Name,
				Version:     pluginVersion.Version,
			},
		)
	}
	return pluginVersions
}

// PluginVersion describes a version of a plugin for
// use in a template version.
type PluginVersion struct {
	Owner   string
	Name    string
	Version string
}

// ParseTemplateVersionConfig parses the input template version config as a path or JSON/YAML literal.
func ParseTemplateVersionConfig(config string) (*TemplateVersionConfig, error) {
	var data []byte
	var err error
	switch filepath.Ext(config) {
	case ".json", ".yaml", ".yml":
		data, err = os.ReadFile(config)
		if err != nil {
			return nil, fmt.Errorf("could not read file: %v", err)
		}
	default:
		data = []byte(config)
	}
	var version externalTemplateConfigVersion
	if err := encoding.UnmarshalJSONOrYAMLNonStrict(data, &version); err != nil {
		return nil, fmt.Errorf("failed to determine version of template version config: %w", err)
	}
	switch version.Version {
	case "":
		return nil, errors.New("template version config version is required")
	case v1Version:
	default:
		return nil, fmt.Errorf("unknown template version config version: %q", version.Version)
	}
	var externalConfig externalTemplateVersionConfig
	if err := encoding.UnmarshalJSONOrYAMLStrict(data, &externalConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal template version config: %w", err)
	}
	templateVersionConfig := &TemplateVersionConfig{
		PluginVersions: make([]PluginVersion, 0, len(externalConfig.PluginVersions)),
	}
	for _, pluginVersion := range externalConfig.PluginVersions {
		templateVersionConfig.PluginVersions = append(templateVersionConfig.PluginVersions, PluginVersion(pluginVersion))
	}
	return templateVersionConfig, nil
}

// File represents a generated file
type File struct {
	Name    string
	Content []byte
}

// MergedPluginResult holds the files for a plugin result.
type MergedPluginResult struct {
	Files []*File
}

// MergeInsertionPoints traverses the plugin results and merges any insertion points present in any responses.
// The order of inserts depends on the order of the input plugins. The returned results are in the same order
// as the input plugin results.
func MergeInsertionPoints(responses []*pluginpb.CodeGeneratorResponse) ([]MergedPluginResult, error) {
	allFiles := make(map[string]*File)
	results := make([]MergedPluginResult, len(responses))
	for i, response := range responses {
		if response.Error != nil {
			return nil, fmt.Errorf("unexpected response error: %s", *response.Error)
		}
		var files []*File
		for _, generatedFile := range response.File {
			fileName := generatedFile.GetName()
			insertionPoint := generatedFile.GetInsertionPoint()
			if insertionPoint != "" {
				parentFile, ok := allFiles[fileName]
				if !ok {
					return nil, fmt.Errorf(
						"response %d requested insertion point in file %q which does not exist",
						i,
						fileName,
					)
				}
				newFileContents, err := appproto.ApplyInsertionPoint(
					context.Background(),
					generatedFile,
					bytes.NewBuffer(parentFile.Content),
				)
				if err != nil {
					return nil, fmt.Errorf(
						"failed to apply insertion point %q in file %q for response %d: %w",
						insertionPoint,
						fileName,
						i,
						err,
					)
				}
				allFiles[fileName].Content = newFileContents
				continue
			}
			if _, ok := allFiles[fileName]; ok {
				return nil, fmt.Errorf("file %q defined multiple times", fileName)
			}
			file := &File{
				Name:    fileName,
				Content: []byte(generatedFile.GetContent()),
			}
			files = append(files, file)
			allFiles[fileName] = file
		}
		results[i] = MergedPluginResult{
			Files: files,
		}
	}

	return results, nil
}

type externalTemplateConfig struct {
	Version string                 `json:"version,omitempty" yaml:"version,omitempty"`
	Plugins []externalPluginConfig `json:"plugins,omitempty" yaml:"plugins,omitempty"`
}

type externalPluginConfig struct {
	Owner   string      `json:"owner,omitempty" yaml:"owner,omitempty"`
	Name    string      `json:"name,omitempty" yaml:"name,omitempty"`
	Options interface{} `json:"opt,omitempty" yaml:"opt,omitempty"`
}

type externalTemplateVersionConfig struct {
	Version        string                  `json:"version,omitempty" yaml:"version,omitempty"`
	PluginVersions []externalPluginVersion `json:"plugin_versions,omitempty" yaml:"plugin_versions,omitempty"`
}

type externalPluginVersion struct {
	Owner   string `json:"owner,omitempty" yaml:"owner,omitempty"`
	Name    string `json:"name,omitempty" yaml:"name,omitempty"`
	Version string `json:"version,omitempty" yaml:"version,omitempty"`
}

type externalTemplateConfigVersion struct {
	Version string `json:"version,omitempty" yaml:"version,omitempty"`
}
