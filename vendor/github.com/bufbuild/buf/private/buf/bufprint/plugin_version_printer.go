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

package bufprint

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
)

type pluginVersionPrinter struct {
	writer io.Writer
}

func newPluginVersionPrinter(
	writer io.Writer,
) *pluginVersionPrinter {
	return &pluginVersionPrinter{
		writer: writer,
	}
}

func (p *pluginVersionPrinter) PrintPluginVersions(ctx context.Context, format Format, nextPageToken string, pluginVersions ...*registryv1alpha1.PluginVersion) error {
	switch format {
	case FormatText:
		return p.printPluginVersionsText(ctx, pluginVersions...)
	case FormatJSON:
		outputPlugins := make([]outputPluginVersion, 0, len(pluginVersions))
		for _, pluginVersion := range pluginVersions {
			outputPlugins = append(
				outputPlugins,
				registryPluginVersionToOutputPluginVersion(pluginVersion),
			)
		}
		return json.NewEncoder(p.writer).Encode(paginationWrapper{
			NextPage: nextPageToken,
			Results:  outputPlugins,
		})
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *pluginVersionPrinter) printPluginVersionsText(ctx context.Context, plugins ...*registryv1alpha1.PluginVersion) error {
	if len(plugins) == 0 {
		return nil
	}
	return WithTabWriter(
		p.writer,
		[]string{
			"Name",
			"Plugin Name",
			"Plugin Owner",
		},
		func(tabWriter TabWriter) error {
			for _, plugin := range plugins {
				if err := tabWriter.Write(
					plugin.Name,
					plugin.PluginName,
					plugin.PluginOwner,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type outputPluginVersion struct {
	Name        string `json:"name,omitempty"`
	PluginName  string `json:"plugin_name,omitempty"`
	PluginOwner string `json:"plugin_owner,omitempty"`
}

func registryPluginVersionToOutputPluginVersion(pluginVersion *registryv1alpha1.PluginVersion) outputPluginVersion {
	return outputPluginVersion{
		Name:        pluginVersion.Name,
		PluginName:  pluginVersion.PluginName,
		PluginOwner: pluginVersion.PluginOwner,
	}
}
