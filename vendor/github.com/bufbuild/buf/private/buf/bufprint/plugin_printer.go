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

type pluginPrinter struct {
	writer io.Writer
}

func newPluginPrinter(
	writer io.Writer,
) *pluginPrinter {
	return &pluginPrinter{
		writer: writer,
	}
}

func (p *pluginPrinter) PrintPlugin(ctx context.Context, format Format, plugin *registryv1alpha1.Plugin) error {
	switch format {
	case FormatText:
		return p.printPluginsText(ctx, plugin)
	case FormatJSON:
		return json.NewEncoder(p.writer).Encode(
			registryPluginToOutputPlugin(plugin),
		)
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *pluginPrinter) PrintPlugins(ctx context.Context, format Format, nextPageToken string, plugins ...*registryv1alpha1.Plugin) error {
	switch format {
	case FormatText:
		return p.printPluginsText(ctx, plugins...)
	case FormatJSON:
		outputPlugins := make([]outputPlugin, 0, len(plugins))
		for _, plugin := range plugins {
			outputPlugins = append(outputPlugins, registryPluginToOutputPlugin(plugin))
		}
		return json.NewEncoder(p.writer).Encode(paginationWrapper{
			NextPage: nextPageToken,
			Results:  outputPlugins,
		})
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *pluginPrinter) printPluginsText(ctx context.Context, plugins ...*registryv1alpha1.Plugin) error {
	if len(plugins) == 0 {
		return nil
	}
	return WithTabWriter(
		p.writer,
		[]string{
			"Owner",
			"Name",
		},
		func(tabWriter TabWriter) error {
			for _, plugin := range plugins {
				if err := tabWriter.Write(
					plugin.Owner,
					plugin.Name,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type outputPlugin struct {
	Name       string `json:"name,omitempty"`
	Owner      string `json:"owner,omitempty"`
	Visibility string `json:"visibility,omitempty"`
}

func registryPluginToOutputPlugin(plugin *registryv1alpha1.Plugin) outputPlugin {
	return outputPlugin{
		Name:       plugin.Name,
		Owner:      plugin.Owner,
		Visibility: plugin.Visibility.String(),
	}
}
