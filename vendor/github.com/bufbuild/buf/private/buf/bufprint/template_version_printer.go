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

type templateVersionPrinter struct {
	writer io.Writer
}

func newTemplateVersionPrinter(writer io.Writer) *templateVersionPrinter {
	return &templateVersionPrinter{
		writer: writer,
	}
}

func (t *templateVersionPrinter) PrintTemplateVersion(ctx context.Context, format Format, templateVersion *registryv1alpha1.TemplateVersion) error {
	switch format {
	case FormatText:
		return t.printTemplateVersionsText(ctx, templateVersion)
	case FormatJSON:
		return json.NewEncoder(t.writer).Encode(
			registryTemplateVersionToOutputTemplateVersion(templateVersion),
		)
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (t *templateVersionPrinter) PrintTemplateVersions(ctx context.Context, format Format, nextPageToken string, templateVersions ...*registryv1alpha1.TemplateVersion) error {
	switch format {
	case FormatText:
		return t.printTemplateVersionsText(ctx, templateVersions...)
	case FormatJSON:
		outputTemplateVersions := make([]outputTemplateVersion, 0, len(templateVersions))
		for _, templateVersion := range templateVersions {
			outputTemplateVersions = append(outputTemplateVersions, registryTemplateVersionToOutputTemplateVersion(templateVersion))
		}
		return json.NewEncoder(t.writer).Encode(paginationWrapper{
			NextPage: nextPageToken,
			Results:  outputTemplateVersions,
		})
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (t *templateVersionPrinter) printTemplateVersionsText(ctx context.Context, templateVersions ...*registryv1alpha1.TemplateVersion) error {
	if len(templateVersions) == 0 {
		return nil
	}
	return WithTabWriter(
		t.writer,
		[]string{
			"Name",
			"Template Owner",
			"Template Name",
		},
		func(tabWriter TabWriter) error {
			for _, templateVersion := range templateVersions {
				if err := tabWriter.Write(
					templateVersion.Name,
					templateVersion.TemplateName,
					templateVersion.TemplateOwner,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type outputTemplateVersion struct {
	Name          string `json:"name,omitempty"`
	TemplateOwner string `json:"template_owner,omitempty"`
	TemplateName  string `json:"template_name,omitempty"`
}

func registryTemplateVersionToOutputTemplateVersion(templateVersion *registryv1alpha1.TemplateVersion) outputTemplateVersion {
	return outputTemplateVersion{
		Name:          templateVersion.Name,
		TemplateOwner: templateVersion.TemplateOwner,
		TemplateName:  templateVersion.TemplateName,
	}
}
