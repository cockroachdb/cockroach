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

type templatePrinter struct {
	writer io.Writer
}

func newTemplatePrinter(writer io.Writer) *templatePrinter {
	return &templatePrinter{
		writer: writer,
	}
}

func (t *templatePrinter) PrintTemplate(ctx context.Context, format Format, template *registryv1alpha1.Template) error {
	switch format {
	case FormatText:
		return t.printTemplatesText(ctx, template)
	case FormatJSON:
		return json.NewEncoder(t.writer).Encode(
			registryTemplateToOutputTemplate(template),
		)
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (t *templatePrinter) PrintTemplates(ctx context.Context, format Format, nextPageToken string, templates ...*registryv1alpha1.Template) error {
	switch format {
	case FormatText:
		return t.printTemplatesText(ctx, templates...)
	case FormatJSON:
		outputTemplates := make([]outputTemplate, 0, len(templates))
		for _, template := range templates {
			outputTemplates = append(outputTemplates, registryTemplateToOutputTemplate(template))
		}
		return json.NewEncoder(t.writer).Encode(paginationWrapper{
			NextPage: nextPageToken,
			Results:  outputTemplates,
		})
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (t *templatePrinter) printTemplatesText(ctx context.Context, templates ...*registryv1alpha1.Template) error {
	if len(templates) == 0 {
		return nil
	}
	return WithTabWriter(
		t.writer,
		[]string{
			"Owner",
			"Name",
		},
		func(tabWriter TabWriter) error {
			for _, template := range templates {
				if err := tabWriter.Write(
					template.Owner,
					template.Name,
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type outputTemplate struct {
	Name       string `json:"name,omitempty"`
	Owner      string `json:"owner,omitempty"`
	Visibility string `json:"visibility,omitempty"`
}

func registryTemplateToOutputTemplate(template *registryv1alpha1.Template) outputTemplate {
	return outputTemplate{
		Name:       template.Name,
		Owner:      template.Owner,
		Visibility: template.Visibility.String(),
	}
}
