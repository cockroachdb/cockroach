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
	"time"

	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
)

type repositoryTagPrinter struct {
	writer io.Writer
}

func newRepositoryTagPrinter(
	writer io.Writer,
) *repositoryTagPrinter {
	return &repositoryTagPrinter{
		writer: writer,
	}
}

func (p *repositoryTagPrinter) PrintRepositoryTag(ctx context.Context, format Format, message *registryv1alpha1.RepositoryTag) error {
	outRepositoryTag := registryTagToOutputTag(message)
	switch format {
	case FormatText:
		return p.printRepositoryTagsText([]outputRepositoryTag{outRepositoryTag})
	case FormatJSON:
		return json.NewEncoder(p.writer).Encode(outRepositoryTag)
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *repositoryTagPrinter) PrintRepositoryTags(ctx context.Context, format Format, nextPageToken string, messages ...*registryv1alpha1.RepositoryTag) error {
	if len(messages) == 0 {
		return nil
	}
	outputRepositoryTags := registryTagsToOutputTags(messages)
	switch format {
	case FormatText:
		return p.printRepositoryTagsText(outputRepositoryTags)
	case FormatJSON:
		return json.NewEncoder(p.writer).Encode(paginationWrapper{
			NextPage: nextPageToken,
			Results:  outputRepositoryTags,
		})
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *repositoryTagPrinter) printRepositoryTagsText(outputRepositoryTags []outputRepositoryTag) error {
	return WithTabWriter(
		p.writer,
		[]string{
			"Name",
			"Commit",
			"Created",
		},
		func(tabWriter TabWriter) error {
			for _, outputRepositoryTag := range outputRepositoryTags {
				if err := tabWriter.Write(
					outputRepositoryTag.Name,
					outputRepositoryTag.Commit,
					outputRepositoryTag.CreateTime.Format(time.RFC3339),
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type outputRepositoryTag struct {
	ID         string    `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	Commit     string    `json:"commit,omitempty"`
	CreateTime time.Time `json:"create_time,omitempty"`
}

func registryTagToOutputTag(repositoryTag *registryv1alpha1.RepositoryTag) outputRepositoryTag {
	return outputRepositoryTag{
		Name:       repositoryTag.Name,
		Commit:     repositoryTag.CommitName,
		CreateTime: repositoryTag.CreateTime.AsTime(),
	}
}

func registryTagsToOutputTags(tags []*registryv1alpha1.RepositoryTag) []outputRepositoryTag {
	outputRepositoryTags := make([]outputRepositoryTag, len(tags))
	for i, repositoryTag := range tags {
		outputRepositoryTags[i] = registryTagToOutputTag(repositoryTag)
	}
	return outputRepositoryTags
}
