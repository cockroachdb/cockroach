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

type repositoryBranchPrinter struct {
	writer io.Writer
}

func newRepositoryBranchPrinter(
	writer io.Writer,
) *repositoryBranchPrinter {
	return &repositoryBranchPrinter{
		writer: writer,
	}
}

func (p *repositoryBranchPrinter) PrintRepositoryBranch(ctx context.Context, format Format, message *registryv1alpha1.RepositoryBranch) error {
	outputBranch := registryBranchToOutputBranch(message)
	switch format {
	case FormatText:
		return p.printRepositoryBranchesText([]outputRepositoryBranch{outputBranch})
	case FormatJSON:
		return json.NewEncoder(p.writer).Encode(outputBranch)
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *repositoryBranchPrinter) PrintRepositoryBranches(ctx context.Context, format Format, nextPageToken string, messages ...*registryv1alpha1.RepositoryBranch) error {
	if len(messages) == 0 {
		return nil
	}
	var outputRepositoryBranches []outputRepositoryBranch
	for _, repositoryBranch := range messages {
		outputRepositoryBranch := registryBranchToOutputBranch(repositoryBranch)
		outputRepositoryBranches = append(outputRepositoryBranches, outputRepositoryBranch)
	}
	switch format {
	case FormatText:
		return p.printRepositoryBranchesText(outputRepositoryBranches)
	case FormatJSON:
		return json.NewEncoder(p.writer).Encode(paginationWrapper{
			NextPage: nextPageToken,
			Results:  outputRepositoryBranches,
		})
	default:
		return fmt.Errorf("unknown format: %v", format)
	}
}

func (p *repositoryBranchPrinter) printRepositoryBranchesText(outputRepositoryBranches []outputRepositoryBranch) error {
	return WithTabWriter(
		p.writer,
		[]string{
			"Name",
			"Created",
		},
		func(tabWriter TabWriter) error {
			for _, outputRepositoryBranch := range outputRepositoryBranches {
				if err := tabWriter.Write(
					outputRepositoryBranch.Name,
					outputRepositoryBranch.CreateTime.Format(time.RFC3339),
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

type outputRepositoryBranch struct {
	ID         string    `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	CreateTime time.Time `json:"create_time,omitempty"`
}

func registryBranchToOutputBranch(repositoryBranch *registryv1alpha1.RepositoryBranch) outputRepositoryBranch {
	return outputRepositoryBranch{
		ID:         repositoryBranch.Id,
		Name:       repositoryBranch.Name,
		CreateTime: repositoryBranch.CreateTime.AsTime(),
	}
}
