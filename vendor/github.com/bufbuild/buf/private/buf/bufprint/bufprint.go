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
	"fmt"
	"io"
	"strconv"

	"github.com/bufbuild/buf/private/gen/proto/apiclient/buf/alpha/registry/v1alpha1/registryv1alpha1apiclient"
	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/protoencoding"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

const (
	// FormatText is the text format.
	FormatText Format = 1
	// FormatJSON is the JSON format.
	FormatJSON Format = 2
)

var (
	// AllFormatsString is the string representation of all Formats.
	AllFormatsString = stringutil.SliceToString([]string{FormatText.String(), FormatJSON.String()})
)

// Format is a format to print.
type Format int

// ParseFormat parses the format.
//
// If the empty string is provided, this is interpeted as FormatText.
func ParseFormat(s string) (Format, error) {
	switch s {
	case "", "text":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	default:
		return 0, fmt.Errorf("unknown format: %s", s)
	}
}

// String implements fmt.Stringer.
func (f Format) String() string {
	switch f {
	case FormatText:
		return "text"
	case FormatJSON:
		return "json"
	default:
		return strconv.Itoa(int(f))
	}
}

// OrganizationPrinter is an organization printer.
type OrganizationPrinter interface {
	PrintOrganization(ctx context.Context, format Format, organization *registryv1alpha1.Organization) error
}

// NewOrganizationPrinter returns a new OrganizationPrinter.
func NewOrganizationPrinter(address string, writer io.Writer) OrganizationPrinter {
	return newOrganizationPrinter(address, writer)
}

// RepositoryPrinter is a repository printer.
type RepositoryPrinter interface {
	PrintRepository(ctx context.Context, format Format, repository *registryv1alpha1.Repository) error
	PrintRepositories(ctx context.Context, format Format, nextPageToken string, repositories ...*registryv1alpha1.Repository) error
}

// NewRepositoryPrinter returns a new RepositoryPrinter.
func NewRepositoryPrinter(
	apiProvider registryv1alpha1apiclient.Provider,
	address string,
	writer io.Writer,
) RepositoryPrinter {
	return newRepositoryPrinter(apiProvider, address, writer)
}

// RepositoryBranchPrinter is a repository branch printer.
type RepositoryBranchPrinter interface {
	PrintRepositoryBranch(ctx context.Context, format Format, repositoryBranch *registryv1alpha1.RepositoryBranch) error
	PrintRepositoryBranches(ctx context.Context, format Format, nextPageToken string, repositoryBranches ...*registryv1alpha1.RepositoryBranch) error
}

// NewRepositoryBranchPrinter returns a new RepositoryBranchPrinter.
func NewRepositoryBranchPrinter(writer io.Writer) RepositoryBranchPrinter {
	return newRepositoryBranchPrinter(writer)
}

// RepositoryTagPrinter is a repository tag printer.
type RepositoryTagPrinter interface {
	PrintRepositoryTag(ctx context.Context, format Format, repositoryTag *registryv1alpha1.RepositoryTag) error
	PrintRepositoryTags(ctx context.Context, format Format, nextPageToken string, repositoryTags ...*registryv1alpha1.RepositoryTag) error
}

// NewRepositoryTagPrinter returns a new RepositoryTagPrinter.
func NewRepositoryTagPrinter(writer io.Writer) RepositoryTagPrinter {
	return newRepositoryTagPrinter(writer)
}

// RepositoryCommitPrinter is a repository commit printer.
type RepositoryCommitPrinter interface {
	PrintRepositoryCommit(ctx context.Context, format Format, repositoryCommit *registryv1alpha1.RepositoryCommit) error
	PrintRepositoryCommits(ctx context.Context, format Format, nextPageToken string, repositoryCommits ...*registryv1alpha1.RepositoryCommit) error
}

// NewRepositoryCommitPrinter returns a new RepositoryCommitPrinter.
func NewRepositoryCommitPrinter(writer io.Writer) RepositoryCommitPrinter {
	return newRepositoryCommitPrinter(writer)
}

// PluginPrinter is a printer for plugins.
type PluginPrinter interface {
	PrintPlugin(ctx context.Context, format Format, plugin *registryv1alpha1.Plugin) error
	PrintPlugins(ctx context.Context, format Format, nextPageToken string, plugins ...*registryv1alpha1.Plugin) error
}

// NewPluginPrinter returns a new PluginPrinter.
func NewPluginPrinter(writer io.Writer) PluginPrinter {
	return newPluginPrinter(writer)
}

// PluginVersionPrinter is a printer for PluginVersions.
type PluginVersionPrinter interface {
	PrintPluginVersions(ctx context.Context, format Format, nextPageToken string, pluginVersions ...*registryv1alpha1.PluginVersion) error
}

// NewPluginVersionPrinter returns a new NewPluginVersionPrinter.
func NewPluginVersionPrinter(writer io.Writer) PluginVersionPrinter {
	return newPluginVersionPrinter(writer)
}

// TemplatePrinter is a printer for Templates.
type TemplatePrinter interface {
	PrintTemplate(ctx context.Context, format Format, template *registryv1alpha1.Template) error
	PrintTemplates(ctx context.Context, format Format, nextPageToken string, templates ...*registryv1alpha1.Template) error
}

// NewTemplatePrinter returns a new NewTemplatePrinter.
func NewTemplatePrinter(writer io.Writer) TemplatePrinter {
	return newTemplatePrinter(writer)
}

// TemplateVersionPrinter is a printer for TemplateVersions.
type TemplateVersionPrinter interface {
	PrintTemplateVersion(ctx context.Context, format Format, templateVersion *registryv1alpha1.TemplateVersion) error
	PrintTemplateVersions(ctx context.Context, format Format, nextPageToken string, templateVersions ...*registryv1alpha1.TemplateVersion) error
}

// NewTemplateVersionPrinter returns a new NewTemplateVersionPrinter.
func NewTemplateVersionPrinter(writer io.Writer) TemplateVersionPrinter {
	return newTemplateVersionPrinter(writer)
}

// TokenPrinter is a printer Tokens.
//
// TODO: update to same format as other printers.
type TokenPrinter interface {
	PrintTokens(ctx context.Context, tokens ...*registryv1alpha1.Token) error
}

// NewTokenPrinter returns a new TokenPrinter.
//
// TODO: update to same format as other printers.
func NewTokenPrinter(writer io.Writer, format Format) (TokenPrinter, error) {
	switch format {
	case FormatText:
		return newTokenTextPrinter(writer), nil
	case FormatJSON:
		return newTokenJSONPrinter(writer), nil
	default:
		return nil, fmt.Errorf("unknown format: %v", format)
	}
}

// TabWriter is a tab writer.
type TabWriter interface {
	Write(values ...string) error
}

// WithTabWriter calls a function with a TabWriter.
//
// Shared with internal packages.
func WithTabWriter(
	writer io.Writer,
	header []string,
	f func(TabWriter) error,
) (retErr error) {
	tabWriter := newTabWriter(writer)
	defer func() {
		retErr = multierr.Append(retErr, tabWriter.Flush())
	}()
	if err := tabWriter.Write(header...); err != nil {
		return err
	}
	return f(tabWriter)
}

// printProtoMessageJSON prints the Protobuf message as JSON.
func printProtoMessageJSON(writer io.Writer, message proto.Message) error {
	data, err := protoencoding.NewJSONMarshalerIndent(nil).Marshal(message)
	if err != nil {
		return err
	}
	_, err = writer.Write(append(data, []byte("\n")...))
	return err
}
