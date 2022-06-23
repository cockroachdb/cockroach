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

package bufconfig

import (
	"bytes"
	"context"
	"errors"
	"text/template"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/pkg/storage"
)

// If this is updated, make sure to update docs.buf.build TODO automate this

const (
	exampleName          = "buf.build/acme/weather"
	tmplUndocumentedData = `{{$top := .}}version: v1
{{if not .NameUnset}}name: {{.Name}}
{{end}}{{if not .DepsUnset}}deps:
{{range $dep := .Deps}}  - {{$dep}}
{{end}}{{end}}lint:
  use:
{{range $lint_id := .LintIDs}}    - {{$lint_id}}
{{end}}breaking:
  use:
{{range $breaking_id := .BreakingIDs}}    - {{$breaking_id}}
{{end}}`
	tmplDocumentationCommentsData = `{{$top := .}}# This specifies the configuration file version.
#
# This controls the configuration file layout, defaults, and lint/breaking
# rules and rule categories. Buf takes breaking changes seriously in
# all aspects, and none of these will ever change for a given version.
#
# The only valid versions are "v1beta1", "v1".
# This key is required.
version: v1

# name is the module name.
{{if .NameUnset}}#{{end}}name: {{.Name}}

# deps are the module dependencies
{{if .DepsUnset}}#{{end}}deps:
{{range $dep := .Deps}}{{if $top.DepsUnset}}#{{end}}  - {{$dep}}
{{end}}
# build contains the options for builds.
#
# This affects the behavior of buf build, as well as the build behavior
# for source lint and breaking change rules.
#
# If you want to build all files in your repository, this section can be
# omitted.
build:

  # excludes is the list of directories to exclude.
  #
  # These directories will not be built or checked. If a directory is excluded,
  # buf treats the directory as if it does not exist.
  #
  # All directory paths in exclude must be relative to the directory of
  # your buf.yaml. Only directories can be specified, and all specified
  # directories must within the root directory.
  {{if not .Uncomment}}#{{end}}excludes:
  {{if not .Uncomment}}#{{end}}  - foo
  {{if not .Uncomment}}#{{end}}  - bar/baz

# lint contains the options for lint rules.
lint:

  # use is the list of rule categories and ids to use for buf lint.
  #
  # Categories are sets of rule ids.
  # Run buf config ls-lint-rules --all to get a list of all rules.
  #
  # The union of the categories and ids will be used.
  #
  # The default is [DEFAULT].
  use:
{{range $lint_id := .LintIDs}}    - {{$lint_id}}
{{end}}
  # except is the list of rule ids or categories to remove from the use
  # list.
  #
  # This allows removal of specific rules from the union of rules derived
  # from the use list of categories and ids.
  {{if not .Uncomment}}#{{end}}except:
  {{if not .Uncomment}}#{{end}}  - ENUM_VALUE_UPPER_SNAKE_CASE

  # ignore is the list of directories or files to ignore for all rules.
  #
  # All directories and file paths are specified relative to the root directory.
  # The directory "." is not allowed - this is equivalent to ignoring
  # everything.
  {{if not .Uncomment}}#{{end}}ignore:
  {{if not .Uncomment}}#{{end}}  - bat
  {{if not .Uncomment}}#{{end}}  - ban/ban.proto

  # ignore_only is the map from rule id or category to file or directory to
  # ignore.
  #
  # All directories and file paths are specified relative to the root directory.
  # The directory "." is not allowed - this is equivalent to using the "except"
  # option.
  #
  # Note you can generate this section using
  # "buf lint --error-format=config-ignore-yaml". The result of this command
  # can be copy/pasted here.
  {{if not .Uncomment}}#{{end}}ignore_only:
  {{if not .Uncomment}}#{{end}}  ENUM_PASCAL_CASE:
  {{if not .Uncomment}}#{{end}}    - foo/foo.proto
  {{if not .Uncomment}}#{{end}}    - bar
  {{if not .Uncomment}}#{{end}}  FIELD_LOWER_SNAKE_CASE:
  {{if not .Uncomment}}#{{end}}    - foo

  # enum_zero_value_suffix affects the behavior of the ENUM_ZERO_VALUE_SUFFIX
  # rule.
  #
  # This will result in this suffix being used instead of the default
  # "_UNSPECIFIED" suffix.
  {{if not .Uncomment}}#{{end}}enum_zero_value_suffix: _UNSPECIFIED

  # rpc_allow_same_request_response affects the behavior of the
  # RPC_REQUEST_RESPONSE_UNIQUE rule.
  #
  # This will result in requests and responses being allowed to be the same
  # type for a single RPC.
  {{if not .Uncomment}}#{{end}}rpc_allow_same_request_response: false

  # rpc_allow_google_protobuf_empty_requests affects the behavior of the
  # RPC_REQUEST_STANDARD_NAME and RPC_REQUEST_RESPONSE_UNIQUE rules.
  #
  # This will result in google.protobuf.Empty requests being ignored for
  # RPC_REQUEST_STANDARD_NAME, and google.protobuf.Empty requests being allowed
  # in multiple RPCs.
  {{if not .Uncomment}}#{{end}}rpc_allow_google_protobuf_empty_requests: false

  # rpc_allow_google_protobuf_empty_responses affects the behavior of the
  # RPC_RESPONSE_STANDARD_NAME and the RPC_REQUEST_RESPONSE_UNIQUE rules.
  #
  # This will result in google.protobuf.Empty responses being ignored for
  # RPC_RESPONSE_STANDARD_NAME, and google.protobuf.Empty responses being
  # allowed in multiple RPCs.
  {{if not .Uncomment}}#{{end}}rpc_allow_google_protobuf_empty_responses: false

  # service_suffix affects the behavior of the SERVICE_SUFFIX rule.
  #
  # This will result in this suffix being used instead of the default "Service"
  # suffix.
  {{if not .Uncomment}}#{{end}}service_suffix: Service

  # allow_comment_ignores allows comment-driven ignores.
  #
  # If this option is set, leading comments can be added within Protobuf files
  # to ignore lint errors for certain components. If any line in a leading
  # comment starts with "buf:lint:ignore ID", then Buf will ignore lint errors
  # for this id. For example:
  #
  #   syntax = "proto3";
  #
  #   // buf:lint:ignore PACKAGE_LOWER_SNAKE_CASE
  #   // buf:lint:ignore PACKAGE_VERSION_SUFFIX
  #   package A;
  #
  # We do not recommend using this, as it allows individual engineers in a
  # large organization to decide on their own lint rule exceptions. However,
  # there are cases where this is necessarily, and we want users to be able to
  # make informed decisions, so we provide this as an opt-in.
  {{if not .Uncomment}}#{{end}}allow_comment_ignores: false

# breaking contains the options for breaking rules.
breaking:

  # use is the list of rule categories and ids to use for
  # buf breaking.
  #
  # Categories are sets of rule ids.
  # Run buf config ls-breaking-rules --all to get a list of all rules.
  #
  # The union of the categories and ids will be used.
  #
  # As opposed to lint, where you may want to do more customization, with
  # breaking is is generally better to only choose one of the following
  # options:
  #
  # - [FILE]
  # - [PACKAGE]
  # - [WIRE]
  # - [WIRE_JSON]
  #
  # The default is [FILE], as done below.
  use:
{{range $breaking_id := .BreakingIDs}}    - {{$breaking_id}}
{{end}}
  # except is the list of rule ids or categories to remove from the use
  # list.
  #
  # This allows removal of specific rules from the union of rules derived
  # from the use list of categories and ids.
  #
  # As opposed to lint, we generally recommend using one of the preconfigured
  # options. Removing specific rules from breaking change detection is an
  # advanced option.
  {{if not .Uncomment}}#{{end}}except:
  {{if not .Uncomment}}#{{end}}  - FIELD_SAME_NAME

  # ignore is the list of directories or files to ignore for all rules.
  #
  # All directories and file paths are specified relative to the root directory.
  # The directory "." is not allowed - this is equivalent to ignoring
  # everything.
  {{if not .Uncomment}}#{{end}}ignore:
  {{if not .Uncomment}}#{{end}}  - bat
  {{if not .Uncomment}}#{{end}}  - ban/ban.proto

  # ignore_only is the map from rule id or category to file or directory to
  # ignore.
  #
  # All directories and file paths are specified relative to a root directory.
  # The directory "." is not allowed - this is equivalent to using the "except"
  # option.
  {{if not .Uncomment}}#{{end}}ignore_only:
  {{if not .Uncomment}}#{{end}}  FIELD_NO_DELETE:
  {{if not .Uncomment}}#{{end}}    - foo/foo.proto
  {{if not .Uncomment}}#{{end}}    - bar
  {{if not .Uncomment}}#{{end}}  WIRE_JSON:
  {{if not .Uncomment}}#{{end}}    - foo

  # ignore_unstable_packages results in ignoring packages with a last component
  # that is one of the unstable forms recognized by the "PACKAGE_VERSION_SUFFIX"
  # lint rule. The following forms will be ignored:
  #
  # - v\d+test.*
  # - v\d+(alpha|beta)\d+
  # - v\d+p\d+(alpha|beta)\d+
  #
  # For example, if this option is set, the following packages will be ignored:
  #
  # - foo.bar.v1alpha1
  # - foo.bar.v1beta1
  # - foo.bar.v1test
  {{if not .Uncomment}}#{{end}}ignore_unstable_packages: false`
)

var (
	defaultLintIDs     = []string{"DEFAULT"}
	defaultBreakingIDs = []string{"FILE"}
	exampleDeps        = []string{
		"buf.build/acme/petapis",
		"buf.build/acme/pkg:alpha",
		"buf.build/acme/paymentapis:7e8b594e68324329a7aefc6e750d18b9",
	}
)

func writeConfig(
	ctx context.Context,
	writeBucket storage.WriteBucket,
	options ...WriteConfigOption,
) error {
	writeConfigOptions := newWriteConfigOptions()
	for _, option := range options {
		option(writeConfigOptions)
	}
	if writeConfigOptions.moduleIdentity == nil && len(writeConfigOptions.dependencyModuleReferences) > 0 {
		return errors.New("cannot set deps without a name for WriteConfig")
	}
	if !writeConfigOptions.documentationComments && writeConfigOptions.uncomment {
		return errors.New("cannot set uncomment without documentationComments for WriteConfig")
	}
	externalConfigV1 := ExternalConfigV1{
		Version: V1Version,
	}
	externalConfigV1.Lint.Use = defaultLintIDs
	externalConfigV1.Breaking.Use = defaultBreakingIDs
	if writeConfigOptions.moduleIdentity != nil {
		externalConfigV1.Name = writeConfigOptions.moduleIdentity.IdentityString()
	}
	for _, dependencyModuleReference := range writeConfigOptions.dependencyModuleReferences {
		externalConfigV1.Deps = append(
			externalConfigV1.Deps,
			dependencyModuleReference.String(),
		)
	}
	tmplData := tmplUndocumentedData
	if writeConfigOptions.documentationComments {
		tmplData = tmplDocumentationCommentsData
	}
	tmpl, err := template.New("tmpl").Parse(tmplData)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(nil)
	if err := tmpl.Execute(
		buffer,
		newTmplParam(
			externalConfigV1,
			writeConfigOptions.uncomment,
		),
	); err != nil {
		return err
	}
	return storage.PutPath(ctx, writeBucket, ExternalConfigV1FilePath, buffer.Bytes())
}

type tmplParam struct {
	Name        string
	NameUnset   bool
	Deps        []string
	DepsUnset   bool
	LintIDs     []string
	BreakingIDs []string
	Uncomment   bool
}

func newTmplParam(externalConfigV1 ExternalConfigV1, uncomment bool) *tmplParam {
	tmplParam := &tmplParam{
		Name:        externalConfigV1.Name,
		Deps:        externalConfigV1.Deps,
		LintIDs:     externalConfigV1.Lint.Use,
		BreakingIDs: externalConfigV1.Breaking.Use,
		Uncomment:   uncomment,
	}
	if tmplParam.Name == "" {
		tmplParam.Name = exampleName
		tmplParam.NameUnset = true
	}
	if len(tmplParam.Deps) == 0 {
		tmplParam.Deps = exampleDeps
		tmplParam.DepsUnset = true
	}
	return tmplParam
}

type writeConfigOptions struct {
	moduleIdentity             bufmodule.ModuleIdentity
	dependencyModuleReferences []bufmodule.ModuleReference
	documentationComments      bool
	uncomment                  bool
}

func newWriteConfigOptions() *writeConfigOptions {
	return &writeConfigOptions{}
}
