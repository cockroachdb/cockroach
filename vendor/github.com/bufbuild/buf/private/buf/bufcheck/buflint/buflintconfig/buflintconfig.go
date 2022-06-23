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

package buflintconfig

import (
	"bytes"
	"io"
	"sort"
	"strings"

	"github.com/bufbuild/buf/private/buf/bufcheck"
	"github.com/bufbuild/buf/private/buf/bufcheck/buflint/internal/buflintv1"
	"github.com/bufbuild/buf/private/buf/bufcheck/buflint/internal/buflintv1beta1"
	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
)

// Rule is a rule.
type Rule interface {
	bufcheck.Rule

	// InternalRule returns the internal Rule.
	InternalRule() *internal.Rule
}

// Config is the check config.
type Config struct {
	// Rules are the lint rules to run.
	//
	// Rules will be sorted by first categories, then id when Configs are
	// created from this package, i.e. created wth ConfigBuilder.NewConfig.
	Rules               []Rule
	IgnoreIDToRootPaths map[string]map[string]struct{}
	IgnoreRootPaths     map[string]struct{}
	AllowCommentIgnores bool
}

// GetRules returns the rules.
func (c *Config) GetRules() []bufcheck.Rule {
	return rulesToBufcheckRules(c.Rules)
}

// NewConfigV1Beta1 returns a new Config.
func NewConfigV1Beta1(externalConfig ExternalConfigV1Beta1) (*Config, error) {
	internalConfig, err := internal.ConfigBuilder{
		Use:                                  externalConfig.Use,
		Except:                               externalConfig.Except,
		IgnoreRootPaths:                      externalConfig.Ignore,
		IgnoreIDOrCategoryToRootPaths:        externalConfig.IgnoreOnly,
		AllowCommentIgnores:                  externalConfig.AllowCommentIgnores,
		EnumZeroValueSuffix:                  externalConfig.EnumZeroValueSuffix,
		RPCAllowSameRequestResponse:          externalConfig.RPCAllowSameRequestResponse,
		RPCAllowGoogleProtobufEmptyRequests:  externalConfig.RPCAllowGoogleProtobufEmptyRequests,
		RPCAllowGoogleProtobufEmptyResponses: externalConfig.RPCAllowGoogleProtobufEmptyResponses,
		ServiceSuffix:                        externalConfig.ServiceSuffix,
	}.NewConfig(
		buflintv1beta1.VersionSpec,
	)
	if err != nil {
		return nil, err
	}
	return internalConfigToConfig(internalConfig), nil
}

// NewConfigV1 returns a new Config.
func NewConfigV1(externalConfig ExternalConfigV1) (*Config, error) {
	internalConfig, err := internal.ConfigBuilder{
		Use:                                  externalConfig.Use,
		Except:                               externalConfig.Except,
		IgnoreRootPaths:                      externalConfig.Ignore,
		IgnoreIDOrCategoryToRootPaths:        externalConfig.IgnoreOnly,
		AllowCommentIgnores:                  externalConfig.AllowCommentIgnores,
		EnumZeroValueSuffix:                  externalConfig.EnumZeroValueSuffix,
		RPCAllowSameRequestResponse:          externalConfig.RPCAllowSameRequestResponse,
		RPCAllowGoogleProtobufEmptyRequests:  externalConfig.RPCAllowGoogleProtobufEmptyRequests,
		RPCAllowGoogleProtobufEmptyResponses: externalConfig.RPCAllowGoogleProtobufEmptyResponses,
		ServiceSuffix:                        externalConfig.ServiceSuffix,
	}.NewConfig(
		buflintv1.VersionSpec,
	)
	if err != nil {
		return nil, err
	}
	return internalConfigToConfig(internalConfig), nil
}

// GetAllRulesV1Beta1 gets all known rules.
//
// Should only be used for printing.
func GetAllRulesV1Beta1() ([]bufcheck.Rule, error) {
	config, err := NewConfigV1Beta1(
		ExternalConfigV1Beta1{
			Use: buflintv1beta1.VersionSpec.AllCategories,
		},
	)
	if err != nil {
		return nil, err
	}
	return rulesToBufcheckRules(config.Rules), nil
}

// GetAllRulesV1 gets all known rules.
//
// Should only be used for printing.
func GetAllRulesV1() ([]bufcheck.Rule, error) {
	config, err := NewConfigV1(
		ExternalConfigV1{
			Use: buflintv1.VersionSpec.AllCategories,
		},
	)
	if err != nil {
		return nil, err
	}
	return rulesToBufcheckRules(config.Rules), nil
}

// ExternalConfigV1Beta1 is an external config.
type ExternalConfigV1Beta1 struct {
	Use    []string `json:"use,omitempty" yaml:"use,omitempty"`
	Except []string `json:"except,omitempty" yaml:"except,omitempty"`
	// IgnoreRootPaths
	Ignore []string `json:"ignore,omitempty" yaml:"ignore,omitempty"`
	// IgnoreIDOrCategoryToRootPaths
	IgnoreOnly                           map[string][]string `json:"ignore_only,omitempty" yaml:"ignore_only,omitempty"`
	EnumZeroValueSuffix                  string              `json:"enum_zero_value_suffix,omitempty" yaml:"enum_zero_value_suffix,omitempty"`
	RPCAllowSameRequestResponse          bool                `json:"rpc_allow_same_request_response,omitempty" yaml:"rpc_allow_same_request_response,omitempty"`
	RPCAllowGoogleProtobufEmptyRequests  bool                `json:"rpc_allow_google_protobuf_empty_requests,omitempty" yaml:"rpc_allow_google_protobuf_empty_requests,omitempty"`
	RPCAllowGoogleProtobufEmptyResponses bool                `json:"rpc_allow_google_protobuf_empty_responses,omitempty" yaml:"rpc_allow_google_protobuf_empty_responses,omitempty"`
	ServiceSuffix                        string              `json:"service_suffix,omitempty" yaml:"service_suffix,omitempty"`
	AllowCommentIgnores                  bool                `json:"allow_comment_ignores,omitempty" yaml:"allow_comment_ignores,omitempty"`
}

// ExternalConfigV1 is an external config.
type ExternalConfigV1 struct {
	Use    []string `json:"use,omitempty" yaml:"use,omitempty"`
	Except []string `json:"except,omitempty" yaml:"except,omitempty"`
	// IgnoreRootPaths
	Ignore []string `json:"ignore,omitempty" yaml:"ignore,omitempty"`
	// IgnoreIDOrCategoryToRootPaths
	IgnoreOnly                           map[string][]string `json:"ignore_only,omitempty" yaml:"ignore_only,omitempty"`
	EnumZeroValueSuffix                  string              `json:"enum_zero_value_suffix,omitempty" yaml:"enum_zero_value_suffix,omitempty"`
	RPCAllowSameRequestResponse          bool                `json:"rpc_allow_same_request_response,omitempty" yaml:"rpc_allow_same_request_response,omitempty"`
	RPCAllowGoogleProtobufEmptyRequests  bool                `json:"rpc_allow_google_protobuf_empty_requests,omitempty" yaml:"rpc_allow_google_protobuf_empty_requests,omitempty"`
	RPCAllowGoogleProtobufEmptyResponses bool                `json:"rpc_allow_google_protobuf_empty_responses,omitempty" yaml:"rpc_allow_google_protobuf_empty_responses,omitempty"`
	ServiceSuffix                        string              `json:"service_suffix,omitempty" yaml:"service_suffix,omitempty"`
	AllowCommentIgnores                  bool                `json:"allow_comment_ignores,omitempty" yaml:"allow_comment_ignores,omitempty"`
}

// PrintFileAnnotations prints the FileAnnotations to the Writer.
//
// Also accepts config-ignore-yaml.
func PrintFileAnnotations(
	writer io.Writer,
	fileAnnotations []bufanalysis.FileAnnotation,
	formatString string,
) error {
	switch s := strings.ToLower(strings.TrimSpace(formatString)); s {
	case "config-ignore-yaml":
		return printFileAnnotationsConfigIgnoreYAML(writer, fileAnnotations)
	default:
		return bufanalysis.PrintFileAnnotations(writer, fileAnnotations, s)
	}
}

func printFileAnnotationsConfigIgnoreYAML(
	writer io.Writer,
	fileAnnotations []bufanalysis.FileAnnotation,
) error {
	if len(fileAnnotations) == 0 {
		return nil
	}
	ignoreIDToRootPathMap := make(map[string]map[string]struct{})
	for _, fileAnnotation := range fileAnnotations {
		fileInfo := fileAnnotation.FileInfo()
		if fileInfo == nil || fileAnnotation.Type() == "" {
			continue
		}
		rootPathMap, ok := ignoreIDToRootPathMap[fileAnnotation.Type()]
		if !ok {
			rootPathMap = make(map[string]struct{})
			ignoreIDToRootPathMap[fileAnnotation.Type()] = rootPathMap
		}
		rootPathMap[fileInfo.Path()] = struct{}{}
	}
	if len(ignoreIDToRootPathMap) == 0 {
		return nil
	}

	sortedIgnoreIDs := make([]string, 0, len(ignoreIDToRootPathMap))
	ignoreIDToSortedRootPaths := make(map[string][]string, len(ignoreIDToRootPathMap))
	for id, rootPathMap := range ignoreIDToRootPathMap {
		sortedIgnoreIDs = append(sortedIgnoreIDs, id)
		rootPaths := make([]string, 0, len(rootPathMap))
		for rootPath := range rootPathMap {
			rootPaths = append(rootPaths, rootPath)
		}
		sort.Strings(rootPaths)
		ignoreIDToSortedRootPaths[id] = rootPaths
	}
	sort.Strings(sortedIgnoreIDs)

	buffer := bytes.NewBuffer(nil)
	_, _ = buffer.WriteString(`version: v1
lint:
  ignore_only:
`)
	for _, id := range sortedIgnoreIDs {
		_, _ = buffer.WriteString("    ")
		_, _ = buffer.WriteString(id)
		_, _ = buffer.WriteString(":\n")
		for _, rootPath := range ignoreIDToSortedRootPaths[id] {
			_, _ = buffer.WriteString("      - ")
			_, _ = buffer.WriteString(rootPath)
			_, _ = buffer.WriteString("\n")
		}
	}
	_, err := writer.Write(buffer.Bytes())
	return err
}

func internalConfigToConfig(internalConfig *internal.Config) *Config {
	return &Config{
		Rules:               internalRulesToRules(internalConfig.Rules),
		IgnoreIDToRootPaths: internalConfig.IgnoreIDToRootPaths,
		IgnoreRootPaths:     internalConfig.IgnoreRootPaths,
		AllowCommentIgnores: internalConfig.AllowCommentIgnores,
	}
}

func internalRulesToRules(internalRules []*internal.Rule) []Rule {
	if internalRules == nil {
		return nil
	}
	rules := make([]Rule, len(internalRules))
	for i, internalRule := range internalRules {
		rules[i] = newRule(internalRule)
	}
	return rules
}

func rulesToBufcheckRules(rules []Rule) []bufcheck.Rule {
	if rules == nil {
		return nil
	}
	s := make([]bufcheck.Rule, len(rules))
	for i, e := range rules {
		s[i] = e
	}
	return s
}
