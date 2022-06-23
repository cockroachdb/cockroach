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

package bufmoduleconfig

import "github.com/bufbuild/buf/private/bufpkg/bufmodule"

// Config is a configuration for build.
type Config struct {
	// RootToExcludes contains a map from root to the excludes for that root.
	//
	// Roots are the root directories within a bucket to search for Protobuf files.
	//
	// There will be no between the roots, ie foo/bar and foo are not allowed.
	// All Protobuf files must be unique relative to the roots, ie if foo and bar
	// are roots, then foo/baz.proto and bar/baz.proto are not allowed.
	//
	// All roots will be normalized and validated.
	//
	// Excludes are the directories within a bucket to exclude.
	//
	// There should be no overlap between the excludes, ie foo/bar and foo are not allowed.
	//
	// All excludes must reside within a root, but none will be equal to a root.
	// All excludes will be normalized and validated.
	// The excludes in this map will be relative to the root they map to!
	//
	// If RootToExcludes is empty, the default is "." with no excludes.
	RootToExcludes             map[string][]string
	DependencyModuleReferences []bufmodule.ModuleReference
}

// NewConfigV1Beta1 returns a new, validated Config for the ExternalConfig.
func NewConfigV1Beta1(externalConfig ExternalConfigV1Beta1, deps ...string) (*Config, error) {
	return newConfigV1Beta1(externalConfig, deps...)
}

// NewConfigV1 returns a new, validated Config for the ExternalConfig.
func NewConfigV1(externalConfig ExternalConfigV1, deps ...string) (*Config, error) {
	return newConfigV1(externalConfig, deps...)
}

// ExternalConfigV1Beta1 is an external config.
type ExternalConfigV1Beta1 struct {
	Roots    []string `json:"roots,omitempty" yaml:"roots,omitempty"`
	Excludes []string `json:"excludes,omitempty" yaml:"excludes,omitempty"`
}

// ExternalConfigV1 is an external config.
type ExternalConfigV1 struct {
	Excludes []string `json:"excludes,omitempty" yaml:"excludes,omitempty"`
}
