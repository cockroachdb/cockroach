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

import (
	"fmt"
	"strings"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/bufpkg/bufmodule/internal"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/stringutil"
)

func newConfigV1Beta1(externalConfig ExternalConfigV1Beta1, deps ...string) (*Config, error) {
	dependencyModuleReferences, err := parseDependencyModuleReferences(deps...)
	if err != nil {
		return nil, err
	}

	rootToExcludes := make(map[string][]string)

	roots := externalConfig.Roots
	// not yet relative to roots
	fullExcludes := externalConfig.Excludes

	if len(roots) == 0 {
		roots = []string{"."}
	}
	roots, err = internal.NormalizeAndCheckPaths(roots, "root", normalpath.Relative, true)
	if err != nil {
		return nil, err
	}
	for _, root := range roots {
		// we already checked duplicates, but just in case
		if _, ok := rootToExcludes[root]; ok {
			return nil, fmt.Errorf("unexpected duplicate root: %q", root)
		}
		rootToExcludes[root] = make([]string, 0)
	}

	if len(fullExcludes) == 0 {
		return &Config{
			RootToExcludes:             rootToExcludes,
			DependencyModuleReferences: dependencyModuleReferences,
		}, nil
	}

	// this also verifies that fullExcludes is unique
	fullExcludes, err = internal.NormalizeAndCheckPaths(fullExcludes, "exclude", normalpath.Relative, true)
	if err != nil {
		return nil, err
	}

	// verify that no exclude equals a root directly and only directories are specified
	for _, fullExclude := range fullExcludes {
		if normalpath.Ext(fullExclude) == ".proto" {
			return nil, fmt.Errorf("excludes can only be directories but file %s discovered", fullExclude)
		}
		if _, ok := rootToExcludes[fullExclude]; ok {
			return nil, fmt.Errorf("%s is both a root and exclude, which means the entire root is excluded, which is not valid", fullExclude)
		}
	}

	// verify that all excludes are within a root
	rootMap := stringutil.SliceToMap(roots)
	for _, fullExclude := range fullExcludes {
		switch matchingRoots := normalpath.MapAllEqualOrContainingPaths(rootMap, fullExclude, normalpath.Relative); len(matchingRoots) {
		case 0:
			return nil, fmt.Errorf("exclude %s is not contained in any root, which is not valid", fullExclude)
		case 1:
			root := matchingRoots[0]
			exclude, err := normalpath.Rel(root, fullExclude)
			if err != nil {
				return nil, err
			}
			// just in case
			exclude, err = normalpath.NormalizeAndValidate(exclude)
			if err != nil {
				return nil, err
			}
			rootToExcludes[root] = append(rootToExcludes[root], exclude)
		default:
			// this should never happen, but just in case
			return nil, fmt.Errorf("exclude %q was in multiple roots %v (system error)", fullExclude, matchingRoots)
		}
	}

	for root, excludes := range rootToExcludes {
		uniqueSortedExcludes := stringutil.SliceToUniqueSortedSliceFilterEmptyStrings(excludes)
		if len(excludes) != len(uniqueSortedExcludes) {
			// this should never happen, but just in case
			return nil, fmt.Errorf("excludes %v are not unique (system error)", excludes)
		}
		rootToExcludes[root] = uniqueSortedExcludes
	}
	return &Config{
		RootToExcludes:             rootToExcludes,
		DependencyModuleReferences: dependencyModuleReferences,
	}, nil
}

func newConfigV1(externalConfig ExternalConfigV1, deps ...string) (*Config, error) {
	dependencyModuleReferences, err := parseDependencyModuleReferences(deps...)
	if err != nil {
		return nil, err
	}
	// this also verifies that the excludes are unique, normalized, and validated
	excludes, err := internal.NormalizeAndCheckPaths(externalConfig.Excludes, "exclude", normalpath.Relative, true)
	if err != nil {
		return nil, err
	}
	for _, exclude := range excludes {
		if normalpath.Ext(exclude) == ".proto" {
			return nil, fmt.Errorf("excludes can only be directories but file %s discovered", exclude)
		}
	}
	uniqueSortedExcludes := stringutil.SliceToUniqueSortedSliceFilterEmptyStrings(excludes)
	if len(excludes) != len(uniqueSortedExcludes) {
		// this should never happen, but just in case
		return nil, fmt.Errorf("excludes %v are not unique (system error)", excludes)
	}
	rootToExcludes := map[string][]string{
		".": excludes, // all excludes are relative to the root
	}
	return &Config{
		RootToExcludes:             rootToExcludes,
		DependencyModuleReferences: dependencyModuleReferences,
	}, nil
}

func parseDependencyModuleReferences(deps ...string) ([]bufmodule.ModuleReference, error) {
	if len(deps) == 0 {
		return nil, nil
	}
	moduleReferences := make([]bufmodule.ModuleReference, 0, len(deps))
	for _, dep := range deps {
		dep := strings.TrimSpace(dep)
		moduleReference, err := bufmodule.ModuleReferenceForString(dep)
		if err != nil {
			return nil, err
		}
		moduleReferences = append(moduleReferences, moduleReference)
	}
	if err := bufmodule.ValidateModuleReferencesUniqueByIdentity(moduleReferences); err != nil {
		return nil, err
	}
	return moduleReferences, nil
}
