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

package internal

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/stringutil"
)

const (
	defaultEnumZeroValueSuffix = "_UNSPECIFIED"
	defaultServiceSuffix       = "Service"
)

// Config is the check config.
type Config struct {
	// Rules are the rules to run.
	//
	// Rules will be sorted by first categories, then id when Configs are
	// created from this package, i.e. created wth ConfigBuilder.NewConfig.
	Rules []*Rule

	IgnoreRootPaths     map[string]struct{}
	IgnoreIDToRootPaths map[string]map[string]struct{}

	AllowCommentIgnores    bool
	IgnoreUnstablePackages bool
}

// ConfigBuilder is a config builder.
type ConfigBuilder struct {
	Use    []string
	Except []string

	IgnoreRootPaths               []string
	IgnoreIDOrCategoryToRootPaths map[string][]string

	AllowCommentIgnores    bool
	IgnoreUnstablePackages bool

	EnumZeroValueSuffix                  string
	RPCAllowSameRequestResponse          bool
	RPCAllowGoogleProtobufEmptyRequests  bool
	RPCAllowGoogleProtobufEmptyResponses bool
	ServiceSuffix                        string
}

// NewConfig returns a new Config.
func (b ConfigBuilder) NewConfig(versionSpec *VersionSpec) (*Config, error) {
	return newConfig(b, versionSpec)
}

func newConfig(configBuilder ConfigBuilder, versionSpec *VersionSpec) (*Config, error) {
	configBuilder.Use = stringutil.SliceToUniqueSortedSliceFilterEmptyStrings(configBuilder.Use)
	configBuilder.Except = stringutil.SliceToUniqueSortedSliceFilterEmptyStrings(configBuilder.Except)
	if len(configBuilder.Use) == 0 {
		// default behavior
		configBuilder.Use = versionSpec.DefaultCategories
	}
	if configBuilder.EnumZeroValueSuffix == "" {
		configBuilder.EnumZeroValueSuffix = defaultEnumZeroValueSuffix
	}
	if configBuilder.ServiceSuffix == "" {
		configBuilder.ServiceSuffix = defaultServiceSuffix
	}
	return newConfigForRuleBuilders(
		configBuilder,
		versionSpec.RuleBuilders,
		versionSpec.IDToCategories,
	)
}

func newConfigForRuleBuilders(
	configBuilder ConfigBuilder,
	ruleBuilders []*RuleBuilder,
	idToCategories map[string][]string,
) (*Config, error) {
	// this checks that there are not duplicate IDs for a given revision
	// which would be a system error
	idToRuleBuilder, err := getIDToRuleBuilder(ruleBuilders)
	if err != nil {
		return nil, err
	}
	categoryToIDs := getCategoryToIDs(idToCategories)
	useIDMap, err := transformToIDMap(configBuilder.Use, idToCategories, categoryToIDs)
	if err != nil {
		return nil, err
	}
	exceptIDMap, err := transformToIDMap(configBuilder.Except, idToCategories, categoryToIDs)
	if err != nil {
		return nil, err
	}

	// this removes duplicates
	// we already know that a given rule with the same ID is equivalent
	resultIDToRuleBuilder := make(map[string]*RuleBuilder)

	for id := range useIDMap {
		ruleBuilder, ok := idToRuleBuilder[id]
		if !ok {
			return nil, fmt.Errorf("%q is not a known id after verification", id)
		}
		resultIDToRuleBuilder[ruleBuilder.id] = ruleBuilder
	}
	for id := range exceptIDMap {
		if _, ok := idToRuleBuilder[id]; !ok {
			return nil, fmt.Errorf("%q is not a known id after verification", id)
		}
		delete(resultIDToRuleBuilder, id)
	}

	resultRuleBuilders := make([]*RuleBuilder, 0, len(resultIDToRuleBuilder))
	for _, ruleBuilder := range resultIDToRuleBuilder {
		resultRuleBuilders = append(resultRuleBuilders, ruleBuilder)
	}
	resultRules := make([]*Rule, 0, len(resultRuleBuilders))
	for _, ruleBuilder := range resultRuleBuilders {
		categories, err := getRuleBuilderCategories(ruleBuilder, idToCategories)
		if err != nil {
			return nil, err
		}
		rule, err := ruleBuilder.NewRule(configBuilder, categories)
		if err != nil {
			return nil, err
		}
		resultRules = append(resultRules, rule)
	}
	sortRules(resultRules)

	ignoreIDToRootPathsUnnormalized, err := transformToIDToListMap(configBuilder.IgnoreIDOrCategoryToRootPaths, idToCategories, categoryToIDs)
	if err != nil {
		return nil, err
	}
	ignoreIDToRootPaths := make(map[string]map[string]struct{})
	for id, rootPaths := range ignoreIDToRootPathsUnnormalized {
		for rootPath := range rootPaths {
			if rootPath == "" {
				continue
			}
			rootPath, err := normalpath.NormalizeAndValidate(rootPath)
			if err != nil {
				return nil, err
			}
			if rootPath == "." {
				return nil, fmt.Errorf("cannot specify %q as an ignore path", rootPath)
			}
			resultRootPathMap, ok := ignoreIDToRootPaths[id]
			if !ok {
				resultRootPathMap = make(map[string]struct{})
				ignoreIDToRootPaths[id] = resultRootPathMap
			}
			resultRootPathMap[rootPath] = struct{}{}
		}
	}

	ignoreRootPaths := make(map[string]struct{}, len(configBuilder.IgnoreRootPaths))
	for _, rootPath := range configBuilder.IgnoreRootPaths {
		if rootPath == "" {
			continue
		}
		rootPath, err := normalpath.NormalizeAndValidate(rootPath)
		if err != nil {
			return nil, err
		}
		if rootPath == "." {
			return nil, fmt.Errorf("cannot specify %q as an ignore path", rootPath)
		}
		ignoreRootPaths[rootPath] = struct{}{}
	}

	return &Config{
		Rules:                  resultRules,
		IgnoreIDToRootPaths:    ignoreIDToRootPaths,
		IgnoreRootPaths:        ignoreRootPaths,
		AllowCommentIgnores:    configBuilder.AllowCommentIgnores,
		IgnoreUnstablePackages: configBuilder.IgnoreUnstablePackages,
	}, nil
}

func transformToIDMap(idsOrCategories []string, idToCategories map[string][]string, categoryToIDs map[string][]string) (map[string]struct{}, error) {
	if len(idsOrCategories) == 0 {
		return nil, nil
	}
	idMap := make(map[string]struct{}, len(idsOrCategories))
	for _, idOrCategory := range idsOrCategories {
		if idOrCategory == "" {
			continue
		}
		if _, ok := idToCategories[idOrCategory]; ok {
			id := idOrCategory
			idMap[id] = struct{}{}
		} else if ids, ok := categoryToIDs[idOrCategory]; ok {
			for _, id := range ids {
				idMap[id] = struct{}{}
			}
		} else {
			return nil, fmt.Errorf("%q is not a known id or category", idOrCategory)
		}
	}
	return idMap, nil
}

func transformToIDToListMap(idOrCategoryToList map[string][]string, idToCategories map[string][]string, categoryToIDs map[string][]string) (map[string]map[string]struct{}, error) {
	if len(idOrCategoryToList) == 0 {
		return nil, nil
	}
	idToListMap := make(map[string]map[string]struct{}, len(idOrCategoryToList))
	for idOrCategory, list := range idOrCategoryToList {
		if idOrCategory == "" {
			continue
		}
		if _, ok := idToCategories[idOrCategory]; ok {
			id := idOrCategory
			if _, ok := idToListMap[id]; !ok {
				idToListMap[id] = make(map[string]struct{})
			}
			for _, elem := range list {
				idToListMap[id][elem] = struct{}{}
			}
		} else if ids, ok := categoryToIDs[idOrCategory]; ok {
			for _, id := range ids {
				if _, ok := idToListMap[id]; !ok {
					idToListMap[id] = make(map[string]struct{})
				}
				for _, elem := range list {
					idToListMap[id][elem] = struct{}{}
				}
			}
		} else {
			return nil, fmt.Errorf("%q is not a known id or category", idOrCategory)
		}
	}
	return idToListMap, nil
}

func getCategoryToIDs(idToCategories map[string][]string) map[string][]string {
	categoryToIDs := make(map[string][]string)
	for id, categories := range idToCategories {
		for _, category := range categories {
			categoryToIDs[category] = append(categoryToIDs[category], id)
		}
	}
	return categoryToIDs
}

func getIDToRuleBuilder(ruleBuilders []*RuleBuilder) (map[string]*RuleBuilder, error) {
	m := make(map[string]*RuleBuilder)
	for _, ruleBuilder := range ruleBuilders {
		if _, ok := m[ruleBuilder.id]; ok {
			return nil, fmt.Errorf("duplicate rule ID: %q", ruleBuilder.id)
		}
		m[ruleBuilder.id] = ruleBuilder
	}
	return m, nil
}

func getRuleBuilderCategories(
	ruleBuilder *RuleBuilder,
	idToCategories map[string][]string,
) ([]string, error) {
	categories, ok := idToCategories[ruleBuilder.id]
	if !ok {
		return nil, fmt.Errorf("%q is not configured for categories", ruleBuilder.id)
	}
	// it is ok for categories to be empty, however the map must contain an entry
	// or otherwise this is a system error
	return categories, nil
}

func sortRules(rules []*Rule) {
	sort.Slice(
		rules,
		func(i int, j int) bool {
			// categories are sorted at this point
			// so we know the first category is a top-level category if present
			one := rules[i]
			two := rules[j]
			oneCategories := one.Categories()
			twoCategories := two.Categories()
			if len(oneCategories) > 0 && len(twoCategories) > 0 {
				compare := categoryCompare(oneCategories[0], twoCategories[0])
				if compare < 0 {
					return true
				}
				if compare > 0 {
					return false
				}
			}
			oneCategoriesString := strings.Join(oneCategories, ",")
			twoCategoriesString := strings.Join(twoCategories, ",")
			if oneCategoriesString < twoCategoriesString {
				return true
			}
			if oneCategoriesString > twoCategoriesString {
				return false
			}
			return one.ID() < two.ID()
		},
	)
}
