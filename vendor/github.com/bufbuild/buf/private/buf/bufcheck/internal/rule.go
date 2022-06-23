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
	"encoding/json"
	"sort"

	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/pkg/protosource"
)

// IgnoreFunc is an ignore function.
//
// Descriptors are the descriptors to check for ignores.
// Sometimes we have multiple descriptors, such as when we want to check previous
// file descriptors, or if an entire package is deleted.
//
// Locations are the locations to check for comment ignores.
// Sometimes, we have multiple locations to check, for example with RPC_REQUEST_STANDARD_NAME
// and RPC_RESPONSE_STANDARD_NAME, we want to check both the input/output type, and the method.
//
// Any descriptor or location may be nil.
type IgnoreFunc func(id string, descriptors []protosource.Descriptor, locations []protosource.Location) bool

// CheckFunc is a check function.
type CheckFunc func(id string, ignoreFunc IgnoreFunc, previousFiles []protosource.File, files []protosource.File) ([]bufanalysis.FileAnnotation, error)

// Rule provides a base embeddable rule.
type Rule struct {
	id         string
	categories []string
	purpose    string
	checkFunc  CheckFunc
}

// newRule returns a new Rule.
//
// Categories will be sorted and purpose will have "Checks that "
// prepended and "." appended.
func newRule(
	id string,
	categories []string,
	purpose string,
	checkFunc CheckFunc,
) *Rule {
	c := make([]string, len(categories))
	copy(c, categories)
	sort.Slice(
		c,
		func(i int, j int) bool {
			return categoryCompare(c[i], c[j]) < 0
		},
	)
	return &Rule{
		id:         id,
		categories: c,
		purpose:    "Checks that " + purpose + ".",
		checkFunc:  checkFunc,
	}
}

// ID implements Rule.
func (c *Rule) ID() string {
	return c.id
}

// Categories implements Rule.
func (c *Rule) Categories() []string {
	return c.categories
}

// Purpose implements Rule.
func (c *Rule) Purpose() string {
	return c.purpose
}

// MarshalJSON implements Rule.
func (c *Rule) MarshalJSON() ([]byte, error) {
	return json.Marshal(ruleJSON{ID: c.id, Categories: c.categories, Purpose: c.purpose})
}

func (c *Rule) check(ignoreFunc IgnoreFunc, previousFiles []protosource.File, files []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	return c.checkFunc(c.ID(), ignoreFunc, previousFiles, files)
}

type ruleJSON struct {
	ID         string   `json:"id" yaml:"id"`
	Categories []string `json:"categories" yaml:"categories"`
	Purpose    string   `json:"purpose" yaml:"purpose"`
}
