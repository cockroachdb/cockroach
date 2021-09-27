// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

type RuleRegistry struct {
	syncutil.Mutex
	rules []Rule
}

// AddRules adds rules to the registry
func (r *RuleRegistry) AddRules(rules []Rule) {
	r.Lock()
	defer r.Unlock()
	r.rules = append(r.rules, rules...)
}

// AddRule adds a single rule to the registry
func (r *RuleRegistry) AddRule(rule Rule) {
	r.Lock()
	defer r.Unlock()
	r.rules = append(r.rules, rule)
}
