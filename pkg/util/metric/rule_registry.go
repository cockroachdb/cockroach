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

// RuleRegistry is a list of all rules (AlertingRule and AggregationRule).
//
// All defined rules should be registered in the RuleRegistry to be exported
// as Prometheus alert/recording rules.
type RuleRegistry struct {
	syncutil.Mutex
	rules []Rule
}

// NewRuleRegistry creates a new RuleRegistry.
func NewRuleRegistry() *RuleRegistry {
	return &RuleRegistry{
		rules: []Rule{},
	}
}

// AddRule adds a rule to the registry
func (r *RuleRegistry) AddRule(rule Rule) {
	r.Lock()
	defer r.Unlock()
	r.rules = append(r.rules, rule)
}
