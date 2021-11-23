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

// GetRuleForTest retrieves individual rule for testing purposes from
// the rule registry. The rules are retrieved in O(#rules) time.
func (r *RuleRegistry) GetRuleForTest(ruleName string) Rule {
	r.Lock()
	defer r.Unlock()
	for _, rule := range r.rules {
		if rule.Name() == ruleName {
			return rule
		}
	}
	return nil
}

// GetRuleCountForTest returns the total number of rules.
func (r *RuleRegistry) GetRuleCountForTest() int {
	return len(r.rules)
}
