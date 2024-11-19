// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
