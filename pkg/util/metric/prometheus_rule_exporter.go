// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"gopkg.in/yaml.v3"
)

const (
	alertRuleGroupName     = "rules/alerts"
	recordingRuleGroupName = "rules/recording"
)

// PrometheusRuleNode represents an individual rule node within the YAML output.
type PrometheusRuleNode struct {
	Record      string            `yaml:"record,omitempty"`
	Alert       string            `yaml:"alert,omitempty"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

// PrometheusRuleGroup is a list of recording and alerting rules.
type PrometheusRuleGroup struct {
	Rules []PrometheusRuleNode `yaml:"rules"`
}

// PrometheusRuleExporter initializes recording and alert rules once from the registry.
// These initialized values will be reused for every alert/rule scrapes.
type PrometheusRuleExporter struct {
	ruleRegistry *RuleRegistry
	mu           struct {
		syncutil.Mutex
		// existingRules keeps track of rules already appended
		// with RuleGroups. This prevents duplication of rules
		// being added to RuleGroups map across multiple calls
		// to the exporter.
		existingRules map[string]struct{}
		// RuleGroups keeps track of all declared rules in a
		// YAML compatible format.
		RuleGroups map[string]PrometheusRuleGroup
	}
}

// NewPrometheusRuleExporter creates a new PrometheusRuleExporter.
func NewPrometheusRuleExporter(ruleRegistry *RuleRegistry) *PrometheusRuleExporter {
	pe := PrometheusRuleExporter{
		ruleRegistry: ruleRegistry,
	}
	pe.mu.RuleGroups = make(map[string]PrometheusRuleGroup)
	pe.mu.existingRules = make(map[string]struct{})
	return &pe
}

// ScrapeRegistry scrapes the RuleRegistry to convert the list of registered rules
// to Prometheus compatible rules.
func (re *PrometheusRuleExporter) ScrapeRegistry(ctx context.Context) {
	re.mu.Lock()
	defer re.mu.Unlock()
	re.ruleRegistry.Each(func(rule Rule) {
		ruleGroupName, ruleNode := rule.ToPrometheusRuleNode()
		if ruleGroupName != alertRuleGroupName && ruleGroupName != recordingRuleGroupName {
			log.Warning(ctx, "invalid prometheus group name, skipping rule")
			return
		}

		// If rule has already been added to exporter's tracked rules,
		// we can return
		if _, ok := re.mu.existingRules[rule.Name()]; ok {
			return
		}
		promRuleGroup := re.mu.RuleGroups[ruleGroupName]
		promRuleGroup.Rules = append(promRuleGroup.Rules, ruleNode)
		re.mu.RuleGroups[ruleGroupName] = promRuleGroup
		re.mu.existingRules[rule.Name()] = struct{}{}
	})
}

// PrintAsYAML returns the rules within PrometheusRuleExporter
// as YAML.
// TODO(rimadeodhar): Pipe through the help field as comments
// in the exported YAML.
func (re *PrometheusRuleExporter) PrintAsYAML() ([]byte, error) {
	re.mu.Lock()
	defer re.mu.Unlock()
	return yaml.Marshal(re.mu.RuleGroups)
}
