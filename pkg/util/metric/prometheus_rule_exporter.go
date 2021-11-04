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
	mu struct {
		syncutil.Mutex
		RuleGroups map[string]PrometheusRuleGroup
	}
}

// NewPrometheusRuleExporter creates a new PrometheusRuleExporter.
func NewPrometheusRuleExporter() *PrometheusRuleExporter {
	var pe PrometheusRuleExporter
	pe.mu.RuleGroups = make(map[string]PrometheusRuleGroup)
	return &pe
}

// ScrapeRegistry scrapes the RuleRegistry to convert the list of registered rules
// to Prometheus compatible rules.
func (re *PrometheusRuleExporter) ScrapeRegistry(ctx context.Context, rr *RuleRegistry) {
	re.mu.Lock()
	defer re.mu.Unlock()
	rr.Each(func(rule Rule) {
		ruleGroupName, ruleNode := rule.ToPrometheusRuleNode()
		if ruleGroupName != alertRuleGroupName && ruleGroupName != recordingRuleGroupName {
			log.Warning(ctx, "invalid prometheus group name, skipping rule")
			return
		}
		promRuleGroup := re.mu.RuleGroups[ruleGroupName]
		promRuleGroup.Rules = append(promRuleGroup.Rules, ruleNode)
		re.mu.RuleGroups[ruleGroupName] = promRuleGroup
	})
}

// PrintAsYAMLText returns the rules within PrometheusRuleExporter
// as YAML text.
// TODO(rimadeodhar): Pipe through the help field as comments
// in the exported YAML.
func (re *PrometheusRuleExporter) PrintAsYAMLText() (string, error) {
	re.mu.Lock()
	defer re.mu.Unlock()
	output, err := yaml.Marshal(re.mu.RuleGroups)
	if err != nil {
		return "", err
	}
	return string(output), nil
}
