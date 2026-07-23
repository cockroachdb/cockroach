// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// Rule interface exposes an API for alerting and aggregation rules to be
// consumed.
type Rule interface {
	// Name returns the name of the rule.
	Name() string
	// Labels returns the labels associated with
	// the rule.
	Labels() []LabelPair
	// Expr returns the prometheus expression for the rule.
	Expr() string
	// Help returns a help message for the rule.
	Help() string
	// IsKV returns true if the metrics involved in the
	// rule are for the KV layer.
	IsKV() bool
	// ToPrometheusRuleNode converts the rule to a prometheus equivalent
	// rule.This is used by the PrometheusRuleExporter to export all
	// rules in a prometheus compatible format.
	ToPrometheusRuleNode() (ruleGroupName string, ruleNode PrometheusRuleNode)
}

// AlertingRule encapsulates an alert specification
// for one or more metrics. The alert `expr` is
// specified using the PromQL syntax.
type AlertingRule struct {
	name string
	expr string
	// annotations can be used to provide
	// additional information about the alert
	// such as runbook links etc.
	annotations []LabelPair
	labels      []LabelPair
	// This will be the recommended hold duration
	// for the alert. This should be treated as optional
	// and left unset if there is no recommendation. This
	// maps to the 'for' field within a prometheus alert.
	recommendedHoldDuration time.Duration
	help                    string
	isKV                    bool
}

// AggregationRule encapsulates a way to specify how
// one or more metrics should be aggregated together.
// The aggregation `expr` is specified using PromQL
// syntax.
type AggregationRule struct {
	name   string
	expr   string
	labels []LabelPair
	help   string
	isKV   bool
}

// AlertingRule and AggregationRule should implement the Rule interface.
var _ Rule = &AlertingRule{}
var _ Rule = &AggregationRule{}

// NewAlertingRule creates a new AlertingRule. This returns an error if the expr parameter
// is not a valid PromQL expression.
func NewAlertingRule(
	name string,
	expr string,
	annotations []LabelPair,
	labels []LabelPair,
	recommendedHoldDuration time.Duration,
	help string,
	isKV bool,
) (*AlertingRule, error) {
	if _, err := parser.ParseExpr(expr); err != nil {
		return nil, err
	}
	rule := AlertingRule{
		name:                    name,
		expr:                    expr,
		annotations:             annotations,
		labels:                  labels,
		recommendedHoldDuration: recommendedHoldDuration,
		help:                    help,
		isKV:                    isKV,
	}
	return &rule, nil
}

// NewAggregationRule creates a new AggregationRule. This returns an error if the expr parameter
// is not a valid PromQL expression.
func NewAggregationRule(
	name string, expr string, labels []LabelPair, help string, isKV bool,
) (*AggregationRule, error) {
	if _, err := parser.ParseExpr(expr); err != nil {
		return nil, err
	}
	rule := AggregationRule{
		name:   name,
		expr:   expr,
		labels: labels,
		help:   help,
		isKV:   isKV,
	}
	return &rule, nil
}

// Name implements the Rule interface.
func (a *AlertingRule) Name() string {
	return a.name
}

// Labels implements the Rule interface.
func (a *AlertingRule) Labels() []LabelPair {
	return a.labels
}

// Expr implements the Rule interface.
func (a *AlertingRule) Expr() string {
	return a.expr
}

// Help implements the Rule interface.
func (a *AlertingRule) Help() string {
	return a.help
}

// IsKV implements the Rule interface.
func (a *AlertingRule) IsKV() bool {
	return a.isKV
}

// ToPrometheusRuleNode implements the Rule interface.
func (a *AlertingRule) ToPrometheusRuleNode() (ruleGroupName string, ruleNode PrometheusRuleNode) {
	var node PrometheusRuleNode
	node.Alert = a.name
	node.Expr = a.expr
	if a.recommendedHoldDuration > 0*time.Second {
		node.For = a.recommendedHoldDuration.String()
	}
	node.Labels = getLabelMap(a.labels)
	node.Annotations = getLabelMap(a.annotations)
	return alertRuleGroupName, node
}

// Name implements the Rule interface.
func (ag *AggregationRule) Name() string {
	return ag.name
}

// Labels implements the Rule interface.
func (ag *AggregationRule) Labels() []LabelPair {
	return ag.labels
}

// Expr implements the Rule interface.
func (ag *AggregationRule) Expr() string {
	return ag.expr
}

// Help implements the Rule interface.
func (ag *AggregationRule) Help() string {
	return ag.help
}

// IsKV implements the Rule interface.
func (ag *AggregationRule) IsKV() bool {
	return ag.isKV
}

// ToPrometheusRuleNode implements the Rule interface.
func (ag *AggregationRule) ToPrometheusRuleNode() (
	ruleGroupName string,
	ruleNode PrometheusRuleNode,
) {
	var node PrometheusRuleNode
	node.Record = ag.name
	node.Expr = ag.expr
	node.Labels = getLabelMap(ag.labels)
	return recordingRuleGroupName, node
}

func getLabelMap(labels []LabelPair) map[string]string {
	labelMap := make(map[string]string)
	for _, label := range labels {
		if label.Name != nil && label.Value != nil {
			labelMap[*label.Name] = *label.Value
		}
	}
	return labelMap
}
