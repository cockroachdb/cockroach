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
}

type AlertingRule struct {
	name        string
	expr        string
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

func (a *AlertingRule) Name() string {
	return a.name
}

func (a *AlertingRule) Labels() []LabelPair {
	return a.labels
}

func (a *AlertingRule) Expr() string {
	return a.expr
}

func (a *AlertingRule) Help() string {
	return a.help
}

func (a *AlertingRule) IsKV() bool {
	return a.isKV
}

func (ag *AggregationRule) Name() string {
	return ag.name
}

func (ag *AggregationRule) Labels() []LabelPair {
	return ag.labels
}

func (ag *AggregationRule) Expr() string {
	return ag.expr
}

func (ag *AggregationRule) Help() string {
	return ag.help
}

func (ag *AggregationRule) IsKV() bool {
	return ag.isKV
}
