// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/gogo/protobuf/proto"
)

// MetricRules is the set of alerting and aggregation rules
// for KV. To add a new rule involving KV metrics, create
// a new member within the metricRules struct and add it
// to the `ruleRegistry`.
type MetricRules struct {
	ruleRegistry *metric.RuleRegistry

	// Alerting rules.
	unavailableRanges     *metric.AlertingRule
	underreplicatedRanges *metric.AlertingRule
	requestsStuckInRaft   *metric.AlertingRule
	highOpenFDCount       *metric.AlertingRule

	// Aggregation rules.
	nodeCapacity                  *metric.AggregationRule
	clusterCapacity               *metric.AggregationRule
	nodeCapacityAvailable         *metric.AggregationRule
	clusterCapacityAvailable      *metric.AggregationRule
	capacityAvailableRatio        *metric.AggregationRule
	nodeCapacityAvailableRatio    *metric.AggregationRule
	clusterCapacityAvailableRatio *metric.AggregationRule
}

// NewMetricRules creates a new MetricRules object for tracking KV
// metric rules.
func NewMetricRules(ruleRegistry *metric.RuleRegistry) *MetricRules {
	return &MetricRules{
		ruleRegistry: ruleRegistry,
	}
}

// CreateAndAddRules initializes all KV metric rules and adds them
// to the rule registry for tracking. All rules are exported in the
// YAML format.
func (mr *MetricRules) CreateAndAddRules(ctx context.Context) {
	mr.createAndRegisterUnavailableRangesRule(ctx)
	mr.createAndRegisterUnderReplicatedRangesRule(ctx)
	mr.createAndRegisterRequestsStuckInRaftRule(ctx)
	mr.createAndRegisterHighOpenFDCountRule(ctx)
	mr.createAndRegisterNodeCapacityRule(ctx)
	mr.createAndRegisterClusterCapacityRule(ctx)
	mr.createAndRegisterNodeCapacityAvailableRule(ctx)
	mr.createAndRegisterClusterCapacityAvailableRule(ctx)
	mr.createAndRegisterCapacityAvailableRatioRule(ctx)
	mr.createAndRegisterNodeCapacityAvailableRatioRule(ctx)
	mr.createAndRegisterClusterCapacityAvailableRatioRule(ctx)
}

func (mr *MetricRules) createAndRegisterUnavailableRangesRule(ctx context.Context) {
	name := "UnavailableRanges"
	expr := "(sum by(instance, cluster) (ranges_unavailable)) > 0"
	var annotations []metric.LabelPair
	annotations = append(annotations, metric.LabelPair{
		Name:  proto.String("summary"),
		Value: proto.String("Instance {{ $labels.instance }} has {{ $value }} unavailable ranges"),
	})
	recommendedHoldDuration := 10 * time.Minute
	help := "This check detects when the number of ranges with less than quorum replicas live are non-zero for too long"

	var err error
	mr.unavailableRanges, err = metric.NewAlertingRule(
		name,
		expr,
		annotations,
		nil,
		recommendedHoldDuration,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.unavailableRanges)
}

func (mr *MetricRules) createAndRegisterUnderReplicatedRangesRule(ctx context.Context) {
	name := "UnderreplicatedRanges"
	expr := "(sum by(instance, cluster) (ranges_underreplicated)) > 0"
	var annotations []metric.LabelPair
	annotations = append(annotations, metric.LabelPair{
		Name:  proto.String("summary"),
		Value: proto.String("Instance {{ $labels.instance }} has {{ $value }} under-replicated ranges"),
	})
	recommendedHoldDuration := time.Hour
	help := "This check detects when the number of ranges with less than desired replicas live is non-zero for too long."

	var err error
	mr.underreplicatedRanges, err = metric.NewAlertingRule(
		name,
		expr,
		annotations,
		nil,
		recommendedHoldDuration,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.underreplicatedRanges)
}

func (mr *MetricRules) createAndRegisterRequestsStuckInRaftRule(ctx context.Context) {
	name := "RequestsStuckInRaft"
	expr := "requests_slow_raft > 0"
	var annotations []metric.LabelPair
	annotations = append(annotations, metric.LabelPair{
		Name:  proto.String("summary"),
		Value: proto.String("{{ $value }} requests stuck in raft on {{ $labels.instance }}"),
	})
	recommendedHoldDuration := 10 * time.Minute
	help := "This check detects when requests are taking a very long time in replication."

	var err error
	mr.requestsStuckInRaft, err = metric.NewAlertingRule(
		name,
		expr,
		annotations,
		nil,
		recommendedHoldDuration,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.requestsStuckInRaft)
}

func (mr *MetricRules) createAndRegisterHighOpenFDCountRule(ctx context.Context) {
	name := "HighOpenFDCount"
	expr := "sys_fd_open / sys_fd_softlimit > 0.8"
	var annotations []metric.LabelPair
	annotations = append(annotations, metric.LabelPair{
		Name:  proto.String("summary"),
		Value: proto.String("Too many open file descriptors on {{ $labels.instance }}: {{ $value }} fraction used"),
	})
	recommendedHoldDuration := 10 * time.Minute
	help := "This check detects when a cluster is getting close to the open file descriptor limit"

	var err error
	mr.highOpenFDCount, err = metric.NewAlertingRule(
		name,
		expr,
		annotations,
		nil,
		recommendedHoldDuration,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.highOpenFDCount)
}

func (mr *MetricRules) createAndRegisterNodeCapacityRule(ctx context.Context) {
	name := "node:capacity"
	expr := "sum without(store) (capacity)"
	help := "Aggregation expression to compute node capacity."

	var err error
	mr.nodeCapacity, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.nodeCapacity)
}

func (mr *MetricRules) createAndRegisterClusterCapacityRule(ctx context.Context) {
	name := "cluster:capacity"
	expr := "sum without(instance) (node:capacity)"
	help := "Aggregation expression to compute cluster capacity."

	var err error
	mr.clusterCapacity, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.clusterCapacity)
}

func (mr *MetricRules) createAndRegisterNodeCapacityAvailableRule(ctx context.Context) {
	name := "node:capacity_available"
	expr := "sum without(store) (capacity_available)"
	help := "Aggregation expression to compute available capacity for a node."

	var err error
	mr.nodeCapacityAvailable, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.nodeCapacityAvailable)
}

func (mr *MetricRules) createAndRegisterClusterCapacityAvailableRule(ctx context.Context) {
	name := "cluster:capacity_available"
	expr := "sum without(instance) (node:capacity_available)"
	help := "Aggregation expression to compute available capacity for a cluster."

	var err error
	mr.clusterCapacityAvailable, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.clusterCapacityAvailable)
}

func (mr *MetricRules) createAndRegisterCapacityAvailableRatioRule(ctx context.Context) {
	name := "capacity_available:ratio"
	expr := "capacity_available / capacity"
	help := "Aggregation expression to compute available capacity ratio."

	var err error
	mr.capacityAvailableRatio, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.capacityAvailableRatio)
}

func (mr *MetricRules) createAndRegisterNodeCapacityAvailableRatioRule(ctx context.Context) {
	name := "node:capacity_available:ratio"
	expr := "node:capacity_available / node:capacity"
	help := "Aggregation expression to compute available capacity ratio for a node."

	var err error
	mr.nodeCapacityAvailableRatio, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.nodeCapacityAvailableRatio)
}

func (mr *MetricRules) createAndRegisterClusterCapacityAvailableRatioRule(ctx context.Context) {
	name := "cluster:capacity_available:ratio"
	expr := "cluster:capacity_available/cluster:capacity"
	help := "Aggregation expression to compute available capacity ratio for a cluster."

	var err error
	mr.clusterCapacityAvailableRatio, err = metric.NewAggregationRule(
		name,
		expr,
		nil,
		help,
		true,
	)
	mr.maybeAddRuleToRegistry(ctx, err, name, mr.clusterCapacityAvailableRatio)
}

func (mr *MetricRules) maybeAddRuleToRegistry(
	ctx context.Context, err error, name string, rule metric.Rule,
) {
	if err != nil {
		log.Errorf(ctx, "unable to create kv rule %s: %s", name, err.Error())
	}
	if mr.ruleRegistry == nil {
		log.Errorf(ctx, "unable to add kv rule %s: rule registry uninitialized", name)
	}
	mr.ruleRegistry.AddRule(rule)
}
