// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package metric provides server metrics (a.k.a. transient stats) and a framework
for using these metrics in alerting and aggregation rules for CockroachDB server.
These metrics are persisted to the time-series database and
are viewable through the web interface and the /_status/metrics/<NODEID> HTTP
endpoint.
All rules created for metrics are exported through an HTTP endpoint in a YAML
format at /api/v2/rules/. These rules are meant to be used as guidelines from
engineers implementing metric(s) on how metric(s) can be aggregated and used in alerts.
The expressions used to specify the rules use PromQL syntax and are validated
while creating new rules. The exported rules are intended to be used as is or modified
to set up alerts and dashboards within Prometheus by SREs (Cockroach Cloud clusters) and
our customers(self-hosted CockroachDB clusters).

# Adding a new metric

First, add the metric to a Registry.

Next, call methods such as Counter() and Rate() on the Registry to register the
metric. For example:

	exec := &Executor{
		...
		selectCount: sqlRegistry.Counter("sql.select.count"),
		...
	}

This code block registers the metric "sql.select.count" in sql.Registry. The
metric can then be accessed through the "selectCount" variable, which can be
updated as follows:

	func (e *Executor) doSelect() {
		// do the SELECT
		e.selectCount.Inc(1)
	}

To add the metric to the web UI, modify the appropriate file in
"ui/ts/pages/*.ts". Someone more qualified than me can elaborate, like @maxlang.

# Sub-registries

It's common for a Registry to become part of another Registry through the "Add"
and "MustAdd" methods.

	func NewNodeStatusMonitor(serverRegistry *registry) {
		nodeRegistry := metric.NewRegistry()

		// Add the registry for this node to the root-level server Registry. When
		// accessed from through the serverRegistry, all metrics from the
		// nodeRegistry will have the prefix "cr.node.".
		serverRegistry.MustAdd("cr.node.%s", nodeRegistry)
	}

Node-level sub-registries are added by calling:

	(*metric.MetricRecorder).AddNodeRegistry(YOUR_NODE_SUBREGISTRY)

# Adding a new rule

There are two types of rules:
 1. AlertingRule: This rule is used to provide guidelines on how
    one or metrics can be used for alerts.
 2. AggregationRule: This rule is used to provide guidelines on
    how one or more metrics can be aggregated to provide indicators
    about system health.

Both rules use PromQL syntax for specifying the expression for the rule.
The expression is validated while initializing a new rule using the
Prometheus library.

To export a new aggregation or alerting rule:
 1. Create a new rule using the NewAlertingRule / NewAggregationRule
    constructor. For example:
    rule := metric.NewAlertingRule(
    ruleName,
    promQLExpression,
    annotations,
    labels,
    recommendedHoldDuration,
    help,
    isKV,
    )
    The isKV field is a boolean field and should be set to true
    for all rules involving KV metrics. For alerting rules, it is highly
    recommended providing runbook-like information about what steps need
    to be taken to handle the alert. You can provide this information in
    the annotations field of the AlertingRule.
 2. Register the rules with the ruleRegistry parameter of the KV server
    using AddRule() method within the RuleRegistry.

# Testing

After your test does something to trigger your new metric update, you'll
probably want to call methods in TestServer such as MustGetSQLCounter() to
verify that the metric was updated correctly. See "sql/metric_test.go" for an
example.

Additionally, you can manually verify that your metric is updating by using the
metrics endpoint.  For example, if you're running the Cockroach DB server with
the "--insecure" flag, you can use access the endpoint as follows:

	$ curl http://localhost:8080/_status/nodes/1

	(some other output)
	"cr.node.sql.select.count.1": 5,
	(some other output)

Note that a prefix and suffix have been added. The prefix "cr.node." denotes that this metric
is node-level. The suffix ".1" specifies that this metric is for node 1.

To ensure the rules you added work as expected, you can use the GetRulesForTest helper API within
the RuleRegistry to confirm the rules are initialized as expected.
Additionally, you can also verify that the rules are being exported correctly by running Cockroach
DB server locally and viewing the rules' endpoint. To view the endpoint, you can either access it via
the Advanced Debug page of DB Console or as follows:

$ curl http://localhost:8080/api/v2/rules/
*/
package metric
