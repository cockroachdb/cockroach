// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package metric provides server metrics (a.k.a. transient stats) for a
CockroachDB server. These metrics are persisted to the time-series database and
are viewable through the web interface and the /_status/metrics/<NODEID> HTTP
endpoint.

Adding a new metric

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

Sub-registries

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

Testing

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
*/
package metric
