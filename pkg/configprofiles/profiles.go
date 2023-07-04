// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package configprofiles

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
)

// alias represents a name that points to a pre-defined profile with
// an alternate description.
type alias struct {
	aliasTarget string
	description string
}

var aliases = map[string]alias{
	"replication-source": {
		aliasTarget: "multitenant+app+sharedservice+repl",
		description: "configuration suitable for a replication source cluster",
	},
	"replication-target": {
		aliasTarget: "multitenant+noapp+repl",
		description: "configuration suitable for a replication target cluster",
	},
}

const defaultProfileName = "default"

type configProfile struct {
	description string
	tasks       []autoconfigpb.Task
}

var staticProfiles = map[string]configProfile{
	defaultProfileName: {
		// Do not define tasks in the "default" profile. It should continue
		// to generate clusters with default configurations.
		description: "no extra configuration applied - using source code defaults",
	},
	// The "example" profile exists for demonstration and documentation
	// purposes.
	"example": {
		description: "creates an 'example' database and data table, for illustration purposes",
		tasks: []autoconfigpb.Task{
			makeTask("create an example database",
				/* nonTxnSQL */ nil,
				/* txnSQL */ []string{
					"CREATE DATABASE IF NOT EXISTS example",
					"CREATE TABLE IF NOT EXISTS example.data AS SELECT 'hello' AS value",
				},
			),
		},
	},
	"multitenant+noapp": {
		description: "multi-tenant cluster with no secondary tenant defined yet",
		tasks:       multitenantClusterInitTasks,
	},
	"multitenant+noapp+repl": {
		description: "multi-tenant cluster with no secondary tenant defined yet, with replication enabled",
		tasks:       enableReplication(multitenantClusterInitTasks),
	},
	"multitenant+app+sharedservice": {
		description: "multi-tenant cluster with one secondary tenant configured to serve SQL application traffic",
		tasks:       multitenantClusterWithAppServiceInitTasks,
	},
	"multitenant+app+sharedservice+repl": {
		description: "multi-tenant cluster with one secondary tenant configured to serve SQL application traffic, with replication enabled",
		tasks:       enableReplication(multitenantClusterWithAppServiceInitTasks),
	},
}

var multitenantClusterInitTasks = []autoconfigpb.Task{
	makeTask("initial cluster config",
		/* nonTxnSQL */ []string{
			// Disable trace redaction (this ought to be configurable per-tenant, but is not possible yet in v23.1).
			"SET CLUSTER SETTING server.secondary_tenants.redact_trace.enabled = false",
			// Enable zone config changes in secondary tenants  (this ought to be configurable per-tenant, but is not possible yet in v23.1).
			"SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true",
			// Enable multi-region abstractions in secondary tenants.
			"SET CLUSTER SETTING sql.multi_region.allow_abstractions_for_secondary_tenants.enabled = true",
			// Disable range coalescing (as long as the problems related
			// to range coalescing have not been solved yet).
			"SET CLUSTER SETTING spanconfig.storage_coalesce_adjacent.enabled = false",
			"SET CLUSTER SETTING spanconfig.tenant_coalesce_adjacent.enabled = false",
			// Make the operator double-check virtual cluster deletions.
			"SET CLUSTER SETTING sql.drop_virtual_cluster.enabled = false",
		},
		nil, /* txnSQL */
	),
	makeTask("create tenant template",
		nil, /* nonTxnSQL */
		/* txnSQL */
		[]string{
			// Create a main secondary tenant template.
			"CREATE VIRTUAL CLUSTER template",
			"ALTER VIRTUAL CLUSTER template GRANT ALL CAPABILITIES",
			// Enable admin scatter/split in tenant SQL.
			// TODO(knz): Move this to in-tenant config task.
			"ALTER VIRTUAL CLUSTER template SET CLUSTER SETTING sql.scatter.allow_for_secondary_tenant.enabled = true",
			"ALTER VIRTUAL CLUSTER template SET CLUSTER SETTING sql.split_at.allow_for_secondary_tenant.enabled = true",
		},
	),
	// Finally.
	makeTask("use the application tenant template by default in CREATE VIRTUAL CLUSTER",
		/* nonTxnSQL */ []string{
			"SET CLUSTER SETTING sql.create_tenant.default_template = 'template'",
		},
		nil, /* txnSQL */
	),
}

var multitenantClusterWithAppServiceInitTasks = append(
	multitenantClusterInitTasks,
	makeTask("create an application tenant",
		nil, /* nonTxnSQL */
		/* txnSQL */ []string{
			// Create the app tenant record.
			"CREATE VIRTUAL CLUSTER application",
			// Run the service for the application tenant.
			"ALTER VIRTUAL CLUSTER application START SERVICE SHARED",
		},
	),
	makeTask("activate application tenant",
		/* nonTxnSQL */ []string{
			// Make the app tenant receive SQL connections by default.
			"SET CLUSTER SETTING server.controller.default_tenant = 'application'",
		},
		nil, /* txnSQL */
	),
)

func enableReplication(baseTasks []autoconfigpb.Task) []autoconfigpb.Task {
	return append(baseTasks,
		makeTask("enable rangefeeds and replication",
			/* nonTxnSQL */ []string{
				"SET CLUSTER SETTING kv.rangefeed.enabled = true",
				"SET CLUSTER SETTING cross_cluster_replication.enabled = true",
			},
			nil, /* txnSQL */
		),
	)
}

func makeTask(description string, nonTxnSQL, txnSQL []string) autoconfigpb.Task {
	return autoconfigpb.Task{
		Description: description,
		// We set MinVersion to BinaryVersionKey to ensure the tasks only
		// start executing after all other version migrations have been
		// completed.
		MinVersion: clusterversion.ByKey(clusterversion.BinaryVersionKey),
		Payload: &autoconfigpb.Task_SimpleSQL{
			SimpleSQL: &autoconfigpb.SimpleSQL{
				NonTransactionalStatements: nonTxnSQL,
				TransactionalStatements:    txnSQL,
			},
		},
	}
}

func init() {
	// Give all tasks monotonically increasing IDs.
	for _, p := range staticProfiles {
		taskID := autoconfigpb.TaskID(1)
		for i := range p.tasks {
			p.tasks[i].TaskID = taskID
			taskID++
		}
	}
}

func TestingGetProfiles() map[string][]autoconfigpb.Task {
	v := make(map[string][]autoconfigpb.Task, len(staticProfiles))
	for n, p := range staticProfiles {
		v[n] = p.tasks
	}
	return v
}
