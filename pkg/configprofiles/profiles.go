// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	hidden      bool
}

var aliases = map[string]alias{
	"replication-source": {
		aliasTarget: "virtual+app+sharedservice+repl",
		description: "configuration suitable for a replication source cluster",
	},
	"replication-target": {
		aliasTarget: "virtual+noapp+repl",
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
	"virtual+noapp": {
		description: "virtualization enabled but no virtual cluster defined yet",
		tasks:       virtClusterInitTasks,
	},
	"virtual+noapp+repl": {
		description: "virtualization enabled but no virtual cluster defined yet, with replication enabled",
		tasks:       enableReplication(virtClusterInitTasks),
	},
	"virtual+app+sharedservice": {
		description: "one virtual cluster configured to serve SQL application traffic",
		tasks:       virtClusterWithAppServiceInitTasks,
	},
	"virtual+app+sharedservice+repl": {
		description: "one virtual cluster configured to serve SQL application traffic, with replication enabled",
		tasks:       enableReplication(virtClusterWithAppServiceInitTasks),
	},
}

// virtClusterInitTasks is the list of tasks that are run when
// virtualization is enabled but no virtual cluster has been created yet.
//
// NOTE: DO NOT MODIFY TASKS HERE. Task execution is identified by the
// task ID; already-run tasks will not re-run. Add tasks at the end of
// each config profile. See enableReplication() for an example.
var virtClusterInitTasks = []autoconfigpb.Task{
	makeTask("initial cluster config",
		/* nonTxnSQL */ []string{
			// Disable trace redaction (this ought to be configurable per-tenant, but is not possible yet in v23.1).
			"SET CLUSTER SETTING trace.redact_at_virtual_cluster_boundary.enabled = false",
			// Enable zone config changes in secondary tenants  (this ought to be configurable per-tenant, but is not possible yet in v23.1).
			"SET CLUSTER SETTING sql.virtual_cluster.feature_access.zone_configs.enabled = true",
			"SET CLUSTER SETTING sql.virtual_cluster.feature_access.zone_configs_unrestricted.enabled = true",
			// Enable multi-region abstractions in secondary tenants.
			"SET CLUSTER SETTING sql.virtual_cluster.feature_access.multiregion.enabled = true",
			// Disable range coalescing (as long as the problems related
			// to range coalescing have not been solved yet).
			"SET CLUSTER SETTING spanconfig.range_coalescing.system.enabled = false",
			"SET CLUSTER SETTING spanconfig.range_coalescing.application.enabled = false",
			// Make the operator double-check virtual cluster deletions.
			"SET CLUSTER SETTING sql.drop_virtual_cluster.enabled = false",
		},
		nil, /* txnSQL */
	),
	makeTask("create virtual cluster template",
		nil, /* nonTxnSQL */
		/* txnSQL */
		[]string{
			// Create a main secondary tenant template.
			"CREATE VIRTUAL CLUSTER template",
			"ALTER VIRTUAL CLUSTER template GRANT ALL CAPABILITIES",
			// Enable admin scatter/split in tenant SQL.
			// TODO(knz): Move this to in-tenant config task.
			"ALTER VIRTUAL CLUSTER template SET CLUSTER SETTING sql.virtual_cluster.feature_access.manual_range_scatter.enabled = true",
			"ALTER VIRTUAL CLUSTER template SET CLUSTER SETTING sql.virtual_cluster.feature_access.manual_range_split.enabled = true",
		},
	),
	// Finally.
	makeTask("use the application virtual cluster template by default in CREATE VIRTUAL CLUSTER",
		/* nonTxnSQL */ []string{
			"SET CLUSTER SETTING sql.create_virtual_cluster.default_template = 'template'",
		},
		nil, /* txnSQL */
	),
}

// NOTE: DO NOT MODIFY TASKS HERE. Task execution is identified by the
// task ID; already-run tasks will not re-run. Add tasks at the end of
// each config profile. See enableReplication() for an example.
var virtClusterWithAppServiceInitTasks = append(
	virtClusterInitTasks[:len(virtClusterInitTasks):len(virtClusterInitTasks)],
	makeTask("create an application virtual cluster",
		nil, /* nonTxnSQL */
		/* txnSQL */ []string{
			// Create the app tenant record.
			"CREATE VIRTUAL CLUSTER application",
			// Run the service for the application tenant.
			"ALTER VIRTUAL CLUSTER application START SERVICE SHARED",
		},
	),
	makeTask("activate application virtual cluster",
		/* nonTxnSQL */ []string{
			// Make the app tenant receive SQL connections by default.
			"SET CLUSTER SETTING server.controller.default_target_cluster = 'application'",
		},
		nil, /* txnSQL */
	),
)

func enableReplication(baseTasks []autoconfigpb.Task) []autoconfigpb.Task {
	return append(baseTasks[:len(baseTasks):len(baseTasks)],
		makeTask("enable rangefeeds and replication",
			/* nonTxnSQL */ []string{
				"SET CLUSTER SETTING kv.rangefeed.enabled = true",
				"SET CLUSTER SETTING physical_replication.enabled = true",
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
