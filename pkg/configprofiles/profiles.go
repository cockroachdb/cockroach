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
	"virtual+noapp+repl": {
		description: "virtualization enabled but no virtual cluster defined yet, with replication enabled",
		tasks:       enableReplication(virtClusterInitTasks),
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
var virtClusterInitTasks = []autoconfigpb.Task{}

// NOTE: DO NOT MODIFY TASKS HERE. Task execution is identified by the
// task ID; already-run tasks will not re-run. Add tasks at the end of
// each config profile. See enableReplication() for an example.
var virtClusterWithAppServiceInitTasks = append(
	virtClusterInitTasks[:len(virtClusterInitTasks):len(virtClusterInitTasks)],
	makeTask("create an application virtual cluster",
		/* noTxnSQL */ []string{
			// Create the app tenant record.
			"CREATE VIRTUAL CLUSTER IF NOT EXISTS application",
			// Run the service for the application tenant.
			"ALTER VIRTUAL CLUSTER application START SERVICE SHARED",
		},
		nil, /* txnSQL */

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
		MinVersion: clusterversion.Latest.Version(),
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
