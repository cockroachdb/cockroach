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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
)

var staticProfileNames = func() (res []string) {
	for name := range staticProfiles {
		res = append(res, name)
	}
	sort.Strings(res)
	return res
}()

const defaultProfileName = "default"

var staticProfiles = map[string][]autoconfigpb.Task{
	defaultProfileName: {
		// Do not define tasks in the "default" profile. It should continue
		// to generate clusters with default configurations.
	},
	// The "example" profile exists for demonstration and documentation
	// purposes.
	"example": {
		makeTask("create an example database",
			/* nonTxnSQL */ []string{
				"CREATE DATABASE IF NOT EXISTS example",
				"CREATE TABLE IF NOT EXISTS example.data AS SELECT 'hello' AS value",
			},
			nil, /* txnSQL */
		),
	},
	"multitenant+shared+noapp": multitenantClusterInitTasks,
	"multitenant+shared+app": append(
		multitenantClusterInitTasks,
		makeTask("create an application tenant",
			nil, /* nonTxnSQL */
			/* txnSQL */ []string{
				// Create the app tenant record.
				"CREATE TENANT application",
				// Run the service for the application tenant.
				"ALTER TENANT application START SERVICE SHARED",
			},
		),
		makeTask("activate application tenant",
			/* nonTxnSQL */ []string{
				// Make the app tenant receive SQL connections by default.
				"SET CLUSTER SETTING server.controller.default_tenant = 'application'",
			},
			nil, /* txnSQL */
		),
	),
}

var multitenantClusterInitTasks = []autoconfigpb.Task{
	makeTask("initial cluster config",
		/* nonTxnSQL */ []string{
			// Disable RU limits.
			"SET CLUSTER SETTING kv.tenant_rate_limiter.burst_limit_seconds = 10000000",
			"SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = -1000",
			"SET CLUSTER SETTING kv.tenant_rate_limiter.read_batch_cost = 0",
			"SET CLUSTER SETTING kv.tenant_rate_limiter.read_cost_per_mebibyte = 0",
			"SET CLUSTER SETTING kv.tenant_rate_limiter.write_cost_per_megabyte = 0",
			"SET CLUSTER SETTING kv.tenant_rate_limiter.write_request_cost = 0",
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
		},
		nil, /* txnSQL */
	),
	makeTask("create tenant template",
		nil, /* nonTxnSQL */
		[]string{
			// Create a main secondary tenant template.
			"CREATE TENANT template",
			"ALTER TENANT template GRANT CAPABILITY can_admin_relocate_range, can_admin_unsplit, can_view_node_info, can_view_tsdb_metrics, exempt_from_rate_limiting",
			// Disable span config limits and splitter.
			// TODO(knz): Move this to in-tenant config task.
			"ALTER TENANT template SET CLUSTER SETTING spanconfig.tenant_limit = 1000000",
			// "ALTER TENANT template SET CLUSTER SETTING spanconfig.tenant_split.enabled = false",
			// Disable RU accounting.
			// TODO(knz): Move this to in-tenant config task.
			"SELECT crdb_internal.update_tenant_resource_limits('template', 10000000000, 0, 10000000000, now(), 0)",
			// Enable admin scatter/split in tenant SQL.
			// TODO(knz): Move this to in-tenant config task.
			"ALTER TENANT template SET CLUSTER SETTING sql.scatter.allow_for_secondary_tenant.enabled = true",
			"ALTER TENANT template SET CLUSTER SETTING sql.split_at.allow_for_secondary_tenant.enabled = true",
		},
	),
	// Finally.
	makeTask("use the application tenant template by default in CREATE TENANT",
		/* nonTxnSQL */ []string{
			"SET CLUSTER SETTING sql.create_tenant.default_template = 'template'",
		},
		nil, /* txnSQL */
	),
}

func makeTask(description string, nonTxnSQL, txnSQL []string) autoconfigpb.Task {
	return autoconfigpb.Task{
		Description: description,
		MinVersion:  clusterversion.ByKey(clusterversion.BinaryVersionKey),
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
	for _, tasks := range staticProfiles {
		taskID := autoconfigpb.TaskID(1)
		for i := range tasks {
			tasks[i].TaskID = taskID
			taskID++
		}
	}
}

func TestingGetProfiles() map[string][]autoconfigpb.Task {
	return staticProfiles
}
