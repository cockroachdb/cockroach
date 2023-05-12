// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serversettings

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

var (
	QueryWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.query_wait",
		"the timeout for waiting for active queries to finish during a drain "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()

	DrainWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with a drain "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting. --drain-wait is to specify the duration of the "+
			"whole draining process, while server.shutdown.drain_wait is to set the "+
			"wait time for health probes to notice that the node is not ready.)",
		0*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()

	ConnectionWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.connection_wait",
		"the maximum amount of time a server waits for all SQL connections to "+
			"be closed before proceeding with a drain. "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		0*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()

	JobRegistryWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.jobs_wait",
		"the maximum amount of time a server waits for all currently executing jobs "+
			"to notice drain request and to perform orderly shutdown",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()
)
