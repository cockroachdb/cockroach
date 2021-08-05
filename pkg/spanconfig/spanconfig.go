// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// CheckAndStartReconciliationJobInterval is a cluster setting to control how
// often the existence of the automatic span config reconciliation job will be
// checked. If the check concludes that the job doesn't exist it will be started.
var CheckAndStartReconciliationJobInterval = settings.RegisterDurationSetting(
	"spanconfig.reconciliation_job.start_interval",
	"how often to check for the span config reconciliation job exists and start it if it doesn't",
	10*time.Minute,
	settings.NonNegativeDuration,
)

// ReconciliationDependencies captures what's needed by the span config
// reconciliation job to perform its task. The job is responsible for
// reconciling a tenant's zone configurations with the clusters span
// configurations.
type ReconciliationDependencies interface {
	// TODO(zcfgs-pod): Placeholder comment until subsequent PRs add useful
	// interfaces here.
	// The job will want access to two interfaces to reconcile.
	// 1. spanconfig.KVAccessor -- this will expose RPCs the job can use to fetch
	// span configs from KV and update them. It'll be implemented by Node for the
	// host tenant and the Connector for secondary tenants.
	// 2. spanconfig.SQLWatcher -- this will maintain a rangefeed over
	// system.{descriptors, zones} and be responsible for generating span config
	// updates. The job will respond to these updates by issuing RPCs using the
	// KVAccessor
}
