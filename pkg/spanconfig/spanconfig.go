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
	// KVAccessor.
}
