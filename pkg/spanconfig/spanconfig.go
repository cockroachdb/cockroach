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
// reconciliation process. The reconciliation process reconciles a tenant's span
// configs with the cluster's.
type ReconciliationDependencies interface {
	// KVAccessor
	// SQLWatcher
}
