// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiespb

const (
	// CanAdminSplit if set to true, grants the tenant the ability to
	// successfully perform `AdminSplit` requests.
	CanAdminSplit = "can_admin_split"
	// CanViewNodeInfo if set to true, grants the tenant the ability
	// retrieve node-level observability data at endpoints such as `_status/nodes`
	// and in the DB Console overview page.
	CanViewNodeInfo = "can_view_node_info"
	// CanViewTSDBMetrics if set to true, grants the tenant the ability to
	// make arbitrary queries of the TSDB of the entire cluster. Currently,
	// we do not store per-tenant metrics so this will surface system metrics
	// to the tenant.
	// TODO(davidh): Revise this once tenant-scoped metrics are implemented in
	// https://github.com/cockroachdb/cockroach/issues/96438
	CanViewTSDBMetrics = "can_view_tsdb_metrics"
)
