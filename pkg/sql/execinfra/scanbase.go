// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Prettier aliases for execinfrapb.ScanVisibility values.
const (
	ScanVisibilityPublic             = execinfrapb.ScanVisibility_PUBLIC
	ScanVisibilityPublicAndNotPublic = execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
)

// ParallelizeScansIfLocal determines whether we can plan multiple table readers
// for the same stage of a local plan.
// TODO(yuzefovich): do we want to remove the setting and have it enabled all
// the time?
var ParallelizeScansIfLocal = settings.RegisterBoolSetting(
	`sql.distsql.parallelize_scans_if_local.enabled`,
	"if true and the physical plan is local, the scans will be performed in parallel, "+
		"split according to the leaseholders for the relevant spans",
	true,
)

// HasParallelProcessors returns whether flow contains multiple processors in
// the same stage.
func HasParallelProcessors(flow *execinfrapb.FlowSpec) bool {
	var seen util.FastIntSet
	for _, p := range flow.Processors {
		if seen.Contains(int(p.StageID)) {
			return true
		}
		seen.Add(int(p.StageID))
	}
	return false
}
