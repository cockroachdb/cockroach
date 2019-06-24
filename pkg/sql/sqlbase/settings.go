// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/settings"

// ParallelScans controls parallelizing multi-range scans when the maximum size
// of the result set is known.
var ParallelScans = settings.RegisterBoolSetting(
	"sql.parallel_scans.enabled",
	"parallelizes scanning different ranges when the maximum result size can be deduced",
	true,
)
