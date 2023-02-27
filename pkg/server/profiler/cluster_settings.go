// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package profiler

import "github.com/cockroachdb/cockroach/pkg/settings"

// ActiveQueryDumpsEnabled wraps "diagnostics.active_query_dumps.enabled"
//
// diagnostics.active_query_dumps.enabled enables the periodic writing of
// active queries on a node to disk, in *.csv format, if a node is determined to
// be under memory pressure.
//
// Note: this feature only works for nodes running on unix hosts with cgroups
// enabled.
var ActiveQueryDumpsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"diagnostics.active_query_dumps.enabled",
	"experimental: enable dumping of anonymized active queries to disk when node is under memory pressure",
	true,
).WithPublic()
