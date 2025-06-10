// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	settings.SystemVisible,
	"diagnostics.active_query_dumps.enabled",
	"experimental: enable dumping of anonymized active queries to disk when node is under memory pressure",
	true,
	settings.WithPublic)

// GoogleCloudProfilerEnabled controls whether Google Cloud Profiler is enabled.
// When enabled, the profiler will send profiling data to Google Cloud Profiler
// service. This should typically only be enabled on GCP clusters.
var GoogleCloudProfilerEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"diagnostics.google_cloud_profiler.enabled",
	"enable Google Cloud Profiler to send profiling data to GCP (typically only useful on GCP clusters)",
	false, /* disabled by default, enabled dynamically based on cloud provider */
	settings.WithPublic)
