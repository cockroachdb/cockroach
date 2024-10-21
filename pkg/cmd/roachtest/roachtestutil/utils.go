// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
)

// SystemInterfaceSystemdUnitName is a convenience function that
// returns the systemd unit name for the system interface
func SystemInterfaceSystemdUnitName() string {
	return install.VirtualClusterLabel(install.SystemInterfaceName, 0)
}

// SetDefaultSQLPort sets the SQL port to the default of 26257 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultSQLPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.SQLPort = config.DefaultSQLPort
	}
}

// SetDefaultAdminUIPort sets the AdminUI port to the default of 26258 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultAdminUIPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.AdminUIPort = config.DefaultAdminUIPort
	}
}

func GetWorkloadHistogramArgsString(t test.Test, c cluster.Cluster) string {
	var histogramArgs string
	if t.ExportOpenmetrics() {
		histogramArgs += " --histogram-export-format='openmetrics' " +
			"--histograms=" + t.PerfArtifactsDir() + "/stats.om " +
			fmt.Sprintf("--openmetrics-labels='%s'", clusterstats.GetDefaultOpenmetricsLabelString(t, c))
	} else {
		histogramArgs = " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
	}

	return histogramArgs
}

func CreaterWorkloadHistogramExporter(t test.Test, c cluster.Cluster) exporter.Exporter {
	var metricsExporter exporter.Exporter
	if t.ExportOpenmetrics() {
		labels := clusterstats.GetDefaultOpenmetricsLabelMap(t, c)
		openMetricsExporter := &exporter.OpenMetricsExporter{}
		openMetricsExporter.SetLabels(&labels)
		metricsExporter = openMetricsExporter

	} else {
		metricsExporter = &exporter.HdrJsonExporter{}
	}

	return metricsExporter
}
