// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// EveryN provides a way to rate limit noisy log messages. It tracks how
// recently a given log message has been emitted so that it can determine
// whether it's worth logging again.
type EveryN struct {
	util.EveryN
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN {
	return EveryN{EveryN: util.Every(n)}
}

// ShouldLog returns whether it's been more than N time since the last event.
func (e *EveryN) ShouldLog() bool {
	return e.ShouldProcess(timeutil.Now())
}

func GetWorkloadHistogramArgsString(
	t test.Test, c cluster.Cluster, labels map[string]string,
) string {
	var histogramArgs string
	if t.ExportOpenmetrics() {
		histogramArgs += " --histogram-export-format='openmetrics' " +
			"--histograms=" + t.PerfArtifactsDir() + "/stats.om " +
			fmt.Sprintf("--openmetrics-labels='%s'", clusterstats.GetDefaultOpenmetricsLabelString(t, c, labels))
	} else {
		histogramArgs = " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
	}

	return histogramArgs
}

func CreaterWorkloadHistogramExporter(t test.Test, c cluster.Cluster) exporter.Exporter {
	var metricsExporter exporter.Exporter
	if t.ExportOpenmetrics() {
		labels := clusterstats.GetDefaultOpenmetricsLabelMap(t, c, nil)
		openMetricsExporter := &exporter.OpenMetricsExporter{}
		openMetricsExporter.SetLabels(&labels)
		metricsExporter = openMetricsExporter

	} else {
		metricsExporter = &exporter.HdrJsonExporter{}
	}

	return metricsExporter
}

func GetLabelMapFromStruct(opts interface{}) map[string]string {
	result := make(map[string]string)
	val := reflect.ValueOf(opts)
	typ := reflect.TypeOf(opts)

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldName := typ.Field(i).Name
		var fieldValue string

		switch field.Kind() {
		case reflect.String:
			fieldValue = field.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldValue = strconv.FormatInt(field.Int(), 10)
		case reflect.Float32, reflect.Float64:
			fieldValue = strconv.FormatFloat(field.Float(), 'f', -1, 64)
		case reflect.Bool:
			fieldValue = strconv.FormatBool(field.Bool())
		default:
		}

		result[fieldName] = fieldValue
	}

	return result
}
