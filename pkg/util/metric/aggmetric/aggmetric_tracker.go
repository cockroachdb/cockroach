// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	databaseNameLabel    = "db"
	applicationNameLabel = "app"
)

var AppNameLabelEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.application_name_metrics.enabled",
	"when enabled, SQL metrics would export application name as and additional label."+
		"The number of unique label combinations is limited to 5000 by default.",
	false, /* default */
	settings.WithPublic)

var DBNameLabelEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.database_name_metrics.enabled",
	"when enabled, SQL metrics would export database name as and additional label."+
		"The number of unique label combinations is limited to 5000 by default.",
	false, /* default */
	settings.WithPublic)

// MetricTracker tracks all childSet with StorageTypeCache for registered metrics. This is used to
// register and reinitialise all tracked childSet when the settings are changed.
// When AppNameLabelEnabled and DBNameLabelEnabled are updated in the settings,
// the metric tracker will clear children and update childSet labels so that the new labels are
// used during prometheus export.
type MetricTracker struct {
	mu                  syncutil.Mutex
	metrics             map[string]*childSet
	DBNameLabelEnabled  bool
	AppNameLabelEnabled bool
}

// GetMetricTracker initializes the metric tracker. Method should be invoked once
// to track metrics. It also updates SetOnChange for AppNameLabelEnabled and
// DBNameLabelEnabled flags.
func GetMetricTracker() *MetricTracker {
	return &MetricTracker{
		metrics: make(map[string]*childSet),
	}
}

// RegisterWithMetric registers the childSet of StorageTypeCache with the metric tracker so that it
// can be tracked and updated when the settings are changed.
func (metricTracker *MetricTracker) RegisterWithMetric(name string, cs *childSet) {
	metricTracker.mu.Lock()
	defer metricTracker.mu.Unlock()
	metricTracker.metrics[name] = cs
}

// ReinitialiseMetrics re-initializes all tracked child metrics. Reinitialisation is done
// based on the new settings values for AppNameLabelEnabled and DBNameLabelEnabled flags.
// The method will update labels for all childSet in the metric tracker amd clear
// existing children. It doesn't update parent metric values for the given childSet.
func (metricTracker *MetricTracker) ReinitialiseMetrics(sv *settings.Values) {
	metricTracker.mu.Lock()
	defer metricTracker.mu.Unlock()
	metricTracker.DBNameLabelEnabled = DBNameLabelEnabled.Get(sv)
	metricTracker.AppNameLabelEnabled = AppNameLabelEnabled.Get(sv)

	for _, cs := range metricTracker.metrics {
		var labelSet []string

		if metricTracker.DBNameLabelEnabled {
			labelSet = append(labelSet, databaseNameLabel)
		}
		if metricTracker.AppNameLabelEnabled {
			labelSet = append(labelSet, applicationNameLabel)
		}

		prevLabels := cs.mu.labels
		for _, label := range prevLabels {
			if label != databaseNameLabel && label != applicationNameLabel {
				labelSet = append(labelSet, label)
			}
		}

		cs.reinitialise(labelSet...)
	}
}

// GetMetricLabelValues returns the label values for the metric. Method will include
// db and app labels if the corresponding settings are enabled.
func (metricTracker *MetricTracker) GetMetricLabelValues(
	dbName string, appName string, labelValues ...string,
) []string {
	var labels []string
	if metricTracker.DBNameLabelEnabled {
		labels = append(labels, dbName)
	}
	if metricTracker.AppNameLabelEnabled {
		labels = append(labels, appName)
	}
	labels = append(labels, labelValues...)
	return labels
}
