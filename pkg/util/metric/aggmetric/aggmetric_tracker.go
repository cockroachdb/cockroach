// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aggmetric

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var AppNameLabelEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.application_name_metrics.enabled",
	"include 'app' label in SQL metrics."+
		" It supports limited number of metrics for which app label is added.",
	false, /* default */
	settings.WithPublic)

var DBNameLabelEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.database_name_metrics.enabled",
	"include 'db' label in SQL metrics."+
		" It supports limited number of metrics for which db label is added.",
	false, /* default */
	settings.WithPublic)

// metricTracker tracks all registered childSet for registered metrics.
var metricTracker = struct {
	sync.Once
	mu      syncutil.Mutex
	metrics map[string]*childSet
}{}

// InitMetricTracker initializes the metric tracker. Method should be invoked once
// to track metrics. It also updates SetOnChange for AppNameLabelEnabled and
// DBNameLabelEnabled flags.
func InitMetricTracker(sv *settings.Values) {
	metricTracker.Do(func() {
		metricTracker.metrics = make(map[string]*childSet)
	})

	AppNameLabelEnabled.SetOnChange(sv, func(_ context.Context) {
		reinitialiseMetrics(sv)
	})

	DBNameLabelEnabled.SetOnChange(sv, func(_ context.Context) {
		reinitialiseMetrics(sv)
	})
}

func registerMetric(name string, cs *childSet) {
	metricTracker.mu.Lock()
	defer metricTracker.mu.Unlock()
	metricTracker.metrics[name] = cs
}

// reinitialiseMetrics re-initializes all tracked child metrics. Reinitialisation is done
// based on the new settings values for AppNameLabelEnabled and DBNameLabelEnabled flags.
func reinitialiseMetrics(sv *settings.Values) {
	metricTracker.mu.Lock()
	defer metricTracker.mu.Unlock()

	for _, cs := range metricTracker.metrics {
		var labelSet []string

		if DBNameLabelEnabled.Get(sv) {
			labelSet = append(labelSet, "db_name")
		}
		if AppNameLabelEnabled.Get(sv) {
			labelSet = append(labelSet, "app_name")
		}

		prevLabels := cs.mu.labels
		for _, label := range prevLabels {
			if label != "db_name" && label != "app_name" {
				labelSet = append(labelSet, label)
			}
		}

		cs.reinitialise(labelSet...)
	}
}

// GetMetricLabelValues returns the label values for the metric. Method will include
// dbName and appName labels if the corresponding flags are enabled.
func GetMetricLabelValues(
	sv *settings.Values, dbName string, appName string, labelValues ...string,
) []string {
	var labels []string
	if DBNameLabelEnabled.Get(sv) {
		labels = append(labels, dbName)
	}
	if AppNameLabelEnabled.Get(sv) {
		labels = append(labels, appName)
	}
	labels = append(labels, labelValues...)
	return labels

}
