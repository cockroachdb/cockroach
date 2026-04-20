// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	prometheusgo "github.com/prometheus/client_model/go"
)

const (
	// instanceTypeLabel is the Prometheus label name used to distinguish
	// node-level, store-level, and cluster-level metrics.
	instanceTypeLabel = "instance_type"

	// nodeIDLabel is the Prometheus label for the CRDB node ID.
	nodeIDLabel = "node_id"

	// storeIDLabel is the Prometheus label for the CRDB store ID.
	storeIDLabel = "store_id"

	// TSDB metric name prefixes.
	nodePrefix    = "cr.node."
	storePrefix   = "cr.store."
	clusterPrefix = "cr.cluster."
)

// MetricCatalog maintains a bidirectional mapping between Prometheus-style
// metric names and CockroachDB TSDB fully-qualified names. The mapping is
// built from the output of MetricsRecorder.GetRecordedMetricNames().
//
// TSDB names like "cr.node.sql.conn.count" are converted to Prometheus-style
// names by stripping the prefix and replacing dots with underscores:
// "sql_conn_count". The instance type ("node", "store", "cluster") is exposed
// as a separate Prometheus label rather than being encoded in the metric name.
type MetricCatalog struct {
	mu struct {
		syncutil.RWMutex
		// promToTSDB maps Prometheus-style names to TSDB fully-qualified names.
		promToTSDB map[string]string
		// tsdbToProm maps TSDB fully-qualified names to Prometheus-style names.
		tsdbToProm map[string]string
		// allPromNames is a sorted list of all Prometheus-style names.
		allPromNames []string
		// metadataByProm maps Prometheus-style names to metric metadata.
		metadataByProm map[string]metric.Metadata
	}
}

// MetricInfo holds the metadata for a metric as returned by the
// /api/v1/metadata endpoint.
type MetricInfo struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

// NewMetricCatalog creates a MetricCatalog and populates it from:
//   - tsdbNames: maps short metric names to fully-qualified TSDB names, as
//     returned by MetricsRecorder.GetRecordedMetricNames().
//   - allMetadata: maps short metric names to their Metadata, as returned
//     by MetricsRecorder.GetMetricsMetadata().
func NewMetricCatalog(
	tsdbNames map[string]string, allMetadata map[string]metric.Metadata,
) *MetricCatalog {
	c := &MetricCatalog{}
	c.Refresh(tsdbNames, allMetadata)
	return c
}

// Refresh replaces the catalog contents.
func (c *MetricCatalog) Refresh(
	tsdbNames map[string]string, allMetadata map[string]metric.Metadata,
) {
	promToTSDB := make(map[string]string, len(tsdbNames))
	tsdbToProm := make(map[string]string, len(tsdbNames))
	allPromNames := make([]string, 0, len(tsdbNames))
	metadataByProm := make(map[string]metric.Metadata, len(tsdbNames))

	for shortName, tsdbName := range tsdbNames {
		promName := tsdbToPromName(tsdbName)
		promToTSDB[promName] = tsdbName
		tsdbToProm[tsdbName] = promName
		allPromNames = append(allPromNames, promName)
		// Look up metadata using the base metric name (strip histogram
		// suffixes like -p50, -p99, -count, etc.).
		baseName := shortName
		for _, suffix := range histogramSuffixes {
			if strings.HasSuffix(baseName, suffix) {
				baseName = baseName[:len(baseName)-len(suffix)]
				break
			}
		}
		if md, ok := allMetadata[baseName]; ok {
			metadataByProm[promName] = md
		} else if md, ok := allMetadata[shortName]; ok {
			metadataByProm[promName] = md
		}
	}
	sort.Strings(allPromNames)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.promToTSDB = promToTSDB
	c.mu.tsdbToProm = tsdbToProm
	c.mu.allPromNames = allPromNames
	c.mu.metadataByProm = metadataByProm
}

// histogramSuffixes are the suffixes added to histogram metric names when
// they are stored in TSDB as individual computed values.
var histogramSuffixes = []string{"-p50", "-p75", "-p90", "-p99", "-p999", "-p9999", "-max", "-count", "-sum", "-avg"}

// TSDBName returns the fully-qualified TSDB name for a Prometheus-style name.
func (c *MetricCatalog) TSDBName(promName string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name, ok := c.mu.promToTSDB[promName]
	return name, ok
}

// PromName returns the Prometheus-style name for a fully-qualified TSDB name.
func (c *MetricCatalog) PromName(tsdbName string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name, ok := c.mu.tsdbToProm[tsdbName]
	return name, ok
}

// AllPromNames returns a sorted slice of all Prometheus-style metric names.
func (c *MetricCatalog) AllPromNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, len(c.mu.allPromNames))
	copy(result, c.mu.allPromNames)
	return result
}

// MatchPromNames returns all Prometheus-style names that match the given
// function. This is used to support regex matchers on __name__.
func (c *MetricCatalog) MatchPromNames(matchFn func(string) bool) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result []string
	for _, name := range c.mu.allPromNames {
		if matchFn(name) {
			result = append(result, name)
		}
	}
	return result
}

// AllMetadata returns metadata for all metrics, keyed by Prometheus name.
func (c *MetricCatalog) AllMetadata() map[string][]MetricInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string][]MetricInfo, len(c.mu.allPromNames))
	for _, promName := range c.mu.allPromNames {
		info := MetricInfo{Type: "gauge", Help: promName}
		if md, ok := c.mu.metadataByProm[promName]; ok {
			info.Help = strings.TrimSpace(md.Help)
			info.Type = promMetricType(md.MetricType)
			info.Unit = md.Unit.String()
		}
		result[promName] = []MetricInfo{info}
	}
	return result
}

// promMetricType converts a Prometheus client_model MetricType to the
// lowercase string used in the Prometheus HTTP API.
func promMetricType(t prometheusgo.MetricType) string {
	switch t {
	case prometheusgo.MetricType_COUNTER:
		return "counter"
	case prometheusgo.MetricType_GAUGE:
		return "gauge"
	case prometheusgo.MetricType_HISTOGRAM:
		return "histogram"
	case prometheusgo.MetricType_SUMMARY:
		return "summary"
	default:
		return "gauge"
	}
}

// InstanceType returns "node", "store", or "cluster" based on the
// fully-qualified TSDB name's prefix.
func InstanceType(tsdbName string) string {
	switch {
	case strings.HasPrefix(tsdbName, storePrefix):
		return "store"
	case strings.HasPrefix(tsdbName, clusterPrefix):
		return "cluster"
	default:
		return "node"
	}
}

// SourceLabel returns the Prometheus label name appropriate for the given
// instance type. Node metrics use "node_id", store metrics use "store_id".
func SourceLabel(instanceType string) string {
	if instanceType == "store" {
		return storeIDLabel
	}
	return nodeIDLabel
}

// tsdbToPromName converts a TSDB fully-qualified name to a Prometheus-style
// name by stripping the prefix and replacing dots with underscores.
//
// Examples:
//
//	"cr.node.sql.conn.count" -> "sql_conn_count"
//	"cr.store.livebytes"     -> "livebytes"
//	"cr.cluster.rebalancing" -> "rebalancing"
func tsdbToPromName(tsdbName string) string {
	short := tsdbName
	for _, prefix := range []string{nodePrefix, storePrefix, clusterPrefix} {
		if strings.HasPrefix(tsdbName, prefix) {
			short = tsdbName[len(prefix):]
			break
		}
	}
	return strings.ReplaceAll(short, ".", "_")
}
