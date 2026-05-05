// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsutil

import (
	"encoding/gob"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// ChildMetricClass identifies the class an entry in allowedChildMetrics is
// expected to be.
type ChildMetricClass int

const (
	// Counter expects a Prometheus counter at runtime.
	Counter ChildMetricClass = iota + 1
	// Gauge expects a Prometheus gauge at runtime.
	Gauge
	// Histogram expects an aggmetric.AggHistogram at runtime.
	Histogram
)

// allowedChildMetrics is the list of metrics that should have child metrics
// collected and recorded to TSDB, mapped to the type each metric is expected
// to be. This is a curated list to prevent unbounded cardinality while still
// capturing the most important per-changefeed metrics.
//
// Use LookupChildMetricClass to read entries.
var allowedChildMetrics = map[string]ChildMetricClass{
	"changefeed.aggregator_progress":                  Gauge,
	"changefeed.backfill_pending_ranges":              Gauge,
	"changefeed.checkpoint_progress":                  Gauge,
	"changefeed.emitted_batch_sizes":                  Histogram,
	"changefeed.emitted_messages":                     Counter,
	"changefeed.error_retries":                        Counter,
	"changefeed.internal_retry_message_count":         Gauge,
	"changefeed.lagging_ranges":                       Gauge,
	"changefeed.max_behind_nanos":                     Gauge,
	"changefeed.sink_backpressure_nanos":              Histogram,
	"changefeed.sink_io_inflight":                     Gauge,
	"changefeed.stage.downstream_client_send.latency": Histogram,
	"changefeed.total_ranges":                         Gauge,
}

// LookupChildMetricClass returns the declared ChildMetricClass for an
// child metric in allowedChildMetrics if it is present.
func LookupChildMetricClass(name string) (ChildMetricClass, bool) {
	class, ok := allowedChildMetrics[name]
	return class, ok
}

// IsAllowedChildMetric checks if a metric name matches one of the allowed child metrics.
func IsAllowedChildMetric(name string) bool {
	for _, prefix := range []string{"cr.node.", "cr.store."} {
		if strings.HasPrefix(name, prefix) {
			name = strings.TrimPrefix(name, prefix)
			break
		}
	}
	_, ok := allowedChildMetrics[name]
	return ok
}

// DumpRawTo is a helper that gob-encodes all messages received from the
// source stream to the given WriteCloser.
func DumpRawTo(src tspb.RPCTimeSeries_DumpRawClient, out io.Writer) error {
	enc := gob.NewEncoder(out)
	for {
		data, err := src.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := enc.Encode(data); err != nil {
			return err
		}
	}
}

// MakeTenantSource creates a source given a NodeID and a TenantID.
func MakeTenantSource(nodeID string, tenantID string) string {
	if tenantID != "" {
		return fmt.Sprintf("%s-%s", nodeID, tenantID)
	}
	return nodeID
}

// MakeTenantSourcePrefix adds the tenant source suffix to a given source.
func MakeTenantSourcePrefix(source string) string {
	if source != "" {
		return fmt.Sprintf("%s-", source)
	}
	return source
}

// DecodeSource splits a source into its individual components.
//
// primarySource can refer to NodeID or StoreID depending on the metric stored.
// tenantSource refers to the TenantID of the secondary tenant (empty string for
// system tenant for backwards compatibility).
func DecodeSource(source string) (primarySource string, tenantSource string) {
	splitSources := strings.Split(source, "-")
	primarySource = splitSources[0]
	if len(splitSources) > 1 {
		tenantSource = splitSources[1]
	}
	return primarySource, tenantSource
}
