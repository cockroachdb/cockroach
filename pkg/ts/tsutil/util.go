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

// AllowedChildMetrics is the list of metrics that should have child metrics
// collected and recorded to TSDB. This is a curated list to prevent unbounded
// cardinality while still capturing the most important per-changefeed metrics.
var AllowedChildMetrics = map[string]struct{}{
	"changefeed.max_behind_nanos":                     {},
	"changefeed.error_retries":                        {},
	"changefeed.internal_retry_message_count":         {},
	"changefeed.stage.downstream_client_send.latency": {},
	"changefeed.emitted_messages":                     {},
	"changefeed.sink_backpressure_nanos":              {},
	"changefeed.backfill_pending_ranges":              {},
	"changefeed.sink_io_inflight":                     {},
	"changefeed.lagging_ranges":                       {},
	"changefeed.aggregator_progress":                  {},
	"changefeed.checkpoint_progress":                  {},
	"changefeed.emitted_batch_sizes":                  {},
	"changefeed.total_ranges":                         {},
}

// IsAllowedChildMetric checks if a metric name matches one of the allowed child metrics.
func IsAllowedChildMetric(name string) bool {
	metricName := name
	for _, prefix := range []string{"cr.node.", "cr.store."} {
		if strings.HasPrefix(metricName, prefix) {
			metricName = strings.TrimPrefix(metricName, prefix)
			break
		}
	}
	_, ok := AllowedChildMetrics[metricName]
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
