// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import prometheusgo "github.com/prometheus/client_model/go"

// VecChildSnapshot is a point-in-time snapshot of a single labeled
// child from a vector metric. It satisfies Iterable so it can be used
// directly in metric registries and reporting pipelines.
type VecChildSnapshot struct {
	Metadata
	Labels map[string]string
	Value  int64
}

var _ Iterable = (*VecChildSnapshot)(nil)

// GetLabels returns the label key-value pairs for this snapshot.
func (s *VecChildSnapshot) GetLabels() map[string]string { return s.Labels }

// GetMetadata returns the snapshot's metadata.
func (s *VecChildSnapshot) GetMetadata() Metadata { return s.Metadata }

// Inspect calls f with the snapshot itself.
func (s *VecChildSnapshot) Inspect(f func(interface{})) { f(s) }

// PromLabelsToMap converts prometheus LabelPair protos to a map.
func PromLabelsToMap(pairs []*prometheusgo.LabelPair) map[string]string {
	m := make(map[string]string, len(pairs))
	for _, lp := range pairs {
		m[lp.GetName()] = lp.GetValue()
	}
	return m
}

// EachChild calls f for each labeled child in the GaugeVec.
func (gv *GaugeVec) EachChild(f func(*VecChildSnapshot)) {
	for _, pm := range gv.ToPrometheusMetrics() {
		f(&VecChildSnapshot{
			Metadata: gv.Metadata,
			Labels:   PromLabelsToMap(pm.Label),
			Value:    int64(pm.GetGauge().GetValue()),
		})
	}
}

// EachChild calls f for each labeled child in the CounterVec.
func (cv *CounterVec) EachChild(f func(*VecChildSnapshot)) {
	for _, pm := range cv.ToPrometheusMetrics() {
		f(&VecChildSnapshot{
			Metadata: cv.Metadata,
			Labels:   PromLabelsToMap(pm.Label),
			Value:    int64(pm.GetCounter().GetValue()),
		})
	}
}
