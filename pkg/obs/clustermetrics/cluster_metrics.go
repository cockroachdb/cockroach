// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type cmMetadata struct {
	mu struct {
		syncutil.RWMutex
		registeredMetadata       map[string]metric.Metadata
		registeredLabeledMetrics map[string][]string
	}
}

func newCmMetadata() *cmMetadata {
	m := &cmMetadata{}
	m.mu.registeredMetadata = make(map[string]metric.Metadata)
	m.mu.registeredLabeledMetrics = make(map[string][]string)
	return m
}

var metadata = newCmMetadata()

// RegisterClusterMetric registers metadata for a scalar cluster metric.
// This should be called in an init() function so that metadata is available
// before the cmreader.updater starts reading from the rangefeed.
func RegisterClusterMetric(name string, md metric.Metadata) {
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	metadata.mu.registeredMetadata[name] = md
}

// RegisterLabeledClusterMetric registers metadata for a labeled (vector)
// cluster metric. This should be called in an init() function so that
// metadata is available before the cmreader.updater starts reading from
// the rangefeed.
func RegisterLabeledClusterMetric(name string, md metric.Metadata, labels []string) {
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	metadata.mu.registeredMetadata[name] = md
	metadata.mu.registeredLabeledMetrics[name] = labels
}

// GetClusterMetricMetadata retrieves the metadata for a cluster metric by
// name. For labeled metrics the returned label slice is non-nil; for
// unlabeled metrics it is nil.
func GetClusterMetricMetadata(name string) (metric.Metadata, []string, bool) {
	metadata.mu.RLock()
	defer metadata.mu.RUnlock()
	meta, ok := metadata.mu.registeredMetadata[name]
	if !ok {
		return metric.Metadata{}, nil, false
	}
	labels := metadata.mu.registeredLabeledMetrics[name]
	return meta, labels, true
}

func testingRemoveMetadata(name string) {
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	delete(metadata.mu.registeredMetadata, name)
	delete(metadata.mu.registeredLabeledMetrics, name)
}

// TestingRegisterClusterMetric adds a metric to the registry for testing.
// It returns a cleanup function that removes the entry.
func TestingRegisterClusterMetric(name string, md metric.Metadata) func() {
	RegisterClusterMetric(name, md)
	return func() {
		testingRemoveMetadata(name)
	}
}

// TestingRegisterLabeledClusterMetric adds a labeled metric for testing.
// It returns a cleanup function that removes the entry.
func TestingRegisterLabeledClusterMetric(name string, md metric.Metadata, labels []string) func() {
	RegisterLabeledClusterMetric(name, md, labels)
	return func() {
		testingRemoveMetadata(name)
	}
}
