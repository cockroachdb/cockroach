// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cmmetrics holds the cluster metric types and metadata
// registry. Both cmwriter (registration at construction time) and
// cmwatcher (lookup when materializing rangefeed rows) import this
// package, avoiding a circular dependency through the parent
// clustermetrics package.
package cmmetrics

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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

// RegisterClusterMetric registers metadata for a scalar cluster metric
// so that it is available before the cmreader starts reading from the
// rangefeed.
func RegisterClusterMetric(name string, md metric.Metadata) {
	metadata.mu.Lock()
	defer metadata.mu.Unlock()
	metadata.mu.registeredMetadata[name] = md
}

// RegisterLabeledClusterMetric registers metadata for a labeled (vector)
// cluster metric so that it is available before the cmreader starts
// reading from the rangefeed.
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

// ensureClusterMetricRegistered is called by scalar metric constructors
// (NewCounter, NewGauge, NewWriteStopwatch). In production builds or when
// skipInitCheck is set, it unconditionally registers the metadata. In
// test builds it checks whether the constructor is being called from
// an init() function: if so, it registers normally; if not, it
// verifies the metadata was already registered and panics otherwise.
func ensureClusterMetricRegistered(name string, md metric.Metadata) {
	if !buildutil.CrdbTestBuild || skipInitCheck.Load() || calledDuringInit() {
		RegisterClusterMetric(name, md)
		return
	}
	if _, _, ok := GetClusterMetricMetadata(name); !ok {
		panic(fmt.Sprintf(
			"cluster metric %q constructed outside init() without pre-registered metadata",
			name,
		))
	}
}

// ensureLabeledClusterMetricRegistered is the labeled-metric variant
// of ensureClusterMetricRegistered. It is called by vector metric
// constructors (NewGaugeVec, NewCounterVec).
func ensureLabeledClusterMetricRegistered(name string, md metric.Metadata, labels []string) {
	if !buildutil.CrdbTestBuild || skipInitCheck.Load() || calledDuringInit() {
		RegisterLabeledClusterMetric(name, md, labels)
		return
	}
	if _, _, ok := GetClusterMetricMetadata(name); !ok {
		panic(fmt.Sprintf(
			"cluster metric %q constructed outside init() without pre-registered metadata",
			name,
		))
	}
}

// calledDuringInit walks the call stack to determine whether the
// current goroutine is executing inside an init() function or a
// package-level variable initializer. The skip count of 4 accounts
// for: runtime.Callers, calledDuringInit, ensureClusterMetricRegistered,
// and the NewXxx constructor.
func calledDuringInit() bool {
	var pcs [10]uintptr
	n := runtime.Callers(4, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".init") ||
			strings.Contains(frame.Function, ".init.") {
			return true
		}
		if !more {
			break
		}
	}
	return false
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
