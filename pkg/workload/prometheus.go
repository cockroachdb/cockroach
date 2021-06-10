// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// PrometheusRegistry is the registry containing all prometheus metadata
	// for workload scoped m,etrics.
	PrometheusRegistry = prometheus.NewRegistry()
	// PrometheusMetrics returns the factory in which metrics can be made
	// for workload scoped metrics.
	PrometheusMetrics = promauto.With(PrometheusRegistry)
)

// PrometheusNamespace returns the namespace for workload prometheus metrics.
const PrometheusNamespace = "workload"
