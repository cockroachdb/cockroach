// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package model

import "golang.org/x/perf/benchmath"

type BuilderOptions struct {
	// Thresholds stores the statistical thresholds used during comparison of
	// samples.
	thresholds *benchmath.Thresholds
}

type BuilderOption func(*BuilderOptions)

func newBuilderOptions() *BuilderOptions {
	return &BuilderOptions{
		thresholds: &benchmath.DefaultThresholds,
	}
}
func WithThresholds(t *benchmath.Thresholds) BuilderOption {
	return func(o *BuilderOptions) {
		o.thresholds = t
	}
}
