// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// If a stock/off-the-shelf Go SDK is used instead of the bazel-managed SDK
// pulled from Cockroach's fork of Go, the Pacer, which normally interacts with
// our fork's runtime extensions, cannot function normally. To allow the package
// to at least compile, we have a stub copy of pacer. Note: we fork the whole
// Pacer instead of having a narrower Yielder with separate implementations to
// avoid any overhead of calling a wrapper as Pace() is extremely sensitive to
// per-call cost.
//go:build !bazel

package admission

import (
	"context"
	"time"
)

// Pacer is a stub for use in non-bazel builds.
type Pacer struct {
	unit time.Duration
	wi   WorkInfo
	wq   *ElasticCPUWorkQueue

	Yield bool
}

// Pace is a noop.
func (p *Pacer) Pace(ctx context.Context) (readmitted bool, err error) {
	return false, nil
}

// Close is part of the Pacer interface.
func (p *Pacer) Close() {
}
