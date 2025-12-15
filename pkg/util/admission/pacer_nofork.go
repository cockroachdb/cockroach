// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// In unsupported non-bazel builds, a stock/off-the-shelf Go SDK may be used
// instead of the bazel-managed SDK pulled from Cockroach's fork of Go. In these
// builds, we just stub out the Pacer, which normally interacts with our fork's
// runtime extensions. We copy the whole pacer, instead of having a Yeilder type
// with two implementations, to as the call frequency to Pace makes it sensitive
// to even tiny changes in overhad eg if such a wrapper weren't inlined.
//go:build !bazel

package admission

import (
	"context"
)

// Pacer is a stub for use in non-bazel builds.
type Pacer struct {
	Yield bool
}

// Pace is a noop.
func (p *Pacer) Pace(ctx context.Context) (readmitted bool, err error) {
	return false, nil
}

// Close is part of the Pacer interface.
func (p *Pacer) Close() {
}
