// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package test

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"

// Monitor is an interface for monitoring cockroach processes during a test.
type Monitor interface {
	ExpectProcessDeath(nodes option.NodeListOption, opts ...option.OptionFunc)
	ExpectProcessHealthy(nodes option.NodeListOption, opts ...option.OptionFunc)
}
