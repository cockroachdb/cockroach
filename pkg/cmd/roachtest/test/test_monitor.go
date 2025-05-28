// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package test

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"

// Monitor is an interface for monitoring cockroach processes during a test.
type Monitor interface {
	ExpectProcessDead(nodes option.NodeListOption, opts ...option.OptionFunc)
	ExpectProcessAlive(nodes option.NodeListOption, opts ...option.OptionFunc)
	AvailableNodes(virtualClusterName string) option.NodeListOption
}
