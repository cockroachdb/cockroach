// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"

// Registry is the interface again which tests are registered with the roachtest
// test runner.
type Registry interface {
	MakeClusterSpec(nodeCount int, opts ...spec.Option) spec.ClusterSpec
	Add(TestSpec)
}
