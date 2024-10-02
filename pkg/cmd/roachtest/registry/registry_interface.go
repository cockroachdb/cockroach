// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registry is the interface again which tests are registered with the roachtest
// test runner.
type Registry interface {
	MakeClusterSpec(nodeCount int, opts ...spec.Option) spec.ClusterSpec
	Add(TestSpec)
	AddOperation(OperationSpec)
	PromFactory() promauto.Factory
}
