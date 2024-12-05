// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operation

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type Operation interface {
	// ClusterCockroach returns the path to the Cockroach binary on the target
	// cluster.
	ClusterCockroach() string
	ClusterSettings() install.ClusterSettings
	StartOpts() option.StartOpts

	Name() string
	Error(args ...interface{})
	Errorf(string, ...interface{})
	FailNow()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool

	L() *logger.Logger
	Status(args ...interface{})
	WorkloadCluster() cluster.Cluster
}
