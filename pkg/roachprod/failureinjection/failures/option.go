// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import "github.com/cockroachdb/cockroach/pkg/roachprod/install"

type ClusterOptionFunc func(*ClusterOptions)

func Secure(secure bool) ClusterOptionFunc {
	return func(o *ClusterOptions) {
		o.secure = secure
	}
}

func LocalCertsPath(localCertsPath string) ClusterOptionFunc {
	return func(o *ClusterOptions) {
		o.localCertsPath = localCertsPath
	}
}

func ReplicationFactor(replicationFactor int) ClusterOptionFunc {
	return func(o *ClusterOptions) {
		o.replicationFactor = replicationFactor
	}
}

func DefaultVirtualClusterName(virtualClusterName string) ClusterOptionFunc {
	return func(o *ClusterOptions) {
		o.defaultVirtualCluster.name = virtualClusterName
	}
}
func DefaultSQLInstance(instance int) ClusterOptionFunc {
	return func(o *ClusterOptions) {
		o.defaultVirtualCluster.instance = instance
	}
}

type virtualClusterOpt struct {
	name     string
	instance int
}

type virtualClusterOptFunc func(*virtualClusterOpt)

func withSystemCluster() virtualClusterOptFunc {
	return func(o *virtualClusterOpt) {
		o.name = install.SystemInterfaceName
		o.instance = 0
	}
}
