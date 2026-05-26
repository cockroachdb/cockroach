// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

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
