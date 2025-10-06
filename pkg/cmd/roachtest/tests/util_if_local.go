// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"

func ifLocal(c cluster.Cluster, trueVal, falseVal string) string {
	if c.IsLocal() {
		return trueVal
	}
	return falseVal
}
