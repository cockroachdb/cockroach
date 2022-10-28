// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import "github.com/cockroachdb/cockroach/pkg/clusterversion"

func init() {
	// TODO(irfansharif): Remove this in 23.1.
	clusterversion.Refer(clusterversion.V22_2, isAtLeast22dot2ClusterVersion)
}
