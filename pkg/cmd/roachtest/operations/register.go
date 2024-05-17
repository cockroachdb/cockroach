// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package operations

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"

// RegisterOperations registers all operations to the Registry. This powers `roachtest run-operations`.
func RegisterOperations(r registry.Registry) {
	registerAddColumn(r)
	registerAddIndex(r)
	registerNetworkPartition(r)
	registerDiskStall(r)
	registerNodeKill(r)
	registerClusterSettings(r)
}
