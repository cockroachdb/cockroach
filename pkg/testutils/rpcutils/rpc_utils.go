// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpcutils

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
)

// allGoodHealthChecker is an implementation of nodedialer.HealthChecker that
// considers any node healthy.
type AllGoodHealthChecker struct{}

var _ nodedialer.HealthChecker = AllGoodHealthChecker{}

// ConnHealth implements the nodedialer.HealthChecker interface.
func (a AllGoodHealthChecker) ConnHealth(_ roachpb.NodeID) error {
	return nil
}
