// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfiglimiter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
)

var _ spanconfig.Limiter = &NoopLimiter{}

// NoopLimiter is a Limiter that simply no-ops (i.e. doesn't limit anything).
type NoopLimiter struct{}

// ShouldLimit is part of the spanconfig.Limiter interface.
func (n NoopLimiter) ShouldLimit(context.Context, *kv.Txn, int) (bool, error) {
	return false, nil
}
