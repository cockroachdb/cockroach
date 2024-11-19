// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
