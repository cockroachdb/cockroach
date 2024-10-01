// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (d *deferredVisitor) RefreshStats(_ context.Context, op scop.RefreshStats) error {
	d.DeferredMutationStateUpdater.RefreshStats(op.TableID)
	return nil
}
