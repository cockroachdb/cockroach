// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func (f *Failer) GetReadWriteBytes(
	ctx context.Context, l *logger.Logger, node install.Nodes,
) (int, int, error) {
	failure, ok := f.sharedFailer.failureMode().(*CGroupDiskStaller)
	if !ok {
		return 0, 0, errors.Newf("GetReadWriteBytes is only supported for %s", CgroupsDiskStallName)
	}
	return failure.GetReadWriteBytes(ctx, l, node)
}
