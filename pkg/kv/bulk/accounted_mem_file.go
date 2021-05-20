// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// accountedMemFile wraps a MemFile so that can account for the memory it uses
// in the given monitor if one is provided.
type accountedMemFile struct {
	storage.MemFile

	ctx    context.Context
	memAcc *mon.BoundAccount
}

// makeAccountedMemFile creates an accountedMemFile that can keep track of its
// allocations towards the memory monitor provided.
// Memory written to this buffer will be freed upon Close().
func makeAccountedMemFile(ctx context.Context, memMon *mon.BytesMonitor) (f accountedMemFile) {
	f.ctx = ctx
	if memMon != nil {
		memAcc := memMon.MakeBoundAccount()
		f.memAcc = &memAcc
	}

	return
}

// Write implements the writeCloseSyncer interface. It accounts for memory it
// uses.
func (f *accountedMemFile) Write(p []byte) (n int, err error) {
	oldCap := f.MemFile.Cap()
	n, err = f.MemFile.Write(p)
	if err != nil {
		return n, err
	}

	// Although this accounts for the memory after allocating it, the SST batcher
	// predicts if it expects the capacity to increase and will flush eagerly if it
	// does.
	if f.memAcc != nil {
		newCap := f.MemFile.Cap()
		amountToGrow := int64(newCap - oldCap)
		// We do not expect for this allocation to error. If it does that means that
		// the prediction of the caller was not aggressive enough (ie it was less than
		// amountToGrow).
		if err := f.memAcc.Grow(f.ctx, amountToGrow); err != nil {
			log.Errorf(f.ctx, "unexpectedly could not grow accounted mem file; grew by %d", amountToGrow)
			return n, err
		}
	}

	return n, nil
}

// Close implements the writeCloseSyncer interface.
func (f *accountedMemFile) Close() error {
	if f.memAcc != nil {
		f.memAcc.Close(f.ctx)
	}

	return f.MemFile.Close()
}
