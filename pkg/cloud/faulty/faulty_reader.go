// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package faulty

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

type faultyReader struct {
	wrappedReader ioctx.ReadCloserCtx
	ioFaults      fault.Strategy
	opFaults      fault.Strategy
	basename      string
}

func (f *faultyReader) maybeInjectErr(
	ctx context.Context, opName string, strategy fault.Strategy,
) error {
	if !strategy.ShouldInject(ctx, opName) {
		return nil
	}
	return errors.Newf("injected error for op '%s' on object '%s'", opName, f.basename)
}

// Close implements ioctx.ReadCloserCtx.
func (f *faultyReader) Close(ctx context.Context) error {
	err := f.wrappedReader.Close(ctx)
	if err != nil {
		return err
	}
	return f.maybeInjectErr(ctx, "faultyReader.Close", f.opFaults)
}

// Read implements ioctx.ReadCloserCtx.
func (f *faultyReader) Read(ctx context.Context, p []byte) (n int, err error) {
	if err := f.maybeInjectErr(ctx, "faultyReader.Read", f.ioFaults); err != nil {
		return 0, err
	}
	return f.wrappedReader.Read(ctx, p)
}

var _ ioctx.ReadCloserCtx = &faultyReader{}
