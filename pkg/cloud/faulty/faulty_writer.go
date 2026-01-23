// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package faulty

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/errors"
)

type faultyWriter struct {
	ctx           context.Context
	wrappedWriter io.WriteCloser
	opFaults      fault.Strategy
	ioFaults      fault.Strategy
	basename      string
}

func (f *faultyWriter) maybeInjectErr(opName string, strategy fault.Strategy) error {
	if !strategy.ShouldInject(f.ctx, opName) {
		return nil
	}
	return errors.Newf("injected error for op '%s' on object '%s'", opName, f.basename)
}

func (f *faultyWriter) Close() error {
	err := f.wrappedWriter.Close()
	if err != nil {
		return err
	}
	return f.maybeInjectErr("faultyWriter.Close", f.opFaults)
}

func (f *faultyWriter) Write(p []byte) (n int, err error) {
	if err := f.maybeInjectErr("faultyWriter.Write", f.ioFaults); err != nil {
		return 0, err
	}
	return f.wrappedWriter.Write(p)
}
