// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

// BackgroundPipe is a helper for providing a Writer that is backed by a pipe
// that has a background process reading from it. It *must* be Closed().
func BackgroundPipe(
	ctx context.Context, fn func(ctx context.Context, pr io.Reader) error,
) io.WriteCloser {
	pr, pw := io.Pipe()
	w := &backgroundPipe{w: pw, grp: ctxgroup.WithContext(ctx), ctx: ctx}
	w.grp.GoCtx(func(ctc context.Context) error {
		err := fn(ctx, pr)
		if err != nil {
			closeErr := pr.CloseWithError(err)
			err = errors.CombineErrors(err, closeErr)
		} else {
			err = pr.Close()
		}
		return err
	})
	return w
}

type backgroundPipe struct {
	w   *io.PipeWriter
	grp ctxgroup.Group
	ctx context.Context
}

// Write writes to the writer.
func (s *backgroundPipe) Write(p []byte) (int, error) {
	return s.w.Write(p)
}

// Close closes the writer, finishing the write operation.
func (s *backgroundPipe) Close() error {
	err := s.w.CloseWithError(s.ctx.Err())
	return errors.CombineErrors(err, s.grp.Wait())
}
