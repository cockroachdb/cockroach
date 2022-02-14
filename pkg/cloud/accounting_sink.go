// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/multitenant"
)

type accountingSink struct {
	ExternalStorage

	costController multitenant.TenantSideExternalIOInterceptor
}

var _ ExternalStorage = (*accountingSink)(nil)

// Writer returns a writer for the requested name.
func (a *accountingSink) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	innerWriter, err := a.ExternalStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}
	return &accountingWriter{
		inner:          innerWriter,
		ctx:            ctx,
		costController: a.costController,
	}, nil
}

type accountingWriter struct {
	inner          io.WriteCloser
	ctx            context.Context
	costController multitenant.TenantSideExternalIOInterceptor
}

func (aw *accountingWriter) Write(p []byte) (int, error) {
	n, err := aw.inner.Write(p)
	// TODO(ssd): Measuring this here in inaccurate because of, at
	// least,
	//
	// - retries in the underlying ExternalStorage,
	// - request metadata (e.g. headers), and
	// - transport layer compression.
	aw.costController.OnExternalWriteResponse(aw.ctx, int64(n))
	return n, err
}

func (aw *accountingWriter) Close() error {
	return aw.inner.Close()
}
