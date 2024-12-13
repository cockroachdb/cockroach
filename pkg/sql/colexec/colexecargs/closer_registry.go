// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecargs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type CloserRegistry struct {
	toClose colexecop.Closers
}

func (r *CloserRegistry) NumClosers() int {
	return len(r.toClose)
}

func (r *CloserRegistry) AddCloser(closer colexecop.Closer) {
	r.toClose = append(r.toClose, closer)
}

func (r *CloserRegistry) AddClosers(closers colexecop.Closers) {
	r.toClose = append(r.toClose, closers...)
}

func (r *CloserRegistry) Close(ctx context.Context) {
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		for _, closer := range r.toClose {
			if err := closer.Close(ctx); err != nil && log.V(1) {
				log.Infof(ctx, "error closing Closer: %v", err)
			}
		}
	}); err != nil && log.V(1) {
		log.Infof(ctx, "runtime error closing the closers: %v", err)
	}
}

func (r *CloserRegistry) Reset() {
	for i := range r.toClose {
		r.toClose[i] = nil
	}
	r.toClose = r.toClose[:0]
}
