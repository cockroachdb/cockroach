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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type CloserRegistry struct {
	optionalMu *syncutil.Mutex
	toClose    colexecop.Closers
}

// SetMutex makes the CloserRegistry concurrency-safe via the provided mutex.
// It is a no-op if the mutex has already been set on the registry.
//
// Note that Close and Reset are not protected.
func (r *CloserRegistry) SetMutex(mu *syncutil.Mutex) {
	if r.optionalMu == nil {
		r.optionalMu = mu
	}
}

func (r *CloserRegistry) NumClosers() int {
	if r.optionalMu != nil {
		r.optionalMu.Lock()
		defer r.optionalMu.Unlock()
	}
	return len(r.toClose)
}

func (r *CloserRegistry) AddCloser(closer colexecop.Closer) {
	if r.optionalMu != nil {
		r.optionalMu.Lock()
		defer r.optionalMu.Unlock()
	}
	r.toClose = append(r.toClose, closer)
}

func (r *CloserRegistry) AddClosers(closers colexecop.Closers) {
	if r.optionalMu != nil {
		r.optionalMu.Lock()
		defer r.optionalMu.Unlock()
	}
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
	r.optionalMu = nil
	for i := range r.toClose {
		r.toClose[i] = nil
	}
	r.toClose = r.toClose[:0]
}
