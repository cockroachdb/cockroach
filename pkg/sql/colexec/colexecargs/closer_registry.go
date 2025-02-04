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

// CloserRegistry keeps track of all colexecop.Closers created during the
// operator planning. It is concurrency-safe.
type CloserRegistry struct {
	mu      syncutil.Mutex
	toClose colexecop.Closers
}

func (r *CloserRegistry) NumClosers() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.toClose)
}

func (r *CloserRegistry) AddCloser(closer colexecop.Closer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.toClose = append(r.toClose, closer)
}

func (r *CloserRegistry) AddClosers(closers colexecop.Closers) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.toClose = append(r.toClose, closers...)
}

func (r *CloserRegistry) Close(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.toClose {
		r.toClose[i] = nil
	}
	r.toClose = r.toClose[:0]
}

// BenchmarkReset should only be called from benchmarks in order to prepare the
// registry for the new iteration. This should be used whenever a single
// registry is utilized for the whole benchmark loop.
func (r *CloserRegistry) BenchmarkReset(ctx context.Context) {
	r.Close(ctx)
	r.Reset()
}
