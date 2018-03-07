// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package limit

import (
	"context"

	"github.com/marusama/semaphore"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// ConcurrentRequestLimiter wraps a simple semaphore, adding a tracing span when
// a request is forced to wait.
type ConcurrentRequestLimiter struct {
	spanName string
	sem      semaphore.Semaphore
}

// MakeConcurrentRequestLimiter creates a ConcurrentRequestLimiter.
func MakeConcurrentRequestLimiter(spanName string, limit int) ConcurrentRequestLimiter {
	return ConcurrentRequestLimiter{spanName: spanName, sem: semaphore.New(limit)}
}

// Begin attempts to reserve a spot in the pool, blocking if needed until the
// one is available or the context is canceled and adding a tracing span if it
// is forced to block.
func (l *ConcurrentRequestLimiter) Begin(ctx context.Context) error {
	if l.sem.TryAcquire(1) {
		return nil
	}
	// If not, start a span and begin waiting.
	ctx, span := tracing.ChildSpan(ctx, l.spanName)
	defer tracing.FinishSpan(span)
	return l.sem.Acquire(ctx, 1)
}

// Finish indicates a concurrent request has completed and its reservation can
// be returned to the pool.
func (l *ConcurrentRequestLimiter) Finish() {
	l.sem.Release(1)
}

// SetLimit adjusts the size of the pool.
func (l *ConcurrentRequestLimiter) SetLimit(newLimit int) {
	l.sem.SetLimit(newLimit)
}
