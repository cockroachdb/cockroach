// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"sync"
)

// History is an opaque view of what the executor observed.
// For now, expose only a no-op that can be extended later.
type History interface{}

type history struct {
	mu struct {
		sync.Mutex
		records []record
	}
}

type record struct {
	template string
	attempt  int
	err      error
}

func NewHistory() *history { return &history{} }

func (h *history) Add(template string, attempt int, err error) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	// Keep a bounded buffer of recent records (e.g., last 1024 entries).
	const max = 1024
	if len(h.mu.records) >= max {
		copy(h.mu.records, h.mu.records[1:])
		h.mu.records[max-1] = record{template: template, attempt: attempt, err: err}
		return
	}
	h.mu.records = append(h.mu.records, record{template: template, attempt: attempt, err: err})
}
