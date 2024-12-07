// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// WaitForEvalCategory specifies what category of work is subject to waiting
// in WaitForEval.
type WaitForEvalCategory uint8

const (
	NoWorkWaitsForEval WaitForEvalCategory = iota
	OnlyElasticWorkWaitsForEval
	AllWorkWaitsForEval
)

// Bypass returns true iff the given WorkClass does not need to wait for
// tokens in WaitForEval.
func (wec WaitForEvalCategory) Bypass(wc admissionpb.WorkClass) bool {
	return wec == NoWorkWaitsForEval ||
		(wec == OnlyElasticWorkWaitsForEval && wc == admissionpb.RegularWorkClass)
}

// WaitForEvalConfig provides the latest configuration related to WaitForEval.
type WaitForEvalConfig struct {
	st *cluster.Settings
	mu struct {
		syncutil.RWMutex
		waitCategory            WaitForEvalCategory
		waitCategoryDecreasedCh chan struct{}
	}
	// watcherMu is ordered before mu. cbs are executed while holding watcherMu.
	watcherMu struct {
		syncutil.Mutex
		cbs []WatcherCallback
	}
}

type WatcherCallback func(wc WaitForEvalCategory)

// NewWaitForEvalConfig constructs WaitForEvalConfig.
func NewWaitForEvalConfig(st *cluster.Settings) *WaitForEvalConfig {
	w := &WaitForEvalConfig{st: st}
	kvflowcontrol.Mode.SetOnChange(&st.SV, func(ctx context.Context) {
		w.notifyChanged()
	})
	kvflowcontrol.Enabled.SetOnChange(&st.SV, func(ctx context.Context) {
		w.notifyChanged()
	})
	// Ensure initialization.
	w.notifyChanged()
	return w
}

// notifyChanged is called whenever any of the cluster settings that affect
// WaitForEval change. It is also called for initialization.
func (w *WaitForEvalConfig) notifyChanged() {
	func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		// Call computeCategory while holding w.mu to serialize the computation in
		// case of concurrent callbacks. This ensures the latest settings are used
		// to set the current state, and we don't have a situation where a slow
		// goroutine samples the settings, then after some arbitrary duration
		// acquires the mutex and sets a stale state.
		waitCategory := w.computeCategory()
		if w.mu.waitCategoryDecreasedCh == nil {
			// Initialization.
			w.mu.waitCategoryDecreasedCh = make(chan struct{})
			w.mu.waitCategory = waitCategory
			return
		}
		// Not initialization.
		if w.mu.waitCategory > waitCategory {
			close(w.mu.waitCategoryDecreasedCh)
			w.mu.waitCategoryDecreasedCh = make(chan struct{})
		}
		// Else w.mu.waitCategory <= waitCategory. Since the set of requests that
		// are subject to replication admission/flow control is growing (or staying
		// the same), we don't need to tell the existing waiting requests to restart
		// their wait, using the latest value of waitCategory, since they are
		// unaffected by the change.

		w.mu.waitCategory = waitCategory
	}()
	w.watcherMu.Lock()
	defer w.watcherMu.Unlock()
	w.mu.RLock()
	wc := w.mu.waitCategory
	w.mu.RUnlock()
	for _, cb := range w.watcherMu.cbs {
		cb(wc)
	}
}

// Current returns the current category, and a channel that will be closed if
// the category value decreases (which narrows the set of work that needs to
// WaitForEval).
func (w *WaitForEvalConfig) Current() (WaitForEvalCategory, <-chan struct{}) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mu.waitCategory, w.mu.waitCategoryDecreasedCh
}

func (w *WaitForEvalConfig) computeCategory() WaitForEvalCategory {
	enabled := kvflowcontrol.Enabled.Get(&w.st.SV)
	if !enabled {
		return NoWorkWaitsForEval
	}
	mode := kvflowcontrol.Mode.Get(&w.st.SV)
	switch mode {
	case kvflowcontrol.ApplyToElastic:
		return OnlyElasticWorkWaitsForEval
	case kvflowcontrol.ApplyToAll:
		return AllWorkWaitsForEval
	}
	panic(errors.AssertionFailedf("unknown mode %v", mode))
}

// RegisterWatcher registers a callback that provides the latest state of
// WaitForEvalCategory. The first call happens within this method, before
// returning.
func (w *WaitForEvalConfig) RegisterWatcher(cb WatcherCallback) {
	w.watcherMu.Lock()
	defer w.watcherMu.Unlock()
	w.mu.RLock()
	wc := w.mu.waitCategory
	w.mu.RUnlock()
	cb(wc)
	w.watcherMu.cbs = append(w.watcherMu.cbs, cb)
}
