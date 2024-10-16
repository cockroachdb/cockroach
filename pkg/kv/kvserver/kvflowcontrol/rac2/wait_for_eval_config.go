// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
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
func (waitCat WaitForEvalCategory) Bypass(wc admissionpb.WorkClass) bool {
	return waitCat == NoWorkWaitsForEval ||
		(waitCat == OnlyElasticWorkWaitsForEval && wc == admissionpb.RegularWorkClass)
}

// WaitForEvalConfig provides the latest configuration related to WaitForEval.
type WaitForEvalConfig struct {
	st *cluster.Settings
	mu struct {
		syncutil.RWMutex
		waitCategory            WaitForEvalCategory
		waitCategoryDecreasedCh chan struct{}
	}
}

// NewWaitForEvalConfig constructs WaitForEvalConfig.
func NewWaitForEvalConfig(st *cluster.Settings) *WaitForEvalConfig {
	w := &WaitForEvalConfig{st: st}
	w.mu.waitCategory = w.computeCategory()
	w.mu.waitCategoryDecreasedCh = make(chan struct{})
	return w
}

// NotifyChanged must be called whenever any of the cluster settings that
// affect WaitForEval change.
func (w *WaitForEvalConfig) NotifyChanged() {
	waitCategory := w.computeCategory()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.waitCategory > waitCategory {
		close(w.mu.waitCategoryDecreasedCh)
		w.mu.waitCategoryDecreasedCh = make(chan struct{})
	}
	w.mu.waitCategory = waitCategory
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
