// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package repro

import (
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	StepInit = StepID(iota)
	StepGCStart
	StepGCSnapshotTaken
	StepSplitStart
	StepSplitApplied
	StepRebalancedRHS
	StepFirstClearRange
	StepGCSent
	StepConsistencyRHS
)

type Bus struct{}

var globalBloke *Bloke[Bus]

func Init() {
	if !buildutil.CrdbTestBuild || globalBloke != nil {
		panic("can only have one global test-only repro")
	}
	globalBloke = NewBloke[Bus](log.Dev)
	globalBloke.Step(StepInit, "repro initialized")
}

func Step(id StepID, msg string) {
	if globalBloke != nil {
		globalBloke.Step(id, msg)
	}
}

func StepIf(id StepID, rw func(bus *Bus) bool, msg string) {
	if globalBloke != nil {
		globalBloke.StepIf(id, rw, msg)
	}
}

func StepEx(id StepID, rw func(bus *Bus) bool, msg string) {
	if globalBloke != nil {
		globalBloke.StepEx(id, rw, msg)
	}
}

func Done(id StepID) bool {
	return globalBloke != nil && globalBloke.Done(id)
}
