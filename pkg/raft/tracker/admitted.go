// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

type flowPri uint8

const (
	flowLowPri flowPri = iota
	flowNormalPri
	flowAboveNormalPri
	flowHighPri
	flowPriCount
)

type AdmittedMarks = [flowPriCount]uint64

type Admitted struct {
	Marks AdmittedMarks
}

func (a *Admitted) OnTermBump() {
	a.Marks = AdmittedMarks{}
}

func (a *Admitted) OnSnapshot(index uint64) {
	for i := range a.Marks {
		a.Marks[i] = max(a.Marks[i], index)
	}
}

func (a *Admitted) Set(marks AdmittedMarks) bool {
	updated := false
	for i, mark := range marks {
		if mark > a.Marks[i] {
			a.Marks[i] = mark
			updated = true
		}
	}
	return updated
}
