// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"io"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const windowFramerTmpl = "pkg/sql/colexec/colexecwindow/window_framer_tmpl.go"

var windowFrameModes = []tree.WindowFrameMode{tree.ROWS, tree.GROUPS, tree.RANGE}
var windowFrameStartBoundTypes = []tree.WindowFrameBoundType{
	tree.UnboundedPreceding, tree.OffsetPreceding, tree.CurrentRow, tree.OffsetFollowing,
}
var windowFrameEndBoundTypes = []tree.WindowFrameBoundType{
	tree.OffsetPreceding, tree.CurrentRow, tree.OffsetFollowing, tree.UnboundedFollowing,
}

func windowFramerGenerator(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_OP_STRING", "{{.OpString}}",
		"_FRAME_MODE", "{{modeToExecinfrapb .ModeType}}",
		"_START_BOUND", "{{boundToExecinfrapb .BoundType}}",
		"_END_BOUND", "{{boundToExecinfrapb .BoundType}}",
	)
	s := r.Replace(inputFileContents)

	tmpl, err := template.New("window_framer").Funcs(
		template.FuncMap{
			"buildDict":          buildDict,
			"modeToExecinfrapb":  modeToExecinfrapb,
			"boundToExecinfrapb": boundToExecinfrapb,
		}).Parse(s)
	if err != nil {
		return err
	}

	var windowFrameTmplInfos []windowFramerModeInfo
	for _, modeType := range windowFrameModes {
		modeInfo := windowFramerModeInfo{
			ModeType: modeType,
		}
		for _, startBoundType := range windowFrameStartBoundTypes {
			startBoundInfo := windowFramerStartBoundInfo{
				BoundType: startBoundType,
			}
			for _, endBoundType := range windowFrameEndBoundTypes {
				if endBoundType < startBoundType {
					// This pair of bounds would entail a syntax error.
					continue
				}
				opString := "windowFramer" + modeType.Name() + startBoundType.Name() + endBoundType.Name()
				endBoundInfo := windowFramerEndBoundInfo{
					windowFramerTmplInfo: windowFramerTmplInfo{
						OpString:       opString,
						ModeType:       modeType,
						StartBoundType: startBoundType,
						EndBoundType:   endBoundType,
					},
					BoundType: endBoundType,
				}
				startBoundInfo.EndBoundTypes = append(startBoundInfo.EndBoundTypes, endBoundInfo)
			}
			modeInfo.StartBoundTypes = append(modeInfo.StartBoundTypes, startBoundInfo)
		}
		windowFrameTmplInfos = append(windowFrameTmplInfos, modeInfo)
	}

	return tmpl.Execute(wr, windowFrameTmplInfos)
}

func init() {
	registerGenerator(windowFramerGenerator, "window_framer.eg.go", windowFramerTmpl)
}

type windowFramerModeInfo struct {
	ModeType        tree.WindowFrameMode
	StartBoundTypes []windowFramerStartBoundInfo
}

type windowFramerStartBoundInfo struct {
	BoundType     tree.WindowFrameBoundType
	EndBoundTypes []windowFramerEndBoundInfo
}

type windowFramerEndBoundInfo struct {
	windowFramerTmplInfo
	BoundType tree.WindowFrameBoundType
}

type windowFramerTmplInfo struct {
	OpString       string
	ModeType       tree.WindowFrameMode
	StartBoundType tree.WindowFrameBoundType
	EndBoundType   tree.WindowFrameBoundType
}

func (overload windowFramerTmplInfo) GroupsMode() bool {
	return overload.ModeType == tree.GROUPS
}

func (overload windowFramerTmplInfo) RowsMode() bool {
	return overload.ModeType == tree.ROWS
}

func (overload windowFramerTmplInfo) RangeMode() bool {
	return overload.ModeType == tree.RANGE
}

func (overload windowFramerTmplInfo) StartUnboundedPreceding() bool {
	return overload.StartBoundType == tree.UnboundedPreceding
}

func (overload windowFramerTmplInfo) StartOffsetPreceding() bool {
	return overload.StartBoundType == tree.OffsetPreceding
}

func (overload windowFramerTmplInfo) StartCurrentRow() bool {
	return overload.StartBoundType == tree.CurrentRow
}

func (overload windowFramerTmplInfo) StartOffsetFollowing() bool {
	return overload.StartBoundType == tree.OffsetFollowing
}

func (overload windowFramerTmplInfo) EndOffsetPreceding() bool {
	return overload.EndBoundType == tree.OffsetPreceding
}

func (overload windowFramerTmplInfo) EndCurrentRow() bool {
	return overload.EndBoundType == tree.CurrentRow
}

func (overload windowFramerTmplInfo) EndOffsetFollowing() bool {
	return overload.EndBoundType == tree.OffsetFollowing
}

func (overload windowFramerTmplInfo) EndUnboundedFollowing() bool {
	return overload.EndBoundType == tree.UnboundedFollowing
}

func (overload windowFramerTmplInfo) OffsetPreceding() bool {
	return overload.StartBoundType == tree.OffsetPreceding ||
		overload.EndBoundType == tree.OffsetPreceding
}

func (overload windowFramerTmplInfo) StartHasOffset() bool {
	return overload.StartBoundType == tree.OffsetPreceding ||
		overload.StartBoundType == tree.OffsetFollowing
}

func (overload windowFramerTmplInfo) EndHasOffset() bool {
	return overload.EndBoundType == tree.OffsetPreceding ||
		overload.EndBoundType == tree.OffsetFollowing
}

func (overload windowFramerTmplInfo) HasOffset() bool {
	return overload.StartHasOffset() || overload.EndHasOffset()
}

func (overload windowFramerTmplInfo) BothUnbounded() bool {
	return overload.StartBoundType == tree.UnboundedPreceding &&
		overload.EndBoundType == tree.UnboundedFollowing
}

func modeToExecinfrapb(mode tree.WindowFrameMode) string {
	switch mode {
	case tree.RANGE:
		return "execinfrapb.WindowerSpec_Frame_RANGE"
	case tree.ROWS:
		return "execinfrapb.WindowerSpec_Frame_ROWS"
	case tree.GROUPS:
		return "execinfrapb.WindowerSpec_Frame_GROUPS"
	}
	return ""
}

func boundToExecinfrapb(bound tree.WindowFrameBoundType) string {
	switch bound {
	case tree.UnboundedPreceding:
		return "execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING"
	case tree.OffsetPreceding:
		return "execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING"
	case tree.CurrentRow:
		return "execinfrapb.WindowerSpec_Frame_CURRENT_ROW"
	case tree.OffsetFollowing:
		return "execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING"
	case tree.UnboundedFollowing:
		return "execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING"
	}
	return ""
}
