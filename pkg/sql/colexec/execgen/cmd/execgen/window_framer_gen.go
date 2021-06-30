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
		"_EXCLUDES_ROWS", "{{.Exclude}}",
	)
	s := r.Replace(inputFileContents)

	tmpl, err := template.New("window_framer").Funcs(
		template.FuncMap{
			"buildDict":            buildDict,
			"modeToExecinfrapb":    modeToExecinfrapb,
			"boundToExecinfrapb":   boundToExecinfrapb,
			"excludeToExecinfrapb": excludeToExecinfrapb,
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
				endBoundInfo := windowFramerEndBoundInfo{
					BoundType: endBoundType,
				}
				for _, exclude := range []bool{false, true} {
					opString := "windowFramer" + modeType.Name() + startBoundType.Name() + endBoundType.Name()
					if exclude {
						opString += "Exclude"
					}
					excludeInfo := windowFramerExcludeInfo{
						windowFramerTmplInfo: windowFramerTmplInfo{
							OpString:       opString,
							ModeType:       modeType,
							StartBoundType: startBoundType,
							EndBoundType:   endBoundType,
						},
						Exclude: exclude,
					}
					endBoundInfo.ExcludeInfos = append(endBoundInfo.ExcludeInfos, excludeInfo)
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
	BoundType    tree.WindowFrameBoundType
	ExcludeInfos []windowFramerExcludeInfo
}

type windowFramerExcludeInfo struct {
	windowFramerTmplInfo
	Exclude bool
}

type windowFramerTmplInfo struct {
	OpString       string
	ModeType       tree.WindowFrameMode
	StartBoundType tree.WindowFrameBoundType
	EndBoundType   tree.WindowFrameBoundType
	Exclude        bool
}

func (overload windowFramerTmplInfo) GroupsMode() bool {
	return overload.ModeType == tree.GROUPS
}

var _ = windowFramerTmplInfo{}.GroupsMode()

func (overload windowFramerTmplInfo) RowsMode() bool {
	return overload.ModeType == tree.ROWS
}

var _ = windowFramerTmplInfo{}.RowsMode()

func (overload windowFramerTmplInfo) RangeMode() bool {
	return overload.ModeType == tree.RANGE
}

var _ = windowFramerTmplInfo{}.RangeMode()

func (overload windowFramerTmplInfo) StartUnboundedPreceding() bool {
	return overload.StartBoundType == tree.UnboundedPreceding
}

var _ = windowFramerTmplInfo{}.StartUnboundedPreceding()

func (overload windowFramerTmplInfo) StartOffsetPreceding() bool {
	return overload.StartBoundType == tree.OffsetPreceding
}

var _ = windowFramerTmplInfo{}.StartOffsetPreceding()

func (overload windowFramerTmplInfo) StartCurrentRow() bool {
	return overload.StartBoundType == tree.CurrentRow
}

var _ = windowFramerTmplInfo{}.StartCurrentRow()

func (overload windowFramerTmplInfo) StartOffsetFollowing() bool {
	return overload.StartBoundType == tree.OffsetFollowing
}

var _ = windowFramerTmplInfo{}.StartOffsetFollowing()

func (overload windowFramerTmplInfo) EndOffsetPreceding() bool {
	return overload.EndBoundType == tree.OffsetPreceding
}

var _ = windowFramerTmplInfo{}.EndOffsetPreceding()

func (overload windowFramerTmplInfo) EndCurrentRow() bool {
	return overload.EndBoundType == tree.CurrentRow
}

var _ = windowFramerTmplInfo{}.EndCurrentRow()

func (overload windowFramerTmplInfo) EndOffsetFollowing() bool {
	return overload.EndBoundType == tree.OffsetFollowing
}

var _ = windowFramerTmplInfo{}.EndOffsetFollowing()

func (overload windowFramerTmplInfo) EndUnboundedFollowing() bool {
	return overload.EndBoundType == tree.UnboundedFollowing
}

var _ = windowFramerTmplInfo{}.EndUnboundedFollowing()

func (overload windowFramerTmplInfo) OffsetPreceding() bool {
	return overload.StartBoundType == tree.OffsetPreceding ||
		overload.EndBoundType == tree.OffsetPreceding
}

var _ = windowFramerTmplInfo{}.OffsetPreceding()

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

var _ = windowFramerTmplInfo{}.HasOffset()

func (overload windowFramerTmplInfo) BothUnbounded() bool {
	return overload.StartBoundType == tree.UnboundedPreceding &&
		overload.EndBoundType == tree.UnboundedFollowing
}

var _ = windowFramerTmplInfo{}.BothUnbounded()

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

func excludeToExecinfrapb(exclude tree.WindowFrameExclusion) string {
	switch exclude {
	case tree.NoExclusion:
		return "execinfrapb.WindowerSpec_Frame_NO_EXCLUSION"
	case tree.ExcludeCurrentRow:
		return "execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW"
	case tree.ExcludeGroup:
		return "execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP"
	case tree.ExcludeTies:
		return "execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES"
	}
	return ""
}
