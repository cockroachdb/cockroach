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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
)

const windowFramerTmpl = "pkg/sql/colexec/colexecwindow/window_framer_tmpl.go"

var windowFrameModes = []treewindow.WindowFrameMode{treewindow.ROWS, treewindow.GROUPS, treewindow.RANGE}
var windowFrameStartBoundTypes = []treewindow.WindowFrameBoundType{
	treewindow.UnboundedPreceding, treewindow.OffsetPreceding, treewindow.CurrentRow, treewindow.OffsetFollowing,
}
var windowFrameEndBoundTypes = []treewindow.WindowFrameBoundType{
	treewindow.OffsetPreceding, treewindow.CurrentRow, treewindow.OffsetFollowing, treewindow.UnboundedFollowing,
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
	ModeType        treewindow.WindowFrameMode
	StartBoundTypes []windowFramerStartBoundInfo
}

type windowFramerStartBoundInfo struct {
	BoundType     treewindow.WindowFrameBoundType
	EndBoundTypes []windowFramerEndBoundInfo
}

type windowFramerEndBoundInfo struct {
	BoundType    treewindow.WindowFrameBoundType
	ExcludeInfos []windowFramerExcludeInfo
}

type windowFramerExcludeInfo struct {
	windowFramerTmplInfo
	Exclude bool
}

type windowFramerTmplInfo struct {
	OpString       string
	ModeType       treewindow.WindowFrameMode
	StartBoundType treewindow.WindowFrameBoundType
	EndBoundType   treewindow.WindowFrameBoundType
	Exclude        bool
}

func (overload windowFramerTmplInfo) GroupsMode() bool {
	return overload.ModeType == treewindow.GROUPS
}

var _ = windowFramerTmplInfo{}.GroupsMode()

func (overload windowFramerTmplInfo) RowsMode() bool {
	return overload.ModeType == treewindow.ROWS
}

var _ = windowFramerTmplInfo{}.RowsMode()

func (overload windowFramerTmplInfo) RangeMode() bool {
	return overload.ModeType == treewindow.RANGE
}

var _ = windowFramerTmplInfo{}.RangeMode()

func (overload windowFramerTmplInfo) StartUnboundedPreceding() bool {
	return overload.StartBoundType == treewindow.UnboundedPreceding
}

var _ = windowFramerTmplInfo{}.StartUnboundedPreceding()

func (overload windowFramerTmplInfo) StartOffsetPreceding() bool {
	return overload.StartBoundType == treewindow.OffsetPreceding
}

var _ = windowFramerTmplInfo{}.StartOffsetPreceding()

func (overload windowFramerTmplInfo) StartCurrentRow() bool {
	return overload.StartBoundType == treewindow.CurrentRow
}

var _ = windowFramerTmplInfo{}.StartCurrentRow()

func (overload windowFramerTmplInfo) StartOffsetFollowing() bool {
	return overload.StartBoundType == treewindow.OffsetFollowing
}

var _ = windowFramerTmplInfo{}.StartOffsetFollowing()

func (overload windowFramerTmplInfo) EndOffsetPreceding() bool {
	return overload.EndBoundType == treewindow.OffsetPreceding
}

var _ = windowFramerTmplInfo{}.EndOffsetPreceding()

func (overload windowFramerTmplInfo) EndCurrentRow() bool {
	return overload.EndBoundType == treewindow.CurrentRow
}

var _ = windowFramerTmplInfo{}.EndCurrentRow()

func (overload windowFramerTmplInfo) EndOffsetFollowing() bool {
	return overload.EndBoundType == treewindow.OffsetFollowing
}

var _ = windowFramerTmplInfo{}.EndOffsetFollowing()

func (overload windowFramerTmplInfo) EndUnboundedFollowing() bool {
	return overload.EndBoundType == treewindow.UnboundedFollowing
}

var _ = windowFramerTmplInfo{}.EndUnboundedFollowing()

func (overload windowFramerTmplInfo) OffsetPreceding() bool {
	return overload.StartBoundType == treewindow.OffsetPreceding ||
		overload.EndBoundType == treewindow.OffsetPreceding
}

var _ = windowFramerTmplInfo{}.OffsetPreceding()

func (overload windowFramerTmplInfo) StartHasOffset() bool {
	return overload.StartBoundType == treewindow.OffsetPreceding ||
		overload.StartBoundType == treewindow.OffsetFollowing
}

func (overload windowFramerTmplInfo) EndHasOffset() bool {
	return overload.EndBoundType == treewindow.OffsetPreceding ||
		overload.EndBoundType == treewindow.OffsetFollowing
}

func (overload windowFramerTmplInfo) HasOffset() bool {
	return overload.StartHasOffset() || overload.EndHasOffset()
}

var _ = windowFramerTmplInfo{}.HasOffset()

func (overload windowFramerTmplInfo) BothUnbounded() bool {
	return overload.StartBoundType == treewindow.UnboundedPreceding &&
		overload.EndBoundType == treewindow.UnboundedFollowing
}

var _ = windowFramerTmplInfo{}.BothUnbounded()

func modeToExecinfrapb(mode treewindow.WindowFrameMode) string {
	switch mode {
	case treewindow.RANGE:
		return "execinfrapb.WindowerSpec_Frame_RANGE"
	case treewindow.ROWS:
		return "execinfrapb.WindowerSpec_Frame_ROWS"
	case treewindow.GROUPS:
		return "execinfrapb.WindowerSpec_Frame_GROUPS"
	}
	return ""
}

func boundToExecinfrapb(bound treewindow.WindowFrameBoundType) string {
	switch bound {
	case treewindow.UnboundedPreceding:
		return "execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING"
	case treewindow.OffsetPreceding:
		return "execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING"
	case treewindow.CurrentRow:
		return "execinfrapb.WindowerSpec_Frame_CURRENT_ROW"
	case treewindow.OffsetFollowing:
		return "execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING"
	case treewindow.UnboundedFollowing:
		return "execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING"
	}
	return ""
}

func excludeToExecinfrapb(exclude treewindow.WindowFrameExclusion) string {
	switch exclude {
	case treewindow.NoExclusion:
		return "execinfrapb.WindowerSpec_Frame_NO_EXCLUSION"
	case treewindow.ExcludeCurrentRow:
		return "execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW"
	case treewindow.ExcludeGroup:
		return "execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP"
	case treewindow.ExcludeTies:
		return "execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES"
	}
	return ""
}
