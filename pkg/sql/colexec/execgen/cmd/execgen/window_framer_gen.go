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
)

type windowFrameModeInfo struct {
	ModeString      string
	StartBoundTypes []windowFramerStartBoundInfo
}

type windowFramerStartBoundInfo struct {
	BoundString   string
	EndBoundTypes []windowFramerEndBoundInfo
}

type windowFramerEndBoundInfo struct {
	BoundString string
	Overloads   []windowFrameTmplInfo
}

type windowFrameTmplInfo struct {
	OpString string
	Exclude  bool

	RowsMode                bool
	GroupsMode              bool
	RangeMode               bool
	StartUnboundedPreceding bool
	StartOffsetPreceding    bool
	StartCurrentRow         bool
	StartOffsetFollowing    bool
	EndOffsetPreceding      bool
	EndCurrentRow           bool
	EndOffsetFollowing      bool
	EndUnboundedFollowing   bool
	StartHasOffset          bool
	EndHasOffset            bool
	HasOffsets              bool
}

var windowFramerBoundTypeMap = map[string]int{
	"UnboundedPreceding": 0,
	"OffsetPreceding":    1,
	"CurrentRow":         2,
	"OffsetFollowing":    3,
	"UnboundedFollowing": 4,
}

const windowFramerTmpl = "pkg/sql/colexec/colexecwindow/window_framer_tmpl.go"

const modeReplacement = "{{if eq .ModeString \"Range\"}}execinfrapb.WindowerSpec_Frame_RANGE{{end}}" +
	"{{if eq .ModeString \"Rows\"}}execinfrapb.WindowerSpec_Frame_ROWS{{end}}" +
	"{{if eq .ModeString \"Groups\"}}execinfrapb.WindowerSpec_Frame_GROUPS{{end}}"

const boundReplacement = "{{if eq .BoundString \"UnboundedPreceding\"}}" +
	"execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING{{end}}" +
	"{{if eq .BoundString \"OffsetPreceding\"}}" +
	"execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING{{end}}" +
	"{{if eq .BoundString \"CurrentRow\"}}" +
	"execinfrapb.WindowerSpec_Frame_CURRENT_ROW{{end}}" +
	"{{if eq .BoundString \"OffsetFollowing\"}}" +
	"execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING{{end}}" +
	"{{if eq .BoundString \"UnboundedFollowing\"}}" +
	"execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING{{end}}"

const excludeReplacement = "{{.Exclude}}"

func windowFramerGenerator(inputFileContents string, wr io.Writer) error {
	r := strings.NewReplacer(
		"_OP_STRING", "{{.OpString}}",
		"_FRAME_MODE", modeReplacement,
		"_START_BOUND", boundReplacement,
		"_END_BOUND", boundReplacement,
		"_EXCLUSION_CASE", excludeReplacement,
		"_TYPE_FAMILY", "{{.TypeFamily}}",
		"_TYPE_WIDTH", typeWidthReplacement,
		"_GOTYPE", `{{.GoType}}`,
		"_TYPE", "{{.VecMethod}}",
		"TemplateType", "{{.VecMethod}}",
		"_START_HAS_OFFSET", "or (eq .BoundString \"OFFSET PRECEDING\") (eq .BoundString \"OFFSET FOLLOWING\")",
		"_END_HAS_OFFSET", "or (eq .BoundString \"OFFSET PRECEDING\") (eq .BoundString \"OFFSET FOLLOWING\")",
	)
	s := r.Replace(inputFileContents)

	nextPeerGroup := makeFunctionRegex("_NEXT_PEER_GROUP", 0)
	s = nextPeerGroup.ReplaceAllString(s, `{{template "nextPeerGroup"}}`)

	r = strings.NewReplacer("_IDX", "{{$idx}}")
	s = r.Replace(s)

	s = replaceManipulationFuncs(s)

	tmpl, err := template.New("window_framer").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	var tmplInfos []windowFrameModeInfo
	for _, modeString := range []string{"Rows", "Groups", "Range"} {
		modeInfo := windowFrameModeInfo{
			ModeString: modeString,
		}
		for _, startBoundType := range []string{"UnboundedPreceding", "OffsetPreceding", "CurrentRow", "OffsetFollowing"} {
			startHasOffset := startBoundType == "OffsetPreceding" ||
				startBoundType == "OffsetFollowing"
			startBoundInfo := windowFramerStartBoundInfo{
				BoundString: startBoundType,
			}
			for _, endBoundType := range []string{"OffsetPreceding", "CurrentRow", "OffsetFollowing", "UnboundedFollowing"} {
				if windowFramerBoundTypeMap[endBoundType] < windowFramerBoundTypeMap[startBoundType] {
					continue
				}
				endHasOffset := endBoundType == "OffsetPreceding" ||
					endBoundType == "OffsetFollowing"
				hasOffsets := startHasOffset || endHasOffset
				endBoundInfo := windowFramerEndBoundInfo{
					BoundString: endBoundType,
				}
				for _, exclude := range []bool{false, true} {
					opString := "windowFramer" + modeString + startBoundType + endBoundType
					if exclude {
						opString += "Exclude"
					}
					overload := windowFrameTmplInfo{
						OpString:                opString,
						Exclude:                 exclude,
						RowsMode:                modeString == "Rows",
						GroupsMode:              modeString == "Groups",
						RangeMode:               modeString == "Range",
						StartUnboundedPreceding: startBoundType == "UnboundedPreceding",
						StartOffsetPreceding:    startBoundType == "OffsetPreceding",
						StartCurrentRow:         startBoundType == "CurrentRow",
						StartOffsetFollowing:    startBoundType == "OffsetFollowing",
						EndOffsetPreceding:      endBoundType == "OffsetPreceding",
						EndCurrentRow:           endBoundType == "CurrentRow",
						EndOffsetFollowing:      endBoundType == "OffsetFollowing",
						EndUnboundedFollowing:   endBoundType == "UnboundedFollowing",
						StartHasOffset:          startHasOffset,
						EndHasOffset:            endHasOffset,
						HasOffsets:              hasOffsets,
					}
					endBoundInfo.Overloads = append(endBoundInfo.Overloads, overload)
				}
				startBoundInfo.EndBoundTypes = append(startBoundInfo.EndBoundTypes, endBoundInfo)
			}
			modeInfo.StartBoundTypes = append(modeInfo.StartBoundTypes, startBoundInfo)
		}
		tmplInfos = append(tmplInfos, modeInfo)
	}
	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(windowFramerGenerator, "window_framer.eg.go", windowFramerTmpl)
}
