// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"strings"
)

type dataManipulationReplacementInfo struct {
	re                  *regexp.Regexp
	templatePlaceholder string
	numArgs             int
	replaceWith         string
}

var dataManipulationReplacementInfos = []dataManipulationReplacementInfo{
	{
		templatePlaceholder: "execgen.UNSAFEGET",
		numArgs:             2,
		replaceWith:         "Get",
	},
	{
		templatePlaceholder: "execgen.COPYVAL",
		numArgs:             2,
		replaceWith:         "CopyVal",
	},
	{
		templatePlaceholder: "execgen.SET",
		numArgs:             3,
		replaceWith:         "Set",
	},
	{
		templatePlaceholder: "execgen.SLICE",
		numArgs:             3,
		replaceWith:         "Slice",
	},
	{
		templatePlaceholder: "execgen.COPYSLICE",
		numArgs:             5,
		replaceWith:         "CopySlice",
	},
	{
		templatePlaceholder: "execgen.APPENDSLICE",
		numArgs:             5,
		replaceWith:         "AppendSlice",
	},
	{
		templatePlaceholder: "execgen.APPENDVAL",
		numArgs:             2,
		replaceWith:         "AppendVal",
	},
	{
		templatePlaceholder: "execgen.LEN",
		numArgs:             1,
		replaceWith:         "Len",
	},
	{
		templatePlaceholder: "execgen.ZERO",
		numArgs:             1,
		replaceWith:         "Zero",
	},
	{
		templatePlaceholder: "execgen.RANGE",
		numArgs:             2,
		replaceWith:         "Range",
	},
}

func init() {
	for i, dmri := range dataManipulationReplacementInfos {
		placeHolderArgs := make([]string, dmri.numArgs)
		templResultArgs := make([]string, dmri.numArgs)
		for j := 0; j < dmri.numArgs; j++ {
			placeHolderArgs[j] = `(.*)`
			templResultArgs[j] = fmt.Sprintf("\"$%d\"", j+1)
		}
		dataManipulationReplacementInfos[i].templatePlaceholder += `\(` + strings.Join(placeHolderArgs, ",") + `\)`
		dataManipulationReplacementInfos[i].replaceWith += " " + strings.Join(templResultArgs, " ")
		dataManipulationReplacementInfos[i].re = regexp.MustCompile(dataManipulationReplacementInfos[i].templatePlaceholder)
	}
}

// replaceManipulationFuncs replaces commonly used template placeholders for
// data manipulation. typeIdent is the types.T struct used in the template
// (e.g. "" if using a types.T struct in templates directly or ".Type" if
// stored in the "Type" field of the template struct. body is the template body,
// which is returned with all the replacements. Refer to the init function in
// this file for a list of replacements done.
func replaceManipulationFuncs(typeIdent string, body string) string {
	for _, dmri := range dataManipulationReplacementInfos {
		body = dmri.re.ReplaceAllString(body, fmt.Sprintf("{{ %s.%s }}", typeIdent, dmri.replaceWith))
	}
	return body
}
