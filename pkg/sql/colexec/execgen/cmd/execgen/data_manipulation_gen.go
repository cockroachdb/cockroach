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
		templatePlaceholder: "execgen.COPYVAL",
		numArgs:             2,
		replaceWith:         "CopyVal",
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
		templatePlaceholder: "execgen.SETVARIABLESIZE",
		numArgs:             2,
		replaceWith:         "SetVariableSize",
	},
}

func init() {
	for i, dmri := range dataManipulationReplacementInfos {
		placeHolderArgs := make([]string, dmri.numArgs)
		tmplResultArgs := make([]string, dmri.numArgs)
		for j := 0; j < dmri.numArgs; j++ {
			placeHolderArgs[j] = `(.*)`
			tmplResultArgs[j] = fmt.Sprintf("\"$%d\"", j+1)
		}
		dataManipulationReplacementInfos[i].templatePlaceholder += `\(` + strings.Join(placeHolderArgs, ",") + `\)`
		dataManipulationReplacementInfos[i].replaceWith += " " + strings.Join(tmplResultArgs, " ")
		dataManipulationReplacementInfos[i].re = regexp.MustCompile(dataManipulationReplacementInfos[i].templatePlaceholder)
	}
}

// replaceManipulationFuncs replaces commonly used template placeholders for
// data manipulation. The cursor of the template (i.e. ".") should be pointing
// at the overload struct. body is the template body, which is returned with
// all the replacements. Refer to the init function in this file for a list of
// replacements done.
func replaceManipulationFuncs(body string) string {
	return replaceManipulationFuncsAmbiguous("", body)
}

// replaceManipulationFuncsAmbiguous is similar to replaceManipulationFuncs
// except for the cursor pointing at something other than the overload struct.
// overloadField should specify the path from the cursor to the overload struct
// that should be used for the replacements.
func replaceManipulationFuncsAmbiguous(overloadField string, body string) string {
	for _, dmri := range dataManipulationReplacementInfos {
		body = dmri.re.ReplaceAllString(body, fmt.Sprintf("{{%s.%s}}", overloadField, dmri.replaceWith))
	}
	return body
}
