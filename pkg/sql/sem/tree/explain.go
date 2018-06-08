// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util"
)

// Explain represents an EXPLAIN statement.
type Explain struct {
	// Options defines how EXPLAIN should operate (e.g. VERBOSE).
	// Which options are valid depends on the explain mode. See
	// sql/explain.go for details.
	Options []string

	// Statement is the statement being EXPLAINed.
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *Explain) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPLAIN ")
	if len(node.Options) > 0 {
		// ANALYZE is a special case because it is a statement implemented as an
		// option to EXPLAIN. We therefore create a buffer for all the options in
		// case we hit an ANALYZE to add that right after the EXPLAIN.
		var optsBuffer bytes.Buffer
		for _, opt := range node.Options {
			upperCaseOpt := strings.ToUpper(opt)
			if upperCaseOpt == "ANALYZE" {
				ctx.WriteString("ANALYZE ")
			} else {
				// If we have written to the options buffer, append a comma to separate
				// the previous option from this one.
				if optsBuffer.Len() > 0 {
					optsBuffer.WriteString(", ")
				}
				optsBuffer.WriteString(upperCaseOpt)
			}
		}
		// Write the options.
		ctx.WriteByte('(')
		ctx.Write(optsBuffer.Bytes())
		ctx.WriteString(") ")
	}
	ctx.FormatNode(node.Statement)
}

// ExplainOptions contains information about the options passed to an EXPLAIN
// statement.
type ExplainOptions struct {
	Mode  ExplainMode
	Flags util.FastIntSet
}

// ExplainMode indicates the mode of the explain. Currently there are two modes:
// PLAN (the default) and DISTSQL.
type ExplainMode uint8

const (
	// ExplainPlan shows information about the planNode tree for a query.
	ExplainPlan ExplainMode = iota

	// ExplainDistSQL shows the physical distsql plan for a query and whether a
	// query would be run in "auto" DISTSQL mode. See sql/explain_distsql.go for
	// details.
	ExplainDistSQL

	// ExplainOpt shows the optimized relational expression (from the cost-based
	// optimizer).
	ExplainOpt
)

var explainModeStrings = map[string]ExplainMode{
	"plan":    ExplainPlan,
	"distsql": ExplainDistSQL,
	"opt":     ExplainOpt,
}

// Explain flags.
const (
	ExplainFlagVerbose = iota
	ExplainFlagSymVars
	ExplainFlagTypes
	ExplainFlagNoExpand
	ExplainFlagNoNormalize
	ExplainFlagNoOptimize
	ExplainFlagAnalyze
)

var explainFlagStrings = map[string]int{
	"verbose":     ExplainFlagVerbose,
	"symvars":     ExplainFlagSymVars,
	"types":       ExplainFlagTypes,
	"noexpand":    ExplainFlagNoExpand,
	"nonormalize": ExplainFlagNoNormalize,
	"nooptimize":  ExplainFlagNoOptimize,
	"analyze":     ExplainFlagAnalyze,
}

// ParseOptions parses the options for an EXPLAIN statement.
func (node *Explain) ParseOptions() (ExplainOptions, error) {
	// If not specified, the default mode is ExplainPlan.
	res := ExplainOptions{Mode: ExplainPlan}
	modeSet := false
	for _, opt := range node.Options {
		optLower := strings.ToLower(opt)
		if mode, ok := explainModeStrings[optLower]; ok {
			if modeSet {
				return ExplainOptions{}, fmt.Errorf("cannot set EXPLAIN mode more than once: %s", opt)
			}
			res.Mode = mode
			modeSet = true
			continue
		}
		flag, ok := explainFlagStrings[optLower]
		if !ok {
			return ExplainOptions{}, fmt.Errorf("unsupported EXPLAIN option: %s", opt)
		}
		res.Flags.Add(flag)
	}
	return res, nil
}
