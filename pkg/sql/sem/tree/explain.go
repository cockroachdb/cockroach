// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	// ExplainVec shows the physical vectorized plan for a query and whether a
	// query would be run in "auto" vectorized mode.
	ExplainVec
)

var explainModeStrings = map[string]ExplainMode{
	"plan":    ExplainPlan,
	"distsql": ExplainDistSQL,
	"opt":     ExplainOpt,
	"vec":     ExplainVec,
}

// ExplainModeName returns the human-readable name of a given ExplainMode.
func ExplainModeName(mode ExplainMode) (string, error) {
	for k, v := range explainModeStrings {
		if v == mode {
			return k, nil
		}
	}
	return "", errors.AssertionFailedf("no name for explain mode %v", log.Safe(mode))
}

// Explain flags.
const (
	ExplainFlagVerbose = iota
	ExplainFlagSymVars
	ExplainFlagTypes
	ExplainFlagNoNormalize
	ExplainFlagAnalyze
	ExplainFlagEnv
	ExplainFlagCatalog
)

var explainFlagStrings = map[string]int{
	"verbose":     ExplainFlagVerbose,
	"symvars":     ExplainFlagSymVars,
	"types":       ExplainFlagTypes,
	"nonormalize": ExplainFlagNoNormalize,
	"analyze":     ExplainFlagAnalyze,
	"env":         ExplainFlagEnv,
	"catalog":     ExplainFlagCatalog,
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
				return ExplainOptions{}, pgerror.Newf(pgcode.Syntax,
					"cannot set EXPLAIN mode more than once: %s", opt)
			}
			res.Mode = mode
			modeSet = true
			continue
		}
		flag, ok := explainFlagStrings[optLower]
		if !ok {
			return ExplainOptions{}, pgerror.Newf(pgcode.Syntax,
				"unsupported EXPLAIN option: %s", opt)
		}
		res.Flags.Add(flag)
	}
	return res, nil
}

// ExplainBundle represents an EXPLAIN BUNDLE statement. It is a different node
// type than Explain to allow easier special treatment in the SQL layer.
type ExplainBundle struct {
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *ExplainBundle) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPLAIN BUNDLE ")
	ctx.FormatNode(node.Statement)
}
