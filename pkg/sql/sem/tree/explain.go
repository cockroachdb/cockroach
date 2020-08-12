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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// Explain represents an EXPLAIN statement.
type Explain struct {
	ExplainOptions

	// Statement is the statement being EXPLAINed.
	Statement Statement
}

// ExplainOptions contains information about the options passed to an EXPLAIN
// statement.
type ExplainOptions struct {
	Mode  ExplainMode
	Flags [numExplainFlags + 1]bool
}

// ExplainMode indicates the mode of the explain. Currently there are two modes:
// PLAN (the default) and DISTSQL.
type ExplainMode uint8

const (
	// ExplainPlan shows information about the planNode tree for a query.
	ExplainPlan ExplainMode = 1 + iota

	// ExplainDistSQL shows the physical distsql plan for a query and whether a
	// query would be run in "auto" DISTSQL mode. See sql/explain_distsql.go for
	// details. If the ANALYZE option is included, the plan is also executed and
	// execution statistics are collected and shown in the diagram.
	ExplainDistSQL

	// ExplainOpt shows the optimized relational expression (from the cost-based
	// optimizer).
	ExplainOpt

	// ExplainVec shows the physical vectorized plan for a query and whether a
	// query would be run in "auto" vectorized mode.
	ExplainVec

	numExplainModes = iota
)

var explainModeStrings = [...]string{
	ExplainPlan:    "PLAN",
	ExplainDistSQL: "DISTSQL",
	ExplainOpt:     "OPT",
	ExplainVec:     "VEC",
}

var explainModeStringMap = func() map[string]ExplainMode {
	m := make(map[string]ExplainMode, numExplainModes)
	for i := ExplainMode(1); i <= numExplainModes; i++ {
		m[explainModeStrings[i]] = i
	}
	return m
}()

func (m ExplainMode) String() string {
	if m == 0 || m > numExplainModes {
		panic(errors.AssertionFailedf("invalid ExplainMode %d", m))
	}
	return explainModeStrings[m]
}

// ExplainFlag is a modifier in an EXPLAIN statement (like VERBOSE).
type ExplainFlag uint8

// Explain flags.
const (
	ExplainFlagVerbose ExplainFlag = 1 + iota
	ExplainFlagTypes
	ExplainFlagAnalyze
	ExplainFlagEnv
	ExplainFlagCatalog
	ExplainFlagDebug
	numExplainFlags = iota
)

var explainFlagStrings = [...]string{
	ExplainFlagVerbose: "VERBOSE",
	ExplainFlagTypes:   "TYPES",
	ExplainFlagAnalyze: "ANALYZE",
	ExplainFlagEnv:     "ENV",
	ExplainFlagCatalog: "CATALOG",
	ExplainFlagDebug:   "DEBUG",
}

var explainFlagStringMap = func() map[string]ExplainFlag {
	m := make(map[string]ExplainFlag, numExplainFlags)
	for i := ExplainFlag(1); i <= numExplainFlags; i++ {
		m[explainFlagStrings[i]] = i
	}
	return m
}()

func (f ExplainFlag) String() string {
	if f == 0 || f > numExplainFlags {
		panic(errors.AssertionFailedf("invalid ExplainFlag %d", f))
	}
	return explainFlagStrings[f]
}

// Format implements the NodeFormatter interface.
func (node *Explain) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPLAIN ")
	showMode := node.Mode != ExplainPlan
	// ANALYZE is a special case because it is a statement implemented as an
	// option to EXPLAIN.
	if node.Flags[ExplainFlagAnalyze] {
		ctx.WriteString("ANALYZE ")
		showMode = true
	}
	wroteFlag := false
	if showMode {
		fmt.Fprintf(ctx, "(%s", node.Mode)
		wroteFlag = true
	}

	for f := ExplainFlag(1); f <= numExplainFlags; f++ {
		if f != ExplainFlagAnalyze && node.Flags[f] {
			if !wroteFlag {
				ctx.WriteString("(")
				wroteFlag = true
			} else {
				ctx.WriteString(", ")
			}
			ctx.WriteString(f.String())
		}
	}
	if wroteFlag {
		ctx.WriteString(") ")
	}
	ctx.FormatNode(node.Statement)
}

// ExplainAnalyzeDebug represents an EXPLAIN ANALYZE (DEBUG) statement. It is a
// different node type than Explain to allow easier special treatment in the SQL
// layer.
type ExplainAnalyzeDebug struct {
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *ExplainAnalyzeDebug) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPLAIN ANALYZE (DEBUG) ")
	ctx.FormatNode(node.Statement)
}

// MakeExplain parses the EXPLAIN option strings and generates an explain
// statement.
func MakeExplain(options []string, stmt Statement) (Statement, error) {
	for i := range options {
		options[i] = strings.ToUpper(options[i])
	}
	var opts ExplainOptions
	for _, opt := range options {
		opt = strings.ToUpper(opt)
		if m, ok := explainModeStringMap[opt]; ok {
			if opts.Mode != 0 {
				return nil, pgerror.Newf(pgcode.Syntax, "cannot set EXPLAIN mode more than once: %s", opt)
			}
			opts.Mode = m
			continue
		}
		flag, ok := explainFlagStringMap[opt]
		if !ok {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported EXPLAIN option: %s", opt)
		}
		opts.Flags[flag] = true
	}
	analyze := opts.Flags[ExplainFlagAnalyze]
	if opts.Mode == 0 {
		if analyze {
			// ANALYZE implies DISTSQL.
			opts.Mode = ExplainDistSQL
		} else {
			// Default mode is ExplainPlan.
			opts.Mode = ExplainPlan
		}
	} else if analyze && opts.Mode != ExplainDistSQL {
		return nil, pgerror.Newf(pgcode.Syntax, "EXPLAIN ANALYZE cannot be used with %s", opts.Mode)
	}

	if opts.Flags[ExplainFlagDebug] {
		if !analyze {
			return nil, pgerror.Newf(pgcode.Syntax, "DEBUG flag can only be used with EXPLAIN ANALYZE")
		}
		if len(options) != 2 {
			return nil, pgerror.Newf(pgcode.Syntax,
				"EXPLAIN ANALYZE (DEBUG) cannot be used in conjunction with other flags",
			)
		}
		return &ExplainAnalyzeDebug{Statement: stmt}, nil
	}

	return &Explain{
		ExplainOptions: opts,
		Statement:      stmt,
	}, nil
}
