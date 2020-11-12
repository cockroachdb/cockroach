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
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
	"github.com/cockroachdb/errors"
)

// Explain represents an EXPLAIN statement.
type Explain struct {
	ExplainOptions

	// Statement is the statement being EXPLAINed.
	Statement Statement
}

// ExplainAnalyze represents an EXPLAIN ANALYZE statement.
type ExplainAnalyze struct {
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

	// ExplainDebug generates a statement diagnostics bundle; only used with
	// EXPLAIN ANALYZE.
	ExplainDebug

	numExplainModes = iota
)

var explainModeStrings = [...]string{
	ExplainPlan:    "PLAN",
	ExplainDistSQL: "DISTSQL",
	ExplainOpt:     "OPT",
	ExplainVec:     "VEC",
	ExplainDebug:   "DEBUG",
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
	ExplainFlagEnv
	ExplainFlagCatalog
	numExplainFlags = iota
)

var explainFlagStrings = [...]string{
	ExplainFlagVerbose: "VERBOSE",
	ExplainFlagTypes:   "TYPES",
	ExplainFlagEnv:     "ENV",
	ExplainFlagCatalog: "CATALOG",
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
	wroteFlag := false
	if node.Mode != ExplainPlan {
		fmt.Fprintf(ctx, "(%s", node.Mode)
		wroteFlag = true
	}

	for f := ExplainFlag(1); f <= numExplainFlags; f++ {
		if node.Flags[f] {
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

// doc is part of the docer interface.
func (node *Explain) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Keyword("EXPLAIN")
	var opts []pretty.Doc
	if node.Mode != ExplainPlan {
		opts = append(opts, pretty.Keyword(node.Mode.String()))
	}
	for f := ExplainFlag(1); f <= numExplainFlags; f++ {
		if node.Flags[f] {
			opts = append(opts, pretty.Keyword(f.String()))
		}
	}
	if len(opts) > 0 {
		d = pretty.ConcatSpace(
			d,
			p.bracket("(", p.commaSeparated(opts...), ")"),
		)
	}
	return p.nestUnder(d, p.Doc(node.Statement))
}

// Format implements the NodeFormatter interface.
func (node *ExplainAnalyze) Format(ctx *FmtCtx) {
	fmt.Fprintf(ctx, "EXPLAIN ANALYZE (%s", node.Mode)

	for f := ExplainFlag(1); f <= numExplainFlags; f++ {
		if node.Flags[f] {
			fmt.Fprintf(ctx, ", %s", f.String())
		}
	}
	ctx.WriteString(") ")
	ctx.FormatNode(node.Statement)
}

// doc is part of the docer interface.
func (node *ExplainAnalyze) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Keyword("EXPLAIN ANALYZE")
	var opts []pretty.Doc
	opts = append(opts, pretty.Keyword(node.Mode.String()))
	for f := ExplainFlag(1); f <= numExplainFlags; f++ {
		if node.Flags[f] {
			opts = append(opts, pretty.Keyword(f.String()))
		}
	}
	if len(opts) > 0 {
		d = pretty.ConcatSpace(
			d,
			p.bracket("(", p.commaSeparated(opts...), ")"),
		)
	}
	return p.nestUnder(d, p.Doc(node.Statement))
}

// MakeExplain parses the EXPLAIN option strings and generates an Explain
// or ExplainAnalyze statement.
func MakeExplain(options []string, stmt Statement) (Statement, error) {
	for i := range options {
		options[i] = strings.ToUpper(options[i])
	}
	var opts ExplainOptions
	var analyze bool
	for _, opt := range options {
		opt = strings.ToUpper(opt)
		if m, ok := explainModeStringMap[opt]; ok {
			if opts.Mode != 0 {
				return nil, pgerror.Newf(pgcode.Syntax, "cannot set EXPLAIN mode more than once: %s", opt)
			}
			opts.Mode = m
			continue
		}
		if opt == "ANALYZE" {
			analyze = true
			continue
		}
		flag, ok := explainFlagStringMap[opt]
		if !ok {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported EXPLAIN option: %s", opt)
		}
		opts.Flags[flag] = true
	}
	if opts.Mode == 0 {
		if analyze {
			// ANALYZE implies DISTSQL.
			opts.Mode = ExplainDistSQL
		} else {
			// Default mode is ExplainPlan.
			opts.Mode = ExplainPlan
		}
	}

	if analyze {
		if opts.Mode != ExplainDistSQL && opts.Mode != ExplainDebug && opts.Mode != ExplainPlan {
			return nil, pgerror.Newf(pgcode.Syntax, "EXPLAIN ANALYZE cannot be used with %s", opts.Mode)
		}
		return &ExplainAnalyze{
			ExplainOptions: opts,
			Statement:      stmt,
		}, nil
	}

	if opts.Mode == ExplainDebug {
		return nil, pgerror.Newf(pgcode.Syntax, "DEBUG flag can only be used with EXPLAIN ANALYZE")
	}
	return &Explain{
		ExplainOptions: opts,
		Statement:      stmt,
	}, nil
}
