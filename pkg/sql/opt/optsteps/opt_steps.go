// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optsteps

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pmezard/go-difflib/difflib"
)

// OptSteps implements the stepping algorithm used by the OptTester's OptSteps
// command. See the OptTester.OptSteps comment for more details on the command.
//
// The algorithm works as follows:
//   1. The first time OptSteps.next() is called, OptSteps returns the starting
//      expression tree, with no transformations applied to it.
//
//   2. Each OptSteps.next() call after that will perform N+1 transformations,
//      where N is the number of steps performed during the previous call
//      (starting at 0 with the first call).
//
//   3. Each OptSteps.next() call will build the expression tree from scratch
//      and re-run all transformations that were run in the previous call, plus
//      one additional transformation (N+1). Therefore, the output expression
//      tree from each call will differ from the previous call only by the last
//      transformation's changes.
//
//   4. OptSteps hooks the optimizer's MatchedRule event in order to limit the
//      number of transformations that can be applied, as well as to record the
//      name of the last rule that was applied, for later output.
//
//   5. While this works well for normalization rules, exploration rules are
//      more difficult. This is because exploration rules are not guaranteed to
//      produce a lower cost tree. Unless extra measures are taken, the returned
//      Expr would not include the changed portion of the Memo, since Expr only
//      shows the lowest cost path through the Memo.
//
//   6. To address this issue, OptSteps hooks the optimizer's AppliedRule event
//      and records the expression(s) that the last transformation has affected.
//      It then re-runs the optimizer, but this time using a special Coster
//      implementation that fools the optimizer into thinking that the new
//      expression(s) have the lowest cost. The coster does this by assigning an
//      infinite cost to all other expressions in the same group as the new
//      expression(s), as well as in all ancestor groups.
//
type OptSteps struct {
	fo *forcingOptimizer

	// sql is the input SQL to run optsteps on.
	sql string

	evalCtx *tree.EvalContext
	semaCtx tree.SemaContext
	catalog cat.Catalog

	// steps is the maximum number of rules that can be applied by the optimizer
	// during the current iteration.
	steps int

	// expr is the expression tree produced by the most recent OptSteps iteration.
	expr opt.Expr

	// better is true if expr is lower cost than the expression tree produced by
	// the previous iteration of OptSteps.
	better bool

	// best is the textual representation of the most recent expression tree that
	// was an improvement over the previous best tree.
	best string

	builder strings.Builder

	flags Flags
}

type Flags struct {
	// DisableRules is a set of rules that are not allowed to run.
	DisableRules RuleSet

	// OptStepsSplitDiff, if true, replaces the unified diff output of the
	// optsteps command with a split diff where the before and after expressions
	// are printed in their entirety. The default value is false.
	OptStepsSplitDiff bool

	// Verbose indicates whether verbose test debugging information will be
	// output to stdout when commands run. Only certain commands support this.
	Verbose bool

	// ExprFormat controls the output detail of build / opt/ optsteps command
	// directives.
	ExprFormat memo.ExprFmtFlags

	// ExploreTraceRule restricts the ExploreTrace output to only show the effects
	// of a specific rule.
	ExploreTraceRule opt.RuleName

	// ExploreTraceSkipNoop hides the ExploreTrace output for instances of rules
	// that fire but don't add any new expressions to the memo.
	ExploreTraceSkipNoop bool
}

func NewOptSteps(
	evalCtx *tree.EvalContext, catalog cat.Catalog, sql string, flags Flags,
) *OptSteps {
	return &OptSteps{
		sql:     sql,
		evalCtx: evalCtx,
		semaCtx: tree.MakeSemaContext(),
		catalog: catalog,
		flags:   flags,
	}
}

// OptSteps steps through the transformations performed by the optimizer on the
// memo, one-by-one. The output of each step is the lowest cost expression tree
// that also contains the expressions that were changed or added by the
// transformation. The output of each step is diff'd against the output of a
// previous step, using the standard unified diff format.
//
//   CREATE TABLE a (x INT PRIMARY KEY, y INT, UNIQUE INDEX (y))
//
//   SELECT x FROM a WHERE x=1
//
// At the time of this writing, this query triggers 6 rule applications:
//   EnsureSelectFilters     Wrap Select predicate with Filters operator
//   FilterUnusedSelectCols  Do not return unused "y" column from Scan
//   EliminateProject        Remove unneeded Project operator
//   GenerateIndexScans      Explore scanning "y" index to get "x" values
//   ConstrainScan           Explore pushing "x=1" into "x" index Scan
//   ConstrainScan           Explore pushing "x=1" into "y" index Scan
//
// Some steps produce better plans that have a lower execution cost. Other steps
// don't. However, it's useful to see both kinds of steps. The optsteps output
// distinguishes these two cases by using stronger "====" header delimiters when
// a better plan has been found, and weaker "----" header delimiters when not.
// In both cases, the output shows the expressions that were changed or added by
// the rule, even if the total expression tree cost worsened.
//
func (os *OptSteps) OptSteps(ctx context.Context) (string, error) {
	var prevBest, prev, next string
	os.builder.Reset()

	for {
		err := os.Next(ctx)
		if err != nil {
			return "", err
		}

		next = os.fo.o.FormatExpr(os.Root(), os.flags.ExprFormat)

		// This call comes after setting "next", because we want to output the
		// final expression, even though there were no diffs from the previous
		// iteration.
		if os.Done() {
			break
		}

		if prev == "" {
			// Output starting tree.
			os.optStepsDisplay("", next)
			prevBest = next
		} else if next == prev || next == prevBest {
			os.optStepsDisplay(next, next)
		} else if os.IsBetter() {
			// New expression is better than the previous expression. Diff
			// it against the previous *best* expression (might not be the
			// previous expression).
			os.optStepsDisplay(prevBest, next)
			prevBest = next
		} else {
			// New expression is not better than the previous expression, but
			// still show the change. Diff it against the previous expression,
			// regardless if it was a "best" expression or not.
			os.optStepsDisplay(prev, next)
		}

		prev = next
	}

	// Output ending tree.
	os.optStepsDisplay(next, "")

	return os.builder.String(), nil
}

func (os *OptSteps) optStepsDisplay(before string, after string) {
	// bestHeader is used when the expression is an improvement over the previous
	// expression.
	bestHeader := func(e opt.Expr, format string, args ...interface{}) {
		os.separator("=")
		os.output(format, args...)
		if rel, ok := e.(memo.RelExpr); ok {
			os.output("  Cost: %.2f\n", rel.Cost())
		} else {
			os.output("\n")
		}
		os.separator("=")
	}

	// altHeader is used when the expression doesn't improve over the previous
	// expression, but it's still desirable to see what changed.
	altHeader := func(format string, args ...interface{}) {
		os.separator("-")
		os.output(format, args...)
		os.separator("-")
	}

	if before == "" {
		if os.flags.Verbose {
			fmt.Print("------ optsteps verbose output starts ------\n")
		}
		bestHeader(os.Root(), "Initial expression\n")
		os.indent(after)
		return
	}

	if before == after {
		altHeader("%s (no changes)\n", os.LastRuleName())
		return
	}

	if after == "" {
		bestHeader(os.Root(), "Final best expression\n")
		os.indent(before)

		if os.flags.Verbose {
			fmt.Print("------ optsteps verbose output ends ------\n")
		}
		return
	}

	if os.IsBetter() {
		// New expression is better than the previous expression. Diff
		// it against the previous *best* expression (might not be the
		// previous expression).
		bestHeader(os.Root(), "%s\n", os.LastRuleName())
	} else {
		altHeader("%s (higher cost)\n", os.LastRuleName())
	}

	if os.flags.OptStepsSplitDiff {
		os.output("<<<<<<< before\n")
		os.indent(before)
		os.output("=======\n")
		os.indent(after)
		os.output(">>>>>>> after\n")
	} else {
		diff := difflib.UnifiedDiff{
			A:       difflib.SplitLines(before),
			B:       difflib.SplitLines(after),
			Context: 100,
		}
		text, _ := difflib.GetUnifiedDiffString(diff)
		// Skip the "@@ ... @@" header (first line).
		text = strings.SplitN(text, "\n", 2)[1]
		os.indent(text)
	}
}

func (os *OptSteps) output(format string, args ...interface{}) {
	fmt.Fprintf(&os.builder, format, args...)
	if os.flags.Verbose {
		fmt.Printf(format, args...)
	}
}

func (os *OptSteps) separator(sep string) {
	os.output("%s\n", strings.Repeat(sep, 80))
}

func (os *OptSteps) indent(str string) {
	str = strings.TrimRight(str, " \n\t\r")
	lines := strings.Split(str, "\n")
	for _, line := range lines {
		os.output("  %s\n", line)
	}
}

// Root returns the node tree produced by the most recent OptSteps iteration.
func (os *OptSteps) Root() opt.Expr {
	return os.expr
}

// LastRuleName returns the name of the rule that was most recently matched by
// the optimizer.
func (os *OptSteps) LastRuleName() opt.RuleName {
	return os.fo.lastMatched
}

// IsBetter returns true if root is lower cost than the expression tree
// produced by the previous iteration of OptSteps.
func (os *OptSteps) IsBetter() bool {
	return os.better
}

// Done returns true if there are no more rules to apply. Further calls to the
// next method will result in a panic.
func (os *OptSteps) Done() bool {
	// remaining starts out equal to steps, and is decremented each time a rule
	// is applied. If it never reaches zero, then all possible rules were
	// already applied, and optimization is complete.
	return os.fo != nil && os.fo.remaining != 0
}

// Next triggers the next iteration of OptSteps. If there is no error, then
// results of the iteration can be accessed via the Root, LastRuleName, and
// IsBetter methods.
func (os *OptSteps) Next(ctx context.Context) error {
	if os.Done() {
		panic("iteration already complete")
	}

	fo, err := newForcingOptimizer(ctx, os, os.steps, false /* ignoreNormRules */, true /* disableCheckExpr */)
	if err != nil {
		return err
	}

	os.fo = fo
	os.expr = fo.Optimize()
	text := os.expr.String()

	// If the expression text changes, then it must have gotten better.
	os.better = text != os.best
	if os.better {
		os.best = text
	} else if !os.Done() {
		// The expression is not better, so suppress the lowest cost expressions
		// so that the changed portions of the tree will be part of the output.
		fo2, err := newForcingOptimizer(ctx, os, os.steps, false /* ignoreNormRules */, true /* disableCheckExpr */)
		if err != nil {
			return err
		}

		if fo.lastAppliedSource == nil {
			// This was a normalization that created a new memo group.
			fo2.RestrictToExpr(fo.LookupPath(fo.lastAppliedTarget))
		} else if fo.lastAppliedTarget == nil {
			// This was an exploration rule that didn't add any expressions to the
			// group, so only ancestor groups need to be suppressed.
			path := fo.LookupPath(fo.lastAppliedSource)
			fo2.RestrictToExpr(path[:len(path)-1])
		} else {
			// This was an exploration rule that added one or more expressions to
			// an existing group. Suppress all other members of the group.
			member := fo.lastAppliedTarget.(memo.RelExpr)
			for member != nil {
				fo2.RestrictToExpr(fo.LookupPath(member))
				member = member.NextExpr()
			}
		}
		os.expr = fo2.Optimize()
	}

	os.steps++
	return nil
}

// OptStepsWeb is similar to Optsteps but it uses a special web page for
// formatting the output. The result will be an URL which contains the encoded
// data.
func (os *OptSteps) OptStepsWeb(ctx context.Context) (string, error) {
	normDiffStr, err := os.optStepsNormDiff(ctx)
	if err != nil {
		return "", err
	}

	exploreDiffStr, err := os.optStepsExploreDiff(ctx)
	if err != nil {
		return "", err
	}
	url, err := os.encodeOptstepsURL(normDiffStr, exploreDiffStr)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

// optStepsNormDiff produces the normalization steps as a diff where each step
// is a pair of "files" (showing the before and after plans).
func (os *OptSteps) optStepsNormDiff(ctx context.Context) (string, error) {
	// Store all the normalization steps.
	type step struct {
		Name string
		Expr string
	}
	var normSteps []step
	for os := NewOptSteps(os.evalCtx, os.catalog, os.sql, os.flags); !os.Done(); {
		err := os.Next(ctx)
		if err != nil {
			return "", err
		}
		expr := os.fo.o.FormatExpr(os.Root(), os.flags.ExprFormat)
		name := "Initial"
		if len(normSteps) > 0 {
			rule := os.LastRuleName()
			if rule.IsExplore() {
				// Stop at the first exploration rule.
				break
			}
			name = rule.String()
		}
		normSteps = append(normSteps, step{Name: name, Expr: expr})
	}

	var buf bytes.Buffer
	for i, s := range normSteps {
		before := ""
		if i > 0 {
			before = normSteps[i-1].Expr
		}
		after := s.Expr
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(before),
			FromFile: fmt.Sprintf("a/%s", s.Name),
			B:        difflib.SplitLines(after),
			ToFile:   fmt.Sprintf("b/%s", s.Name),
			Context:  10000,
		}
		diffStr, err := difflib.GetUnifiedDiffString(diff)
		if err != nil {
			return "", err
		}
		diffStr = strings.TrimRight(diffStr, " \r\t\n")
		buf.WriteString(diffStr)
		buf.WriteString("\n")
	}
	return buf.String(), nil
}

// optStepsExploreDiff produces the exploration steps as a diff where each new
// expression is shown as a pair of "files" (showing the before and after
// expression). Note that normalization rules that are applied as part of
// creating the new expression are not shown separately.
func (os *OptSteps) optStepsExploreDiff(ctx context.Context) (string, error) {
	et := newExploreTracer(os)

	var buf bytes.Buffer

	for step := 0; ; step++ {
		if step > 2000 {
			os.output("step limit reached\n")
			break
		}
		err := et.Next(ctx)
		if err != nil {
			return "", err
		}
		if et.Done() {
			break
		}

		if os.flags.ExploreTraceRule != opt.InvalidRuleName &&
			et.LastRuleName() != os.flags.ExploreTraceRule {
			continue
		}
		newNodes := et.NewExprs()
		before := et.fo.o.FormatExpr(et.SrcExpr(), os.flags.ExprFormat)

		for i := range newNodes {
			name := et.LastRuleName().String()
			after := memo.FormatExpr(newNodes[i], os.flags.ExprFormat, et.fo.o.Memo(), os.catalog)

			diff := difflib.UnifiedDiff{
				A:        difflib.SplitLines(before),
				FromFile: fmt.Sprintf("a/%s", name),
				B:        difflib.SplitLines(after),
				ToFile:   fmt.Sprintf("b/%s", name),
				Context:  10000,
			}
			diffStr, err := difflib.GetUnifiedDiffString(diff)
			if err != nil {
				return "", err
			}
			diffStr = strings.TrimRight(diffStr, " \r\t\n")
			if diffStr == "" {
				// It's possible that the "new" expression is identical to the original
				// one; ignore it in that case.
				continue
			}
			buf.WriteString(diffStr)
			buf.WriteString("\n")
		}
	}
	return buf.String(), nil
}

func (os *OptSteps) encodeOptstepsURL(normDiff, exploreDiff string) (url.URL, error) {
	output := struct {
		SQL         string
		NormDiff    string
		ExploreDiff string
	}{
		SQL:         os.sql,
		NormDiff:    normDiff,
		ExploreDiff: exploreDiff,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(output); err != nil {
		return url.URL{}, err
	}
	var compressed bytes.Buffer

	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := buf.WriteTo(compressor); err != nil {
		return url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return url.URL{}, err
	}
	url := url.URL{
		Scheme: "https",
		Host:   "raduberinde.github.io",
		Path:   "optsteps.html",
	}
	const githubPagesMaxURLLength = 8100
	if compressed.Len() > githubPagesMaxURLLength {
		// If the compressed data is longer than the maximum allowed URL length
		// for the GitHub Pages server, we include it as a fragment. This
		// prevents the browser from sending this data to the server, avoiding a
		// 414 error from GitHub Pages.
		url.Fragment = compressed.String()
	} else {
		// Otherwise, the compressed data is included as a query parameter. This
		// is preferred because the URL remains valid when anchor links are
		// clicked and fragments are added to the URL by the browser.
		url.RawQuery = compressed.String()
	}
	return url, nil
}

// ExploreTrace steps through exploration transformations performed by the
// optimizer, one-by-one. The output of each step is the expression on which the
// rule was applied, and the expressions that were generated by the rule.
func (os *OptSteps) ExploreTrace(ctx context.Context) (string, error) {
	os.builder.Reset()

	et := newExploreTracer(os)

	for step := 0; ; step++ {
		if step > 2000 {
			os.output("step limit reached\n")
			break
		}
		err := et.Next(ctx)
		if err != nil {
			return "", err
		}
		if et.Done() {
			break
		}

		if os.flags.ExploreTraceRule != opt.InvalidRuleName &&
			et.LastRuleName() != os.flags.ExploreTraceRule {
			continue
		}
		newNodes := et.NewExprs()
		if os.flags.ExploreTraceSkipNoop && len(newNodes) == 0 {
			continue
		}

		if os.builder.Len() > 0 {
			os.output("\n")
		}
		os.separator("=")
		os.output("%s\n", et.LastRuleName())
		os.separator("=")
		os.output("Source expression:\n")
		os.indent(et.fo.o.FormatExpr(et.SrcExpr(), os.flags.ExprFormat))
		if len(newNodes) == 0 {
			os.output("\nNo new expressions.\n")
		}
		for i := range newNodes {
			os.output("\nNew expression %d of %d:\n", i+1, len(newNodes))
			os.indent(memo.FormatExpr(newNodes[i], os.flags.ExprFormat, et.fo.o.Memo(), os.catalog))
		}
	}
	return os.builder.String(), nil
}
