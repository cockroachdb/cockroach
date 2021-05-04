// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// optfmt pretty prints .opt files.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
	"github.com/pmezard/go-difflib/difflib"
)

var (
	write   = flag.Bool("w", false, "write result to (source) file instead of stdout")
	list    = flag.Bool("l", false, "list diffs when formatting differs from optfmt's")
	verify  = flag.Bool("verify", false, "verify output order")
	exprgen = flag.Bool("e", false, "format an exprgen expression")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage of %s [flags] [path ...]:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	switch len(args) {
	case 0:
		orig, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		s, err := prettyify(bytes.NewReader(orig), defaultWidth, *exprgen)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if *verify {
			err := verifyOutput(string(orig), s)
			if err != nil {
				fmt.Fprintln(os.Stderr, s)
				fmt.Fprintf(os.Stderr, "verify failed: %s\n", err)
				os.Exit(1)
			}
		}
		fmt.Print(s)
		return
	}

	for _, name := range args {
		orig, prettied, err := prettyFile(name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", name, err)
			os.Exit(1)
		}
		if bytes.Equal(orig, []byte(prettied)) {
			continue
		}
		if *verify {
			err := verifyOutput(string(orig), prettied)
			if err != nil {
				fmt.Fprintf(os.Stderr, "verify failed: %s: %s\n", name, err)
				os.Exit(1)
			}
		}
		if *write {
			err := ioutil.WriteFile(name, []byte(prettied), 0666)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %s\n", name, err)
				os.Exit(1)
			}
		}
		if *list {
			diff := difflib.UnifiedDiff{
				A:        difflib.SplitLines(string(orig)),
				FromFile: name,
				B:        difflib.SplitLines(prettied),
				ToFile:   name,
				Context:  4,
			}
			diffText, _ := difflib.GetUnifiedDiffString(diff)
			fmt.Print(diffText)
		} else if !*write {
			fmt.Print(prettied)
		}
	}
}

func verifyOutput(orig, prettied string) error {
	origToks := toTokens(orig)
	prettyToks := toTokens(prettied)
	for i, tok := range origToks {
		if i >= len(prettyToks) {
			return fmt.Errorf("pretty ended early after %d tokens", i+1)
		}
		if prettyToks[i] != tok {
			return fmt.Errorf("token %d didn't match:\nnew: %q\norig: %q", i+1, prettyToks[i], tok)
		}
	}
	if len(prettyToks) > len(origToks) {
		return fmt.Errorf("orig ended early after %d tokens", len(origToks))
	}
	return nil
}

func toTokens(input string) []string {
	scanner := lang.NewScanner(strings.NewReader(input))
	var ret []string
	for {
		tok := scanner.Scan()
		lit := scanner.Literal()
		switch tok {
		case lang.WHITESPACE:
			// ignore
		case lang.EOF, lang.ILLEGAL, lang.ERROR:
			ret = append(ret, fmt.Sprintf("%s: %q", tok, lit))
			return ret
		default:
			ret = append(ret, fmt.Sprintf("%s: %s", tok, lit))
		}
	}
}

const defaultWidth = 65

type pp struct {
	p *lang.Parser
}

func prettyFile(name string) (orig []byte, pretty string, err error) {
	orig, err = ioutil.ReadFile(name)
	if err != nil {
		return orig, "", err
	}
	pretty, err = prettyify(bytes.NewReader(orig), defaultWidth, *exprgen)
	return orig, pretty, err
}

func prettyify(r io.Reader, n int, exprgen bool) (string, error) {
	parser := lang.NewParser("")
	parser.SetFileResolver(func(name string) (io.Reader, error) {
		if exprgen {
			return io.MultiReader(strings.NewReader(exprgenPrefix), r), nil
		}
		return r, nil
	})
	parsed := parser.Parse()
	errs := parser.Errors()
	if len(errs) > 0 {
		return "", errs[0]
	}
	p := pp{p: parser}
	var exprs []lang.Expr
	if exprgen {
		exprs = []lang.Expr{parsed.Rules[0].Replace}
	} else {
		exprs = parser.Exprs()
	}
	d := p.toDoc(exprs)
	s := pretty.Pretty(d, n, false, 4, nil)

	// Remove any whitespace at EOL. This can happen in define rules where
	// we always insert a blank line above comments which are nested with
	// a tab, or when comments force a pretty.HardLine with a pretty.Line
	// (which flattens to a space) before them.
	var sb strings.Builder
	delim := ""
	for _, line := range strings.Split(s, "\n") {
		sb.WriteString(delim)
		delim = "\n"
		// Keep comment whitespace.
		if strings.HasPrefix(line, "#") {
			sb.WriteString(line)
		} else {
			sb.WriteString(strings.TrimRight(line, " \t"))
		}
	}
	return sb.String(), nil
}

const exprgenPrefix = `
[R]
(F)
=>
`

func (p *pp) toDoc(exprs []lang.Expr) pretty.Doc {
	doc := pretty.Nil
	for _, expr := range exprs {
		var next pretty.Doc
		switch expr := expr.(type) {
		case *lang.CommentsExpr:
			next = p.docComments(expr, pretty.Nil)
		default:
			next = pretty.Concat(p.docExpr(expr), pretty.Line)
		}
		doc = pretty.ConcatDoc(doc, next, pretty.Line)
	}
	return doc
}

func softstack(docs ...pretty.Doc) pretty.Doc {
	return pretty.Fold(concatSoftBreak, docs...)
}

func concatSoftBreak(a, b pretty.Doc) pretty.Doc {
	return pretty.ConcatDoc(a, b, pretty.SoftBreak)
}

func (p *pp) docDefine(e *lang.DefineExpr) pretty.Doc {
	var docs []pretty.Doc
	for _, com := range p.p.GetComments(e) {
		docs = append(docs, pretty.Text(string(com)))
	}
	if len(e.Tags) > 0 {
		var tags []pretty.Doc
		for _, tag := range e.Tags {
			tags = append(tags, pretty.Text(string(tag)))
		}
		docs = append(docs, pretty.BracketDoc(
			pretty.Text("["),
			pretty.Join(",", tags...),
			pretty.Text("]"),
		))
	}
	name := pretty.Text(fmt.Sprintf("define %s {", e.Name))
	if len(e.Fields) == 0 {
		docs = append(docs, name, pretty.Text("}"))
	} else {
		name = pretty.Concat(name, pretty.SoftBreak)
		docs = append(docs,
			pretty.NestT(pretty.Concat(
				name,
				p.docFields(e.Fields),
			)),
			pretty.Text("}"),
		)
	}
	return softstack(docs...)
}

func (p *pp) docComments(e *lang.CommentsExpr, start pretty.Doc) pretty.Doc {
	doc := start
	for _, com := range *e {
		doc = pretty.Fold(pretty.Concat,
			doc,
			pretty.Text(string(com)),
			pretty.HardLine,
		)
	}
	return doc
}

func (p *pp) docFields(e lang.DefineFieldsExpr) pretty.Doc {
	var docs []pretty.Doc
	for fi, f := range e {
		for comi, com := range p.p.GetComments(f) {
			if fi > 0 && comi == 0 {
				// Insert blank line above a comment.
				docs = append(docs, pretty.Text(""))
			}
			docs = append(docs, pretty.Text(string(com)))
		}
		docs = append(docs, pretty.Text(fmt.Sprintf("%s %s", f.Name, f.Type)))
	}
	return softstack(docs...)
}

func (p *pp) docRule(e *lang.RuleExpr) pretty.Doc {
	var docs []pretty.Doc
	for _, com := range p.p.GetComments(e) {
		docs = append(docs, pretty.Text(string(com)))
	}
	tags := []pretty.Doc{pretty.Text(string(e.Name))}
	for _, tag := range e.Tags {
		tags = append(tags, pretty.Text(string(tag)))
	}
	docs = append(docs, pretty.BracketDoc(
		pretty.Text("["),
		pretty.Join(",", tags...),
		pretty.Text("]"),
	))
	for _, com := range p.p.GetComments(e.Match) {
		docs = append(docs, pretty.Text(string(com)))
	}
	docs = append(docs, p.docOnlyExpr(e.Match))
	docs = append(docs, pretty.Text("=>"))
	for _, com := range p.p.GetComments(e.Replace) {
		docs = append(docs, pretty.Text(string(com)))
	}
	docs = append(docs, p.docOnlyExpr(e.Replace))
	return softstack(docs...)
}

func (p *pp) docExpr(e lang.Expr) pretty.Doc {
	doc := p.docOnlyExpr(e)
	// These expressions add their own comments.
	switch e.(type) {
	case *lang.DefineExpr, *lang.RuleExpr:
		return doc
	}
	if coms := p.p.GetComments(e); len(coms) > 0 {
		doc = pretty.Fold(pretty.Concat,
			p.docComments(&coms, pretty.Line),
			doc,
		)
	}
	return doc
}

// docSlice returns a doc for a SliceExpr. Its first doc is guaranteed to be a
// Line or HardLine.
func (p *pp) docSlice(e lang.SliceExpr) pretty.Doc {
	doc := pretty.Nil
	for _, x := range e {
		coms := p.p.GetComments(x)
		if len(coms) > 0 {
			doc = pretty.Concat(doc, p.docComments(&coms, pretty.Line))
		} else {
			doc = pretty.Concat(doc, pretty.Line)
		}
		doc = pretty.Concat(doc, p.docOnlyExpr(x))
	}
	return doc
}

func (p *pp) docOnlyExpr(e lang.Expr) pretty.Doc {
	switch e := e.(type) {
	case *lang.ListExpr:
		if len(e.Items) == 0 {
			return pretty.Text("[]")
		}
		return pretty.Group(pretty.Fold(pretty.Concat,
			pretty.Text("["),
			pretty.NestT(p.docSlice(e.Items)),
			pretty.Line,
			pretty.Text("]"),
		))
	case *lang.FuncExpr:
		docs := []pretty.Doc{
			pretty.Text("("),
			p.docExpr(e.Name),
			p.docSlice(e.Args),
		}
		return pretty.Group(concatSoftBreak(
			pretty.NestT(pretty.Fold(pretty.Concat, docs...)),
			pretty.Text(")"),
		))
	case *lang.NamesExpr:
		var docs []pretty.Doc
		for i, x := range *e {
			d := p.docExpr(&x)
			if i > 0 {
				d = pretty.ConcatSpace(pretty.Text("|"), d)
			}
			docs = append(docs, d)
		}
		return pretty.NestT(pretty.Fillwords(docs...))
	case *lang.BindExpr:
		return pretty.Concat(
			pretty.Text(fmt.Sprintf("$%s:", e.Label)),
			p.docExpr(e.Target),
		)
	case *lang.LetExpr:
		var docs []pretty.Doc
		docs = append(docs, pretty.SoftBreak)
		for i, l := range e.Labels {
			docs = append(docs, pretty.Text(fmt.Sprintf("$%s", l)))
			if i < len(e.Labels)-1 {
				docs = append(docs, pretty.Line)
			}
		}

		labels := pretty.Group(pretty.Fold(pretty.Concat,
			pretty.Text("("),
			pretty.NestT(pretty.Fold(pretty.Concat, docs...)),
			pretty.SoftBreak,
		))

		binding := pretty.Group(pretty.Fold(pretty.Concat,
			labels,
			pretty.Text("):"),
			p.docExpr(e.Target),
		))

		inner := pretty.Group(pretty.Fold(pretty.Concat,
			binding,
			pretty.Line,
			p.docExpr(e.Result),
		))

		return pretty.BracketDoc(
			pretty.Text("(Let "),
			inner,
			pretty.Text(")"),
		)
	case *lang.AnyExpr:
		return pretty.Text("*")
	case *lang.ListAnyExpr:
		return pretty.Text("...")
	case *lang.RefExpr:
		return pretty.Text(fmt.Sprintf("$%s", e.Label))
	case *lang.AndExpr:
		// In order to prevent nested And expressions, find all of the
		// adjacent Ands and join them.
		var docs []pretty.Doc
		check := []lang.Expr{e.Left, e.Right}
		for len(check) > 0 {
			e := check[0]
			check = check[1:]
			if and, ok := e.(*lang.AndExpr); ok {
				// Append to front to preserve order.
				check = append([]lang.Expr{and.Left, and.Right}, check...)
			} else {
				docs = append(docs, p.docExpr(e))
			}
		}
		return pretty.Group(pretty.NestT(pretty.Join(" &", docs...)))
	case *lang.NotExpr:
		return pretty.Concat(pretty.Text("^"), p.docExpr(e.Input))
	case *lang.NameExpr:
		return pretty.Text(string(*e))
	case *lang.NumberExpr:
		return pretty.Text(fmt.Sprint(*e))
	case *lang.RuleExpr:
		return p.docRule(e)
	case *lang.DefineExpr:
		return p.docDefine(e)
	case *lang.CommentsExpr:
		return p.docComments(e, pretty.Line)
	case *lang.StringExpr:
		return pretty.Text(fmt.Sprintf(`"%s"`, string(*e)))
	default:
		panic(fmt.Errorf("unknown: %T)", e))
	}
}
