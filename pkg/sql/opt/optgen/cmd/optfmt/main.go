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
)

var (
	write = flag.Bool("w", false, "write result to (source) file instead of stdout")
	list  = flag.Bool("l", false, "list files whose formatting differs from optfmt's")
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
		s, err := prettyify(os.Stdin, defaultWidth)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(s)
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
		if *write {
			err := ioutil.WriteFile(name, []byte(prettied), 0666)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %s\n", name, err)
				os.Exit(1)
			}
		}
		if *list {
			fmt.Println(name)
		} else if !*write {
			fmt.Println(prettied)
		}
	}
}

const defaultWidth = 65

func prettyFile(name string) (orig []byte, pretty string, err error) {
	orig, err = ioutil.ReadFile(name)
	if err != nil {
		return orig, "", err
	}
	pretty, err = prettyify(bytes.NewReader(orig), defaultWidth)
	return orig, pretty, err
}

func prettyify(r io.Reader, n int) (string, error) {
	parser := lang.NewParser("")
	parser.SetFileResolver(func(name string) (io.Reader, error) {
		return r, nil
	})
	_ = parser.Parse()
	errs := parser.Errors()
	if len(errs) > 0 {
		return "", errs[0]
	}
	d := toDoc(parser.Exprs())
	s := pretty.Pretty(d, n, false, 4, nil)

	// Remove any whitespace at EOL. This can happen in define rules where
	// we always insert a blank line above comments. Since it is in a
	// nested context, a tab is inserted instead of it being just an empty
	// line.
	var sb strings.Builder
	for _, line := range strings.Split(s, "\n") {
		sb.WriteString(strings.TrimRight(line, " \t"))
		sb.WriteByte('\n')
	}
	return sb.String(), nil
}

func toDoc(exprs []lang.Expr) pretty.Doc {
	var docs []pretty.Doc
	for _, expr := range exprs {
		docs = append(docs, docExpr(expr))
	}
	return pretty.JoinDoc(
		pretty.Concat(pretty.SoftBreak, pretty.SoftBreak),
		docs...,
	)
}

func softstack(docs ...pretty.Doc) pretty.Doc {
	return pretty.Fold(concatSoftBreak, docs...)
}

func concatSoftBreak(a, b pretty.Doc) pretty.Doc {
	return pretty.Concat(
		a,
		pretty.Concat(
			pretty.SoftBreak,
			b,
		),
	)
}

func docDefine(e *lang.DefineExpr) pretty.Doc {
	var docs []pretty.Doc
	if len(e.Comments) > 0 {
		docs = append(docs, docComments(&e.Comments))
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
				docFields(e.Fields),
			)),
			pretty.Text("}"),
		)
	}
	return softstack(docs...)
}

func docComments(e *lang.CommentsExpr) pretty.Doc {
	var docs []pretty.Doc
	for _, com := range *e {
		docs = append(docs, pretty.Text(string(com)))
	}
	return softstack(docs...)
}

func docFields(e lang.DefineFieldsExpr) pretty.Doc {
	var docs []pretty.Doc
	for i, f := range e {
		if len(f.Comments) > 0 {
			if i > 0 {
				// Insert blank line above a comment. The line
				// is applied by softstack below.
				docs = append(docs, pretty.Nil)
			}
			docs = append(docs, docComments(&f.Comments))
		}
		docs = append(docs, pretty.Text(fmt.Sprintf("%s %s", f.Name, f.Type)))
	}
	return softstack(docs...)
}

func docRule(e *lang.RuleExpr) pretty.Doc {
	var docs []pretty.Doc
	if len(e.Comments) > 0 {
		docs = append(docs, docComments(&e.Comments))
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
	docs = append(docs, docExpr(e.Match))
	docs = append(docs, pretty.Text("=>"))
	docs = append(docs, docExpr(e.Replace))
	return softstack(docs...)
}

func docExpr(e lang.Expr) pretty.Doc {
	switch e := e.(type) {
	case *lang.SliceExpr:
		var docs []pretty.Doc
		for _, x := range *e {
			docs = append(docs, docExpr(x))
		}
		return pretty.Stack(docs...)
	case *lang.FuncExpr:
		name := docExpr(e.Name)
		first := pretty.Concat(
			pretty.Text("("),
			name,
		)
		if len(e.Args) == 0 {
			return pretty.Concat(first, pretty.Text(")"))
		}
		return bracket(first, docExpr(&e.Args), pretty.Text(")"), pretty.SoftBreak)
	case *lang.NamesExpr:
		var docs []pretty.Doc
		for i, x := range *e {
			d := docExpr(&x)
			if i > 0 {
				d = pretty.ConcatSpace(pretty.Text("|"), d)
			}
			docs = append(docs, d)
		}
		return pretty.NestT(pretty.Fillwords(docs...))
	case *lang.BindExpr:
		return pretty.Concat(
			pretty.Text(fmt.Sprintf("$%s:", e.Label)),
			docExpr(e.Target),
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
				docs = append(docs, docExpr(e))
			}
		}
		return pretty.Group(pretty.NestT(pretty.Join(" &", docs...)))

		//left := pretty.ConcatSpace(docExpr(e.Left), pretty.Text("&"))
		//return pretty.ConcatLine(left, docExpr(e.Right))
	case *lang.NotExpr:
		return pretty.Concat(pretty.Text("^"), docExpr(e.Input))
	case *lang.ListExpr:
		if len(e.Items) == 0 {
			return pretty.Text("[]")
		}
		return bracket(
			pretty.Text("["),
			docExpr(&e.Items),
			pretty.Text("]"),
			pretty.Line,
		)
	case *lang.NameExpr:
		return pretty.Text(string(*e))
	case *lang.NumberExpr:
		return pretty.Text(fmt.Sprint(*e))
	case *lang.RuleExpr:
		return docRule(e)
	case *lang.DefineExpr:
		return docDefine(e)
	case *lang.CommentsExpr:
		return docComments(e)
	default:
		panic(fmt.Errorf("unknown: %T)", e))
	}
}

func bracket(left, doc, right, line pretty.Doc) pretty.Doc {
	return pretty.Group(pretty.Fold(pretty.Concat,
		left,
		pretty.NestT(pretty.Concat(
			pretty.Line,
			doc,
		)),
		line,
		right,
	))
}
