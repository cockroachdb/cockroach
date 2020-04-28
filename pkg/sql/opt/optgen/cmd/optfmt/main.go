package main

import (
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
)

func main() {
	var f io.Reader
	var err error
	switch len(os.Args) {
	case 1:
		f = os.Stdin
	case 2:
		f, err = os.Open(os.Args[1])
	default:
		fmt.Fprintf(os.Stderr, "usage: %s [path (defaults to stdin if omitted)]\n", os.Args[0])
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	s, err := prettyify(f, 80)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(s)
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
	d := to_doc(parser.Exprs())
	s := pretty.Pretty(d, n, true, 4, nil)
	return s, nil
}

func to_doc(exprs []lang.Expr) pretty.Doc {
	var docs []pretty.Doc
	for _, expr := range exprs {
		docs = append(docs, doc_expr(expr))
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

func doc_define(e *lang.DefineExpr) pretty.Doc {
	var docs []pretty.Doc
	if len(e.Comments) > 0 {
		docs = append(docs, doc_comments(&e.Comments))
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
				doc_fields(e.Fields),
			)),
			pretty.Text("}"),
		)
	}
	return softstack(docs...)
}

func doc_comments(e *lang.CommentsExpr) pretty.Doc {
	var docs []pretty.Doc
	for _, com := range *e {
		docs = append(docs, pretty.Text(string(com)))
	}
	return softstack(docs...)
}

func doc_fields(e lang.DefineFieldsExpr) pretty.Doc {
	var docs []pretty.Doc
	for _, f := range e {
		if len(f.Comments) > 0 {
			docs = append(docs, doc_comments(&f.Comments))
		}
		docs = append(docs, pretty.Text(fmt.Sprintf("%s %s", f.Name, f.Type)))
	}
	return softstack(docs...)
}

func doc_rule(e *lang.RuleExpr) pretty.Doc {
	var docs []pretty.Doc
	if len(e.Comments) > 0 {
		docs = append(docs, doc_comments(&e.Comments))
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
	docs = append(docs, doc_expr(e.Match))
	docs = append(docs, pretty.Text("=>"))
	docs = append(docs, doc_expr(e.Replace))
	return softstack(docs...)
}

func doc_expr(e lang.Expr) pretty.Doc {
	switch e := e.(type) {
	case *lang.SliceExpr:
		var docs []pretty.Doc
		for _, x := range *e {
			docs = append(docs, doc_expr(x))
		}
		return pretty.Stack(docs...)
	case *lang.FuncExpr:
		name := doc_expr(e.Name)
		first := pretty.Concat(
			pretty.Text("("),
			name,
		)
		if len(e.Args) == 0 {
			return pretty.Concat(first, pretty.Text(")"))
		}
		return bracket(first, doc_expr(&e.Args), pretty.Text(")"))
	case *lang.NamesExpr:
		var docs []pretty.Doc
		for _, x := range *e {
			docs = append(docs, pretty.Text(string(x)))
		}
		return pretty.JoinDoc(
			pretty.Text(" | "),
			docs...,
		)
	case *lang.BindExpr:
		return pretty.Concat(
			pretty.Text(fmt.Sprintf("$%s:", e.Label)),
			doc_expr(e.Target),
		)
	case *lang.AnyExpr:
		return pretty.Text("*")
	case *lang.ListAnyExpr:
		return pretty.Text("...")
	case *lang.RefExpr:
		return pretty.Text(fmt.Sprintf("$%s", e.Label))
	case *lang.AndExpr:
		left := pretty.ConcatSpace(doc_expr(e.Left), pretty.Text("&"))
		return pretty.NestT(pretty.ConcatLine(left, doc_expr(e.Right)))
	case *lang.NotExpr:
		return pretty.Concat(pretty.Text("^"), doc_expr(e.Input))
	case *lang.ListExpr:
		return bracket(
			pretty.Text("["),
			doc_expr(&e.Items),
			pretty.Text("]"),
		)
	case *lang.NameExpr:
		return pretty.Text(string(*e))
	case *lang.NumberExpr:
		return pretty.Text(fmt.Sprint(*e))
	case *lang.RuleExpr:
		return doc_rule(e)
	case *lang.DefineExpr:
		return doc_define(e)
	case *lang.CommentsExpr:
		return doc_comments(e)
	default:
		panic(fmt.Errorf("unknown: %T)", e))
	}
}

func bracket(left, doc, right pretty.Doc) pretty.Doc {
	return pretty.Group(pretty.Fold(pretty.Concat,
		left,
		pretty.NestT(pretty.Concat(
			pretty.Line,
			doc,
		)),
		pretty.SoftBreak,
		right,
	))
}
