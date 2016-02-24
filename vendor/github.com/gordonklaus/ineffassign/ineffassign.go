package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

var (
	root            string
	dontRecurseFlag = flag.Bool("n", false, "don't recursively check paths")
)

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		fmt.Println("missing argument: filepath")
		return
	}

	var err error
	root, err = filepath.Abs(flag.Arg(0))
	if err != nil {
		fmt.Printf("Error finding absolute path :%s", err)
		return
	}

	errors := 0
	filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error during filesystem walk: %v\n", err)
			return nil
		}
		if fi.IsDir() {
			if *dontRecurseFlag && path != root || filepath.Base(path) == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		errors += checkPath(path)
		return nil
	})
	if errors > 0 {
		os.Exit(1)
	}
}

func checkPath(path string) (errors int) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return errors
	}

	chk := checker{}
	// Package-level vars are exempt.
	for _, d := range f.Decls {
		if d, ok := d.(*ast.GenDecl); ok && d.Tok == token.VAR {
			for _, s := range d.Specs {
				for _, id := range s.(*ast.ValueSpec).Names {
					chk.info(id).escapes = true
				}
			}
		}
	}
	ast.Walk(chk, f)
	for _, i := range chk {
		if !i.escapes && i.assign > i.use {
			fmt.Printf("%s: %s assigned and not used\n", fset.Position(i.id.Pos()), i.id.Name)
			errors++
		}
	}
	return errors
}

type checker map[*ast.Object]*info

type info struct {
	id            *ast.Ident
	assign, use   token.Pos
	loop, funclit ast.Node
	escapes       bool
}

func (chk checker) Visit(n ast.Node) ast.Visitor {
	switch n := n.(type) {
	case *ast.GenDecl:
		if n.Tok == token.VAR {
			for _, s := range n.Specs {
				for _, id := range s.(*ast.ValueSpec).Names {
					chk.info(id)
				}
			}
		}
	case *ast.AssignStmt:
		for _, x := range n.Rhs {
			ast.Walk(chk, x)
		}
		for _, x := range n.Lhs {
			if id, ok := unparen(x).(*ast.Ident); ok {
				chk.info(id).assign = n.End()
			} else {
				ast.Walk(chk, x)
			}
		}
		return nil
	case *ast.BranchStmt:
		if n.Tok == token.GOTO {
			for _, i := range chk {
				if i.use > n.Label.Obj.Decl.(*ast.LabeledStmt).Pos() && i.use < n.Pos() {
					i.use = n.Pos()
				}
			}
		}
	case *ast.FuncDecl:
		if n.Recv != nil {
			for _, id := range n.Recv.List[0].Names {
				chk.info(id)
			}
		}
	case *ast.FuncType:
		if n.Params != nil {
			for _, f := range n.Params.List {
				for _, id := range f.Names {
					chk.info(id)
				}
			}
		}
		if n.Results != nil {
			for _, f := range n.Results.List {
				for _, id := range f.Names {
					chk.info(id).escapes = true
				}
			}
		}
		return nil
	case *ast.Ident:
		i := chk.info(n)
		i.use = n.Pos()
		if i.funclit != nil {
			i.escapes = true
		}
		if i.loop != nil {
			i.use = i.loop.End()
		}
	case *ast.UnaryExpr:
		if id, ok := unparen(n.X).(*ast.Ident); ok && n.Op == token.AND {
			chk.info(id).escapes = true
		}
	case *ast.SelectorExpr:
		// A method call (possibly delayed via a method value) might implicitly take
		// the address of its receiver, causing it to escape.
		// We can't do any better here without knowing the variable's type.
		if id, ok := unparen(n.X).(*ast.Ident); ok {
			chk.info(id).escapes = true
		}
	case *ast.ForStmt:
		walk(chk, n.Init)
		chk.loop(n, true)
		walk(chk, n.Cond)
		walk(chk, n.Post)
		walk(chk, n.Body)
		chk.loop(n, false)
		return nil
	case *ast.RangeStmt:
		walk(chk, n.X)
		chk.loop(n, true)
		if n.Key != nil {
			lhs := []ast.Expr{n.Key}
			if n.Value != nil {
				lhs = append(lhs, n.Value)
			}
			walk(chk, &ast.AssignStmt{Lhs: lhs, Tok: n.Tok, TokPos: n.TokPos, Rhs: []ast.Expr{&ast.Ident{NamePos: n.X.End()}}})
		}
		walk(chk, n.Body)
		chk.loop(n, false)
		return nil
	case *ast.FuncLit:
		walk(chk, n.Type)
		chk.funclit(n, true)
		walk(chk, n.Body)
		chk.funclit(n, false)
		return nil
	}
	return chk
}

func (chk checker) info(id *ast.Ident) *info {
	i := chk[id.Obj]
	if i == nil {
		i = &info{}
		if id.Obj != nil && id.Name != "_" {
			chk[id.Obj] = i
		}
	}
	i.id = id
	return i
}

func (chk checker) loop(loop ast.Node, enter bool) {
	for _, i := range chk {
		if enter && i.loop == nil {
			i.loop = loop
		}
		if !enter && i.loop == loop {
			i.loop = nil
		}
	}
}

func (chk checker) funclit(funclit ast.Node, enter bool) {
	for _, i := range chk {
		if enter && i.funclit == nil {
			i.funclit = funclit
		}
		if !enter && i.funclit == funclit {
			i.funclit = nil
		}
	}
}

func walk(v ast.Visitor, n ast.Node) {
	if n != nil {
		ast.Walk(v, n)
	}
}

func unparen(x ast.Expr) ast.Expr {
	if p, ok := x.(*ast.ParenExpr); ok {
		return unparen(p.X)
	}
	return x
}
