// Copyright 2015 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

// This file contains the code to process sources, to be able to deduct the
// original types.

package stack

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
)

// Private stuff.

// cacheAST is a cache of parsed Go sources.
type cacheAST struct {
	files  map[string][]byte
	parsed map[string]*parsedFile
}

// augmentGoroutine processes source files to improve call to be more
// descriptive.
//
// It modifies the routine.
func (c *cacheAST) augmentGoroutine(g *Goroutine) error {
	var err error
	for i, call := range g.Stack.Calls {
		// Only load the AST if there's an argument to process.
		if len(call.Args.Values) == 0 {
			continue
		}
		if err1 := c.loadFile(g.Stack.Calls[i].LocalSrcPath); err1 != nil {
			//log.Printf("%s", err)
			err = err1
		}
		if p := c.parsed[call.LocalSrcPath]; p != nil {
			f, err1 := p.getFuncAST(call.Func.Name, call.Line)
			if err1 != nil {
				err = err1
				continue
			}
			if f != nil {
				augmentCall(&g.Stack.Calls[i], f)
			}
		}
	}
	return err
}

// loadFile loads a Go source file and parses the AST tree.
func (c *cacheAST) loadFile(fileName string) error {
	if fileName == "" {
		return nil
	}
	if _, ok := c.parsed[fileName]; ok {
		return nil
	}
	// Do not attempt to parse the same file twice.
	c.parsed[fileName] = nil

	if !strings.HasSuffix(fileName, ".go") {
		// Ignore C and assembly.
		return fmt.Errorf("cannot load non-go file %q", fileName)
	}
	src, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	c.files[fileName] = src
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, fileName, src, 0)
	if err != nil {
		return fmt.Errorf("failed to parse "+wrap, err)
	}
	c.parsed[fileName] = &parsedFile{
		lineToByteOffset: lineToByteOffsets(src),
		parsed:           parsed,
	}
	return nil
}

// lineToByteOffsets extract the line number into raw file offset.
//
// Inserts a dummy 0 at offset 0 so line offsets can be 1 based.
func lineToByteOffsets(src []byte) []int {
	offsets := []int{0, 0}
	for offset := 0; offset < len(src); {
		n := bytes.IndexByte(src[offset:], '\n')
		if n == -1 {
			break
		}
		offset += n + 1
		offsets = append(offsets, offset)
	}
	return offsets
}

// parsedFile is a processed Go source file.
type parsedFile struct {
	lineToByteOffset []int
	parsed           *ast.File
}

// getFuncAST gets the callee site function AST representation for the code
// inside the function f at line l.
func (p *parsedFile) getFuncAST(f string, l int) (d *ast.FuncDecl, err error) {
	if len(p.lineToByteOffset) <= l {
		// The line number in the stack trace line does not exist in the file. That
		// can only mean that the sources on disk do not match the sources used to
		// build the binary.
		return nil, fmt.Errorf("line %d is over line count of %d", l, len(p.lineToByteOffset)-1)
	}

	// Walk the AST to find the lineToByteOffset that fits the line number.
	var lastFunc *ast.FuncDecl
	// Inspect() goes depth first. This means for example that a function like:
	// func a() {
	//   b := func() {}
	//   c()
	// }
	//
	// Were we are looking at the c() call can return confused values. It is
	// important to look at the actual ast.Node hierarchy.
	ast.Inspect(p.parsed, func(n ast.Node) bool {
		if d != nil {
			return false
		}
		if n == nil {
			return true
		}
		if int(n.Pos()) >= p.lineToByteOffset[l] {
			// We are expecting a ast.CallExpr node. It can be harder to figure out
			// when there are multiple calls on a single line, as the stack trace
			// doesn't have file byte offset information, only line based.
			// gofmt will always format to one function call per line but there can
			// be edge cases, like:
			//   a = A{Foo(), Bar()}
			d = lastFunc
			//p.processNode(call, n)
			return false
		} else if f, ok := n.(*ast.FuncDecl); ok {
			lastFunc = f
		}
		return true
	})
	return
}

func name(n ast.Node) string {
	switch t := n.(type) {
	case *ast.InterfaceType:
		return "interface{}"
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	case *ast.StarExpr:
		return "*" + name(t.X)
	case *ast.BasicLit:
		return t.Value
	case *ast.Ellipsis:
		return "..."
	default:
		return "<unknown>"
	}
}

// fieldToType returns the type name and whether if it's an ellipsis.
func fieldToType(f *ast.Field) (string, bool) {
	switch arg := f.Type.(type) {
	case *ast.ArrayType:
		if arg.Len != nil {
			// Array.
			return "[" + name(arg.Len) + "]" + name(arg.Elt), false
		}
		// Slice.
		return "[]" + name(arg.Elt), false
	case *ast.Ellipsis:
		return name(arg.Elt), true
	case *ast.FuncType:
		// Do not print the function signature to not overload the trace.
		return "func", false
	case *ast.Ident:
		return arg.Name, false
	case *ast.InterfaceType:
		return "interface{}", false
	case *ast.SelectorExpr:
		return arg.Sel.Name, false
	case *ast.StarExpr:
		return "*" + name(arg.X), false
	case *ast.MapType:
		return fmt.Sprintf("map[%s]%s", name(arg.Key), name(arg.Value)), false
	case *ast.ChanType:
		return fmt.Sprintf("chan %s", name(arg.Value)), false
	default:
		// TODO(maruel): Implement anything missing.
		return "<unknown>", false
	}
}

// extractArgumentsType returns the name of the type of each input argument.
func extractArgumentsType(f *ast.FuncDecl) ([]string, bool) {
	var fields []*ast.Field
	if f.Recv != nil {
		if len(f.Recv.List) != 1 {
			panic("Expect only one receiver; please fix panicparse's code")
		}
		// If it is an object receiver (vs a pointer receiver), its address is not
		// printed in the stack trace so it needs to be ignored.
		if _, ok := f.Recv.List[0].Type.(*ast.StarExpr); ok {
			fields = append(fields, f.Recv.List[0])
		}
	}
	var types []string
	ellipsis := false
	for _, arg := range append(fields, f.Type.Params.List...) {
		// Assert that ellipsis is only set on the last item of fields?
		var t string
		t, ellipsis = fieldToType(arg)
		mult := len(arg.Names)
		if mult == 0 {
			mult = 1
		}
		for i := 0; i < mult; i++ {
			types = append(types, t)
		}
	}
	return types, ellipsis
}

// augmentCall walks the function and populate call accordingly.
func augmentCall(call *Call, f *ast.FuncDecl) {
	flatArgs := make([]*Arg, 0, len(call.Args.Values))
	call.Args.walk(func(arg *Arg) {
		flatArgs = append(flatArgs, arg)
	})

	pop := func() *Arg {
		if len(flatArgs) == 0 {
			return nil
		}
		a := flatArgs[0]
		flatArgs = flatArgs[1:]
		return a
	}
	popFmt := func(fmtFn func(v uint64) string) string {
		a := pop()
		if a == nil {
			return "<nil>"
		}
		if a.IsOffsetTooLarge {
			return "_"
		}
		return fmtFn(a.Value)
	}
	popName := func() string {
		a := pop()
		if a == nil {
			return "<nil>"
		}
		if len(a.Name) != 0 {
			return a.Name
		}
		if a.IsOffsetTooLarge {
			return "_"
		}
		return fmt.Sprintf("0x%x", a.Value)
	}

	types, extra := extractArgumentsType(f)
	for i := 0; len(flatArgs) != 0; i++ {
		var t string
		if i >= len(types) {
			if !extra {
				// These are unexpected value! Print them as hex.
				call.Args.Processed = append(call.Args.Processed, popName())
				continue
			}
			t = types[len(types)-1]
		} else {
			t = types[i]
		}
		var str string
		switch t {
		case "float32":
			str = popFmt(func(v uint64) string {
				f := float64(math.Float32frombits(uint32(v)))
				return strconv.FormatFloat(f, 'g', -1, 32)
			})
		case "float64":
			str = popFmt(func(v uint64) string {
				f := math.Float64frombits(v)
				return strconv.FormatFloat(f, 'g', -1, 64)
			})
		case "int":
			str = popFmt(func(v uint64) string {
				return strconv.FormatInt(int64(int(v)), 10)
			})
		case "int8":
			str = popFmt(func(v uint64) string {
				return strconv.FormatInt(int64(int8(v)), 10)
			})
		case "int16":
			str = popFmt(func(v uint64) string {
				return strconv.FormatInt(int64(int16(v)), 10)
			})
		case "int32":
			str = popFmt(func(v uint64) string {
				return strconv.FormatInt(int64(int32(v)), 10)
			})
		case "int64":
			str = popFmt(func(v uint64) string {
				return strconv.FormatInt(int64(v), 10)
			})
		case "uint", "uint8", "uint16", "uint32", "uint64":
			str = popFmt(func(v uint64) string {
				return strconv.FormatUint(v, 10)
			})
		case "bool":
			str = popFmt(func(v uint64) string {
				if v == 0 {
					return "false"
				}
				return "true"
			})
		case "string":
			name := popName()
			lenStr := popFmt(func(v uint64) string {
				return strconv.FormatUint(v, 10)
			})
			str = fmt.Sprintf("%s(%s, len=%s)", t, name, lenStr)
		default:
			if strings.HasPrefix(t, "*") {
				str = fmt.Sprintf("%s(%s)", t, popName())
			} else if strings.HasPrefix(t, "[]") {
				name := popName()
				lenStr := popFmt(func(v uint64) string {
					return strconv.FormatUint(v, 10)
				})
				capStr := popFmt(func(v uint64) string {
					return strconv.FormatUint(v, 10)
				})
				str = fmt.Sprintf("%s(%s len=%s cap=%s)", t, name, lenStr, capStr)
			} else {
				if i < len(call.Args.Values) && call.Args.Values[i].IsAggregate {
					// If top-level argument is an aggregate-type, include each
					// of its sub-arguments.
					v := &call.Args.Values[i].Fields
					var fields []string
					v.walk(func(arg *Arg) {
						fields = append(fields, popName())
					})
					if v.Elided {
						fields = append(fields, "...")
					}
					str = fmt.Sprintf("%s{%s}", t, strings.Join(fields, ", "))
				} else {
					// Assumes it's an interface. For now, discard the object
					// value, which is probably not a good idea.
					str = fmt.Sprintf("%s(%s)", t, popName())
					pop()
				}
			}
		}
		call.Args.Processed = append(call.Args.Processed, str)
	}
}
