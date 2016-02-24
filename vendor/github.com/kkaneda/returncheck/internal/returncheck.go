package returncheck

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"strings"

	"golang.org/x/tools/go/loader"
)

// Run loads specified packages and checks if there is any unchecked return
// of the target type (in the target package).
func Run(pkgs []string, targetPkg, targetTypeName string) error {
	targetType, err := loadTargetType(targetPkg, targetTypeName)
	if err != nil {
		return err
	}

	prog, err := loadProgram(pkgs)
	if err != nil {
		return err
	}

	foundUnchecked := false
	for _, pkgInfo := range prog.InitialPackages() {
		for _, f := range pkgInfo.Files {
			v := &visitor{
				prog:       prog,
				pkgInfo:    pkgInfo,
				targetType: targetType,
			}
			ast.Walk(v, f)
			if v.foundUnchecked {
				foundUnchecked = true
			}
		}
	}
	if foundUnchecked {
		return fmt.Errorf("found an unchecked return value")
	}

	return nil
}

// loadTargeType loads the target type in the specified package.
func loadTargetType(pkgName, typeName string) (types.Type, error) {
	prog, err := loadProgram([]string{pkgName})
	if err != nil {
		return nil, err
	}
	for _, pkgInfo := range prog.InitialPackages() {
		if t := pkgInfo.Pkg.Scope().Lookup(typeName); t != nil {
			return t.Type(), nil
		}
	}
	return nil, fmt.Errorf("could not find %s in %s", typeName, pkgName)
}

func loadProgram(args []string) (*loader.Program, error) {
	var conf loader.Config
	rest, err := conf.FromArgs(args, true)
	if err != nil {
		return nil, fmt.Errorf("could not parse arguments: %s", err)
	}
	if len(rest) > 0 {
		return nil, fmt.Errorf("unhandled extra arguments: %v", rest)
	}

	prog, err := conf.Load()
	if err != nil {
		return nil, fmt.Errorf("could not load: %s", err)
	}
	return prog, nil
}

// visitor stores information needs to be accessed via Visit.
type visitor struct {
	prog    *loader.Program
	pkgInfo *loader.PackageInfo
	// targetType is a type to be checked.
	targetType     types.Type
	foundUnchecked bool
}

// Visit implements the ast.Vistor interface.
func (v *visitor) Visit(node ast.Node) ast.Visitor {
	switch stmt := node.(type) {
	case *ast.ExprStmt:
		if call, ok := stmt.X.(*ast.CallExpr); ok {
			v.recordUnchecked(call, -1)
		}
	case *ast.GoStmt:
		v.recordUnchecked(stmt.Call, -1)
	case *ast.DeferStmt:
		v.recordUnchecked(stmt.Call, -1)
	case *ast.AssignStmt:
		if len(stmt.Lhs) != 1 || len(stmt.Rhs) != 1 {
			// Find "_" in the left-hand side of the assigments and check if the corressponding
			// right-hand side expression is a call that returns the target type.
			for i := 0; i < len(stmt.Lhs); i++ {
				if id, ok := stmt.Lhs[i].(*ast.Ident); ok && id.Name == "_" {
					var rhs ast.Expr
					if len(stmt.Rhs) == 1 {
						// ..., stmt.Lhs[i], ... := stmt.Rhs[0]
						rhs = stmt.Rhs[0]
					} else {
						// ..., stmt.Lhs[i], ... := ..., stmt.Rhs[i], ...
						rhs = stmt.Rhs[i]
					}
					if call, ok := rhs.(*ast.CallExpr); ok {
						v.recordUnchecked(call, i)
					}
				}
			}
		}
	}
	return v
}

// recordUnchecked records an error if a given calls has an unchecked
// return. If pos is not a negative value and the call returns a
// tuple, check if the return value at the specified position has a
// target type.
func (v *visitor) recordUnchecked(call *ast.CallExpr, pos int) {
	isTarget := false
	switch t := v.pkgInfo.Types[call].Type.(type) {
	case *types.Named:
		isTarget = v.isTargetType(t)
	case *types.Pointer:
		isTarget = v.isTargetType(t.Elem())
	case *types.Tuple:
		for i := 0; i < t.Len(); i++ {
			if pos >= 0 && i != pos {
				continue
			}
			switch et := t.At(i).Type().(type) {
			case *types.Named:
				isTarget = v.isTargetType(et)
			case *types.Pointer:
				isTarget = v.isTargetType(et.Elem())
			}
		}
	}

	if isTarget {
		pos := v.prog.Fset.Position(call.Lparen)
		line := readLineAt(pos.Filename, pos.Line)
		fmt.Printf("%s\t%s\n", pos, line)
		v.foundUnchecked = true
	}
}

// isTargetType returns true if a given type is identical to the target
// types to be checked.
func (v *visitor) isTargetType(t types.Type) bool {
	// Cannot use types.Identical since t and targetType are
	// obtained from different programs(?).
	return t.String() == v.targetType.String()
}

// readLineAt returns the content of the file at the specified line number.
// Returns "???" when an error is found.
func readLineAt(filename string, line int) string {
	var f, err = os.Open(filename)
	if err != nil {
		return "???"
	}
	var scanner = bufio.NewScanner(f)
	i := 1
	for scanner.Scan() {
		if i == line {
			return strings.TrimSpace(scanner.Text())
		}
		i++
	}
	return "???"
}
