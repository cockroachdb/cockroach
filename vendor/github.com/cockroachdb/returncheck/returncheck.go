package returncheck

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Run loads specified packages and checks if there is any unchecked return
// of the target type (in the target package).
func Run(pkgNames []string, targetPkg, targetTypeName string) error {
	config := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
	}
	targetType, err := loadTargetType(config, targetPkg, targetTypeName)
	if err != nil {
		return err
	}

	pkgs, err := packages.Load(config, pkgNames...)
	if err != nil {
		return fmt.Errorf("could not load: %s", err)
	}

	foundUnchecked := false
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	for _, pkg := range pkgs {
		for _, f := range pkg.Syntax {
			v := &visitor{
				pwd:        wd,
				pkg:        pkg,
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
func loadTargetType(config *packages.Config, pkgName, typeName string) (types.Type, error) {
	pkgs, err := packages.Load(config, pkgName)
	if err != nil {
		return nil, err
	}
	for _, pkg := range pkgs {
		if t := pkg.Types.Scope().Lookup(typeName); t != nil {
			return t.Type(), nil
		}
	}
	return nil, fmt.Errorf("could not find %s in %s", typeName, pkgName)
}

// visitor stores information needs to be accessed via Visit.
type visitor struct {
	pkg *packages.Package
	// targetType is a type to be checked.
	targetType     types.Type
	foundUnchecked bool
	pwd            string
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
	switch t := v.pkg.TypesInfo.Types[call].Type.(type) {
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
		pos := v.pkg.Fset.Position(call.Lparen)
		if file, err := filepath.Rel(v.pwd, pos.Filename); err == nil {
			// Make the output relative to the pwd.
			pos.Filename = file
		}
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
