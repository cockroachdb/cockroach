// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/token"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/kisielk/gotool"
	"golang.org/x/tools/go/loader"
	"go/types"
)

var (
	reportExported = flag.Bool("e", false, "Report exported variables and constants")
)

type object struct {
	pkgPath string
	name    string
}

type visitor struct {
	prog       *loader.Program
	pkg        *loader.PackageInfo
	uses       map[object]int
	positions  map[object]token.Position
	insideFunc bool
}

func getKey(obj types.Object) object {
	if obj == nil {
		return object{}
	}

	pkg := obj.Pkg()
	pkgPath := ""
	if pkg != nil {
		pkgPath = pkg.Path()
	}

	return object{
		pkgPath: pkgPath,
		name:    obj.Name(),
	}
}

func (v *visitor) decl(obj types.Object) {
	key := getKey(obj)
	if _, ok := v.uses[key]; !ok {
		v.uses[key] = 0
	}
	if _, ok := v.positions[key]; !ok {
		v.positions[key] = v.prog.Fset.Position(obj.Pos())
	}
}

func (v *visitor) use(obj types.Object) {
	key := getKey(obj)
	if _, ok := v.uses[key]; ok {
		v.uses[key]++
	} else {
		v.uses[key] = 1
	}
}

func isReserved(name string) bool {
	return name == "_" || strings.HasPrefix(strings.ToLower(name), "_cgo_")
}

func (v *visitor) Visit(node ast.Node) ast.Visitor {
	switch node := node.(type) {
	case *ast.Ident:
		v.use(v.pkg.Info.Uses[node])

	case *ast.ValueSpec:
		if !v.insideFunc {
			for _, ident := range node.Names {
				if !isReserved(ident.Name) {
					v.decl(v.pkg.Info.Defs[ident])
				}
			}
		}
		for _, val := range node.Values {
			ast.Walk(v, val)
		}
		return nil

	case *ast.FuncDecl:
		if node.Body != nil {
			v.insideFunc = true
			ast.Walk(v, node.Body)
			v.insideFunc = false
		}

		return nil
	}

	return v
}

func main() {
	flag.Parse()
	exitStatus := 0
	importPaths := gotool.ImportPaths(flag.Args())
	if len(importPaths) == 0 {
		importPaths = []string{"."}
	}

	ctx := build.Default
	loadcfg := loader.Config{
		Build: &ctx,
	}
	rest, err := loadcfg.FromArgs(importPaths, true)
	if err != nil {
		log.Fatalf("could not parse arguments: %s", err)
	}
	if len(rest) > 0 {
		log.Fatalf("unhandled extra arguments: %v", rest)
	}

	program, err := loadcfg.Load()
	if err != nil {
		log.Fatalf("could not type check: %s", err)
	}

	uses := make(map[object]int)
	positions := make(map[object]token.Position)

	for _, pkgInfo := range program.InitialPackages() {
		if pkgInfo.Pkg.Path() == "unsafe" {
			continue
		}

		v := &visitor{
			prog:      program,
			pkg:       pkgInfo,
			uses:      uses,
			positions: positions,
		}

		for _, f := range v.pkg.Files {
			ast.Walk(v, f)
		}
	}

	var lines []string

	for obj, useCount := range uses {
		if useCount == 0 && (*reportExported || !ast.IsExported(obj.name)) {
			pos := positions[obj]
			lines = append(lines, fmt.Sprintf("%s: %s:%d:%d: %s", obj.pkgPath, pos.Filename, pos.Line, pos.Column, obj.name))
			exitStatus = 1
		}
	}

	sort.Strings(lines)
	for _, line := range lines {
		fmt.Println(line)
	}

	os.Exit(exitStatus)
}
