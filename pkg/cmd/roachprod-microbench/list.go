// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"fmt"
	"go/ast"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/errors"
	"golang.org/x/tools/go/packages"
)

type (
	BenchmarkInfo struct {
		Name string
		Team string
		Args RunArgs
	}

	RunArgs struct {
		Suite     string
		Timeout   time.Duration
		Count     int
		BenchTime string
	}
)

const docMarker = "benchmark-ci:"

func listPackages(pkgDir string, patterns ...string) ([]*packages.Package, error) {
	pkgs, err := packages.Load(
		&packages.Config{
			Mode:  packages.NeedImports | packages.NeedName | packages.NeedSyntax | packages.NeedFiles,
			Dir:   pkgDir,
			Tests: true,
		},
		patterns...,
	)
	return pkgs, errors.Wrap(err, "loading packages")
}

func listBenchmarks(pkgDir string, patterns ...string) ([]BenchmarkInfo, error) {
	absPkgDir, err := filepath.Abs(pkgDir)
	if err != nil {
		return nil, err
	}
	pkgs, err := listPackages(pkgDir, patterns...)
	if err != nil {
		return nil, err
	}

	benchmarkInfoList := make([]BenchmarkInfo, 0)
	for _, pkg := range pkgs {
		for _, f := range pkg.Syntax {
			filename := pkg.Fset.Position(f.Pos()).Filename
			if !strings.HasSuffix(filename, "_test.go") {
				continue
			}
			bi, analyzeErr := analyzeAST(f, absPkgDir, filename)
			if analyzeErr != nil {
				return nil, analyzeErr
			}
			benchmarkInfoList = append(benchmarkInfoList, bi...)
		}
	}
	return benchmarkInfoList, nil
}

func isTestingB(t ast.Expr) bool {
	// Accept "*testing.B" or an identifier named "B" (best-effort)
	switch x := t.(type) {
	case *ast.StarExpr:
		if se, ok := x.X.(*ast.SelectorExpr); ok {
			if id, ok := se.X.(*ast.Ident); ok && id.Name == "testing" && se.Sel.Name == "B" {
				return true
			}
		}
		if id, ok := x.X.(*ast.Ident); ok && id.Name == "B" {
			return true
		}
	}
	return false
}

func analyzeAST(
	f *ast.File, absPkgDir, filename string,
) (benchmarkInfoList []BenchmarkInfo, err error) {
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Name == nil || !strings.HasPrefix(fd.Name.Name, "Benchmark") {
			return true
		}
		// Must have signature like (b *testing.B)
		if len(fd.Type.Params.List) == 0 {
			return true
		}
		if !isTestingB(fd.Type.Params.List[0].Type) {
			return true
		}
		var benchmarkInfo BenchmarkInfo
		benchmarkInfo, err = analyzeBenchmark(fd, absPkgDir, filename)
		if err != nil {
			return false
		}
		benchmarkInfoList = append(benchmarkInfoList, benchmarkInfo)
		return true
	})
	if err != nil {
		return nil, err
	}
	return benchmarkInfoList, nil
}

func analyzeBenchmark(fn *ast.FuncDecl, absolutePkgDir, filename string) (BenchmarkInfo, error) {
	var benchmarkInfo BenchmarkInfo
	benchmarkInfo.Name = fn.Name.Name
	co, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		return BenchmarkInfo{}, err
	}
	relFilename, err := filepath.Rel(absolutePkgDir, filename)
	if err != nil {
		return BenchmarkInfo{}, err
	}
	teams := co.Match(filepath.Join("pkg", relFilename))
	if len(teams) > 0 {
		team := teams[0]
		teamName := strings.TrimPrefix(string(team.TeamName), "cockroachdb/")
		benchmarkInfo.Team = teamName
	}

	// Parsing of the bench document is best-effort and lenient, any malformed lines
	// will be ignored.
	if fn.Doc != nil {
		for _, line := range strings.Split(fn.Doc.Text(), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, docMarker) {
				config := strings.TrimPrefix(line, docMarker)
				config = strings.TrimSpace(config)
				parts := strings.Split(config, ",")
				for _, part := range parts {
					part = strings.TrimSpace(part)
					if part == "" {
						continue
					}
					kv := strings.SplitN(part, "=", 2)
					if len(kv) != 2 {
						continue
					}
					key := strings.TrimSpace(kv[0])
					val := strings.TrimSpace(kv[1])
					switch key {
					case "count":
						_, _ = fmt.Sscanf(val, "%d", &benchmarkInfo.Args.Count)
					case "benchtime":
						benchmarkInfo.Args.BenchTime = val
					case "timeout":
						if d, parseErr := time.ParseDuration(val); parseErr == nil {
							benchmarkInfo.Args.Timeout = d
						}
					case "suite":
						benchmarkInfo.Args.Suite = val
					}
				}
			}
		}
	}

	return benchmarkInfo, nil
}
