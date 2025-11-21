// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
)

type (
	BenchmarkInfo struct {
		Name    string
		Package string
		Team    string
		Args    RunArgs
	}

	RunArgs struct {
		Suite     string
		Timeout   time.Duration
		Count     int
		BenchTime string
	}
)

const docMarker = "benchmark-ci:"

func listBenchmarks(pkgDir string) ([]BenchmarkInfo, error) {
	absPkgDir, err := filepath.Abs(pkgDir)
	if err != nil {
		return nil, err
	}

	benchmarkInfoList := make([]BenchmarkInfo, 0)
	err = filepath.WalkDir(absPkgDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}
		bi, analyzeErr := analyzeBenchmarkAST(f, absPkgDir, path)
		if analyzeErr != nil {
			return analyzeErr
		}
		benchmarkInfoList = append(benchmarkInfoList, bi...)
		return nil
	})
	if err != nil {
		return nil, err
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

func analyzeBenchmarkAST(
	f *ast.File, absPkgDir, filename string,
) (benchmarkInfoList []BenchmarkInfo, err error) {
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Name == nil || !strings.HasPrefix(fd.Name.Name, "Benchmark") {
			return true
		}
		if len(fd.Type.Params.List) == 0 {
			return true
		}
		if !isTestingB(fd.Type.Params.List[0].Type) {
			return true
		}
		var benchmarkInfo BenchmarkInfo
		benchmarkInfo, err = analyzeBenchmarkFunc(fd, absPkgDir, filename)
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

func analyzeBenchmarkFunc(
	fn *ast.FuncDecl, absolutePkgDir, filename string,
) (BenchmarkInfo, error) {
	co, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		return BenchmarkInfo{}, err
	}
	relFilename, err := filepath.Rel(absolutePkgDir, filename)
	if err != nil {
		return BenchmarkInfo{}, err
	}

	var benchmarkInfo BenchmarkInfo
	benchmarkInfo.Name = fn.Name.Name
	benchmarkInfo.Package = filepath.Join("pkg", filepath.Dir(relFilename))
	teams := co.Match(filepath.Join("pkg", relFilename))
	if len(teams) > 0 {
		team := teams[0]
		teamName := strings.TrimPrefix(string(team.TeamName), "cockroachdb/")
		benchmarkInfo.Team = teamName
	}

	// Parsing of the benchmark function document is best-effort and lenient.
	// Any malformed lines will be ignored.
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
