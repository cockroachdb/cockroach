// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package benchdoc

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
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

	nameResolver func() (string, error)
)

const docMarker = "benchmark-ci:"

// AnalyzeBenchmarkDocs traverses the provided AST to identify benchmark functions and extract their associated
// configuration from documentation comments.
// Example benchmark documentation:
// benchmark-ci: count=10, benchtime=50x, timeout=20m, suite=manual
func AnalyzeBenchmarkDocs(
	f *ast.File,
	packageResolver, teamResolver nameResolver,
	lenient bool,
	reportFailure func(token.Pos, error),
) (benchmarkInfoList []BenchmarkInfo, err error) {
	report := func(pos token.Pos, err error) {
		if reportFailure != nil {
			reportFailure(pos, err)
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Name == nil || !strings.HasPrefix(fd.Name.Name, "Benchmark") {
			return true
		}
		if len(fd.Type.Params.List) != 1 {
			return true
		}
		if fd.Type.Results != nil {
			return true
		}
		if !isTestingB(fd.Type.Params.List[0].Type) {
			return true
		}

		var args RunArgs
		args, err = analyzeBenchmarkArgs(fd)
		if err != nil {
			if !lenient {
				report(fd.Pos(), err)
				return false
			}
			err = nil
		}
		var team string
		team, err = teamResolver()
		if err != nil {
			report(fd.Pos(), err)
			return false
		}
		var pkg string
		pkg, err = packageResolver()
		if err != nil {
			report(fd.Pos(), err)
			return false
		}

		benchmarkInfoList = append(benchmarkInfoList, BenchmarkInfo{
			Name:    fd.Name.Name,
			Package: pkg,
			Team:    team,
			Args:    args,
		})
		return true
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

// analyzeBenchmarkArgs extracts benchmark configuration from a function's documentation comments.
// Returns an error if the benchmark-ci documentation is malformed.
func analyzeBenchmarkArgs(fn *ast.FuncDecl) (RunArgs, error) {
	var args RunArgs
	if fn.Doc == nil {
		return args, nil
	}
	for _, line := range strings.Split(fn.Doc.Text(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, docMarker) {
			continue
		}
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
				return RunArgs{}, errors.Newf("malformed key-value pair in %s: %q", fn.Name.Name, part)
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			switch key {
			case "count":
				var count int
				if n, err := fmt.Sscanf(val, "%d", &count); err != nil || n != 1 {
					return RunArgs{}, errors.Newf("invalid count value in %s: %q", fn.Name.Name, val)
				}
				args.Count = count
			case "benchtime":
				args.BenchTime = val
			case "timeout":
				d, err := time.ParseDuration(val)
				if err != nil {
					return RunArgs{}, errors.Wrapf(err, "invalid timeout value in %s", fn.Name.Name)
				}
				args.Timeout = d
			case "suite":
				args.Suite = val
			default:
				return RunArgs{}, errors.Newf("unknown benchmark config key in %s: %q", fn.Name.Name, key)
			}
		}
	}
	return args, nil
}
