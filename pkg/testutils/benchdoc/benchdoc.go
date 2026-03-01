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
		Name        string      `json:"name"`
		Package     string      `json:"package"`
		Team        string      `json:"team"`
		RunArgs     RunArgs     `json:"run_args"`
		CompareArgs CompareArgs `json:"compare_args"`
	}

	RunArgs struct {
		Suite     Suite         `json:"suite,omitempty"`
		Timeout   time.Duration `json:"timeout,omitempty"`
		Count     int           `json:"count,omitempty"`
		BenchTime string        `json:"bench_time,omitempty"`
	}

	CompareArgs struct {
		PostIssue PostIssue `json:"post_issue,omitempty"`
		Threshold float64   `json:"threshold,omitempty"`
	}

	NameResolver func() (string, error)
	PostIssue    string
	Suite        string
)

const (
	PostIssueNone    PostIssue = "none"
	PostIssueNotify  PostIssue = "notify"
	PostIssueBlocker PostIssue = "release-blocker"
)

const (
	SuiteManual Suite = "manual"
	SuiteWeekly Suite = "weekly"
)

const (
	docMarker = "benchmark-ci:"
)

// NewRunArgs creates a new RunArgs with default values
func NewRunArgs() RunArgs {
	return RunArgs{
		Suite: SuiteWeekly,
	}
}

// NewCompareArgs creates a new CompareArgs with default values
func NewCompareArgs() CompareArgs {
	return CompareArgs{
		PostIssue: PostIssueNone,
	}
}

// AnalyzeBenchmarkDocs traverses the provided AST to identify benchmark functions and extract their associated
// configuration from documentation comments.
// Example benchmark documentation:
// benchmark-ci: count=10, benchtime=50x, timeout=20m, suite=manual
func AnalyzeBenchmarkDocs(
	f *ast.File,
	packageResolver, teamResolver NameResolver,
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

		var runArgs RunArgs
		var compareArgs CompareArgs
		runArgs, compareArgs, err = analyzeBenchmarkArgs(fd)
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
			Name:        fd.Name.Name,
			Package:     pkg,
			Team:        team,
			RunArgs:     runArgs,
			CompareArgs: compareArgs,
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
func analyzeBenchmarkArgs(fn *ast.FuncDecl) (RunArgs, CompareArgs, error) {
	runArgs := NewRunArgs()
	compareArgs := NewCompareArgs()
	if fn.Doc == nil {
		return runArgs, compareArgs, nil
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
				return RunArgs{}, CompareArgs{},
					errors.Newf("malformed key-value pair in %s: %q", fn.Name.Name, part)
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			switch key {
			case "count":
				var count int
				if n, err := fmt.Sscanf(val, "%d", &count); err != nil || n != 1 {
					return RunArgs{}, CompareArgs{},
						errors.Newf("invalid count value in %s: %q", fn.Name.Name, val)
				}
				runArgs.Count = count
			case "benchtime":
				runArgs.BenchTime = val
			case "timeout":
				d, err := time.ParseDuration(val)
				if err != nil {
					return RunArgs{}, CompareArgs{},
						errors.Wrapf(err, "invalid timeout value in %s", fn.Name.Name)
				}
				runArgs.Timeout = d
			case "suite":
				switch val {
				case "weekly":
					runArgs.Suite = SuiteWeekly
				case "manual":
					runArgs.Suite = SuiteManual
				default:
					return RunArgs{}, CompareArgs{},
						errors.Newf("invalid suite value in %s: %q", fn.Name.Name, val)
				}
			case "post":
				switch val {
				case "none":
					compareArgs.PostIssue = PostIssueNone
				case "notify":
					compareArgs.PostIssue = PostIssueNotify
				case "release-blocker":
					compareArgs.PostIssue = PostIssueBlocker
				default:
					return RunArgs{}, CompareArgs{},
						errors.Newf("invalid post issue value in %s: %q", fn.Name.Name, val)
				}
			case "threshold":
				var threshold float64
				if n, err := fmt.Sscanf(val, "%f", &threshold); err != nil || n != 1 {
					return RunArgs{}, CompareArgs{},
						errors.Newf("invalid threshold value in %s: %q", fn.Name.Name, val)
				}
				if threshold < 0 || threshold > 1.0 {
					return RunArgs{}, CompareArgs{},
						errors.Newf("threshold must be in range [0.0, 1.0] in %s: %f", fn.Name.Name, threshold)
				}
				compareArgs.Threshold = threshold
			default:
				return RunArgs{}, CompareArgs{},
					errors.Newf("unknown benchmark config key in %s: %q", fn.Name.Name, key)
			}
		}
	}
	return runArgs, compareArgs, nil
}
