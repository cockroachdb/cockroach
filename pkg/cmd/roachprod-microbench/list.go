// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/testutils/benchdoc"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type BenchmarkInfoList []benchdoc.BenchmarkInfo

func listBenchmarks(pkgDir string) (BenchmarkInfoList, error) {
	absPkgDir, err := filepath.Abs(pkgDir)
	if err != nil {
		return nil, err
	}
	co, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		return nil, err
	}

	var benchmarkInfoList BenchmarkInfoList
	g := errgroup.Group{}
	g.SetLimit(runtime.GOMAXPROCS(0))
	resultsCh := make(chan []benchdoc.BenchmarkInfo, 4096)

	// Start a goroutine to collect results
	done := make(chan struct{})
	go func() {
		for biList := range resultsCh {
			benchmarkInfoList = append(benchmarkInfoList, biList...)
		}
		close(done)
	}()

	walkErr := filepath.WalkDir(absPkgDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		g.Go(func() error {
			fset := token.NewFileSet()
			f, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if parseErr != nil {
				return parseErr
			}

			relFilename, err := filepath.Rel(absPkgDir, path)
			if err != nil {
				return err
			}

			teamResolver := func() (string, error) {
				teams := co.Match(filepath.Join("pkg", relFilename))
				if len(teams) > 0 {
					team := teams[0]
					teamName := strings.TrimPrefix(string(team.TeamName), "cockroachdb/")
					return teamName, nil
				}
				return "", nil
			}

			packageResolver := func() (string, error) {
				return filepath.Join("pkg", filepath.Dir(relFilename)), nil
			}

			bi, analyzeErr := benchdoc.AnalyzeBenchmarkDocs(f, packageResolver, teamResolver, true, nil)
			if analyzeErr != nil {
				return analyzeErr
			}
			resultsCh <- bi
			return nil
		})
		return nil
	})

	// Always wait for all goroutines to complete before closing the channel
	groupErr := g.Wait()
	close(resultsCh)
	<-done

	// Check for errors after all goroutines have finished
	if walkErr != nil {
		return nil, walkErr
	}
	if groupErr != nil {
		return nil, groupErr
	}

	return benchmarkInfoList, nil
}

func (p BenchmarkInfoList) export(jsonPath string) error {
	data, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "marshaling benchmark list")
	}
	if err := os.WriteFile(jsonPath, data, 0644); err != nil {
		return errors.Wrapf(err, "writing to %s", jsonPath)
	}
	return nil
}

func loadBenchmarkList(jsonPath string) (BenchmarkInfoList, error) {
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		return nil, errors.Wrapf(err, "reading from %s", jsonPath)
	}
	var list BenchmarkInfoList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, errors.Wrap(err, "unmarshaling benchmark list")
	}
	return list, nil
}
