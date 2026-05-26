// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"flag"
	"html/template"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

var outDir = flag.String("out-dir", "", "path to the root of the cockroach workspace")

func main() {
	flag.Parse()
	if *outDir == "" {
		log.Fatal("You need to pass -out-dir flag as the workspace root")
	}
	interactiveTestsDir, err := bazel.Runfile("pkg/cli/interactive_tests")
	if err != nil {
		log.Fatal(err)
	}
	testGlob := path.Join(interactiveTestsDir, "test*.tcl")

	paths, err := filepath.Glob(testGlob)
	if err != nil {
		log.Fatal(err)
	}
	if len(paths) == 0 {
		log.Fatalf("no testfiles found (%v)", testGlob)
	}

	// testCases maps test name suffix to test file path.
	testCases := make(map[string]string, len(paths))
	for i := range paths {
		lastPkgIdx := strings.LastIndex(paths[i], "pkg/")
		// path needs to be the relative path from pkg/acceptance.
		path := "../" + paths[i][lastPkgIdx+len("pkg/"):]
		testCases[strings.Split(filepath.Base(paths[i]), ".tcl")[0]] = path
	}

	templateVars := make(map[string]interface{})
	templateVars["TestCases"] = testCases
	tmpl := template.Must(template.New("source").Parse(cli_test_template))
	file, err := os.Create(path.Join(*outDir, "pkg", "acceptance", "generated_cli_test.go"))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	if err := tmpl.Execute(file, templateVars); err != nil {
		log.Fatal(err)
	}
}
