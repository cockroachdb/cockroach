// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"flag"
	"html/template"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/errors"
)

var dataDrivenOutPath = flag.String("data-driven", "", "path to the output file")

func genTestDataDriven() {
	type TestCase struct {
		TestFilePath string
		TestName     string
	}
	runFile, err := bazel.Runfile("pkg/backup/testdata/backup-restore")
	if err != nil {
		panic(err)
	}
	var testcases []TestCase
	if err := filepath.WalkDir(runFile, func(filePath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			splitPath := strings.Split(filePath, "pkg/")
			relPathIncludingPkg := "pkg/" + splitPath[len(splitPath)-1]
			tcName := strings.TrimPrefix(relPathIncludingPkg, "pkg/backup/testdata/backup-restore/")
			tcName = strings.ReplaceAll(tcName, "/", "_")
			tcName = strings.ReplaceAll(tcName, "-", "_")
			testcases = append(testcases, TestCase{relPathIncludingPkg, tcName})
		}
		return nil
	}); err != nil {
		panic(err)
	}
	data := struct{ TestCases []TestCase }{TestCases: testcases}
	tmpl := template.Must(template.New("source").Parse(test_data_driven_template))
	file, err := os.Create(filepath.Join(*dataDrivenOutPath))
	if err != nil {
		panic(errors.Wrap(err, "failed to create file"))
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		panic(errors.Wrap(err, "failed to execute template"))
	}
}

func main() {
	flag.Parse()
	if *dataDrivenOutPath == "" {
		panic(`you need to pass values for the following flags:
-restore-memory-monitoring -data-driven`)
	}
	genTestDataDriven()
}
