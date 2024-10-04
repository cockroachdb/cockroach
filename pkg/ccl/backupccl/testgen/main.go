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

var restoreRemoteMonitoringOutPath = flag.String("restore-memory-monitoring", "", "path to the output file")
var dataDrivenOutPath = flag.String("data-driven", "", "path to the output file")
var restoreEntryCoverOutPath = flag.String("restore-entry-cover", "", "path to the output file")
var restoreMidSchemaChangeOutPath = flag.String("restore-mid-schema-change", "", "path to the output file")

func genTestRestoreMemoryMonitoring() {
	type testCase struct {
		NumSplits                int
		NumInc                   int
		RestoreProcessorMaxFiles int
	}
	var testCases []testCase
	for _, numSplits := range []int{10, 100, 200} {
		for _, numInc := range []int{0, 1, 3, 10} {
			for _, restoreProcessorMaxFiles := range []int{5, 10, 20} {
				testCases = append(testCases, testCase{numSplits, numInc, restoreProcessorMaxFiles})
			}
		}
	}
	data := struct{ Tests []testCase }{Tests: testCases}
	tmpl := template.Must(template.New("source").Parse(test_restore_memory_monitoring_template))
	file, err := os.Create(filepath.Join(*restoreRemoteMonitoringOutPath))
	if err != nil {
		panic(errors.Wrap(err, "failed to create file"))
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		panic(errors.Wrap(err, "failed to execute template"))
	}
}

func genTestDataDriven() {
	type TestCase struct {
		TestFilePath string
		TestName     string
	}
	runFile, err := bazel.Runfile("pkg/ccl/backupccl/testdata/backup-restore")
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
			tcName := strings.TrimPrefix(relPathIncludingPkg, "pkg/ccl/backupccl/testdata/backup-restore/")
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

func genTestRestoreEntryCover() {
	type testCase struct {
		NumBackups int
	}
	var testCases []testCase
	for _, numBackups := range []int{1, 2, 3, 5, 9, 10, 11, 12} {
		testCases = append(testCases, testCase{
			NumBackups: numBackups,
		})
	}

	data := struct{ Tests []testCase }{Tests: testCases}
	tmpl := template.Must(template.New("source").Parse(test_restore_entry_cover_template))
	file, err := os.Create(filepath.Join(*restoreEntryCoverOutPath))
	if err != nil {
		panic(errors.Wrap(err, "failed to create file"))
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		panic(errors.Wrap(err, "failed to execute template"))
	}
}

func genTestRestoreMidSchemaChange() {
	type testCase struct {
		SchemaOnly, ClusterRestore bool
	}
	var testCases []testCase
	for _, SchemaOnly := range []bool{true, false} {
		for _, ClusterRestore := range []bool{true, false} {
			testCases = append(testCases, testCase{SchemaOnly, ClusterRestore})
		}
	}
	data := struct{ Tests []testCase }{Tests: testCases}
	tmpl := template.Must(template.New("source").Parse(test_restore_mid_schema_change_template))
	file, err := os.Create(filepath.Join(*restoreMidSchemaChangeOutPath))
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
	if *restoreRemoteMonitoringOutPath == "" || *dataDrivenOutPath == "" || *restoreEntryCoverOutPath == "" || *restoreMidSchemaChangeOutPath == "" {
		panic(`you need to pass values for the following flags:
-restore-memory-monitoring -data-driven -restore-entry-cover -restore-mid-schema-change`)
	}
	genTestRestoreMemoryMonitoring()
	genTestDataDriven()
	genTestRestoreEntryCover()
	genTestRestoreMidSchemaChange()
}
