// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "text/template"

const templateText = `
{{- define "cclHeader" -}}
// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
{{- end }}

{{- define "bslHeader" -}}
// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
{{- end }}

{{- define "header" }}
{{- if .Ccl }}{{ template "cclHeader" }}
{{- else }}{{ template "bslHeader" }}{{ end }}
{{- end }}

{{- define "declareTestdataPaths" }}
{{- if .LogicTest }}var logicTestDir string
{{ end }}{{ if .CclLogicTest }}var cclLogicTestDir string
{{ end }}{{ if .ExecBuildLogicTest }}var execBuildLogicTestDir string
{{ end }}{{ if .SqliteLogicTest }}var sqliteLogicTestDir string
{{ end }}
{{- end }}

{{- define "runLogicTest" }}
{{- if .LogicTest -}}
func runLogicTest(t *testing.T, file string) {
	skip.UnderDeadlock(t, "times out and/or hangs")
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, configIdx, filepath.Join(logicTestDir, file))
}
{{ end }}
{{- end }}

{{- define "runCCLLogicTest" }}
{{- if .CclLogicTest -}}
func runCCLLogicTest(t *testing.T, file string) {
	skip.UnderDeadlock(t, "times out and/or hangs")
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, configIdx, filepath.Join(cclLogicTestDir, file))
}
{{ end }}
{{- end }}

{{- define "runExecBuildLogicTest" }}
{{- if .ExecBuildLogicTest -}}
func runExecBuildLogicTest(t *testing.T, file string) {
	defer sql.TestingOverrideExplainEnvVersion("CockroachDB execbuilder test version")()
	skip.UnderDeadlock(t, "times out and/or hangs")
	serverArgs := logictest.TestServerArgs{
		DisableWorkmemRandomization: true,{{ if .ForceProductionValues }}
		ForceProductionValues:       true,{{end}}
		// Disable the direct scans in order to keep the output of EXPLAIN (VEC)
		// deterministic.
		DisableDirectColumnarScans: true,
	}
	logictest.RunLogicTest(t, serverArgs, configIdx, filepath.Join(execBuildLogicTestDir, file))
}
{{ end }}
{{- end }}

{{- define "runSqliteLogicTest" }}
{{- if .SqliteLogicTest -}}
func runSqliteLogicTest(t *testing.T, file string) {
	skip.UnderDeadlock(t, "times out and/or hangs")
	if !*logictest.Bigtest {
		skip.IgnoreLint(t, "-bigtest flag must be specified to run this test")
	}
	// SQLLite logic tests can be very memory intensive, so we give them larger
	// limit than other logic tests get. Also some of the 'delete' files become
	// extremely slow when MVCC range tombstones are enabled for point deletes,
	// so we disable that.
	serverArgs := logictest.TestServerArgs{
		MaxSQLMemoryLimit: 512 << 20, // 512 MiB
		DisableUseMVCCRangeTombstonesForPointDeletes: true,
		// Some sqlite tests with very low bytes limit value are too slow, so
		// ensure 3 KiB lower bound.
		BatchBytesLimitLowerBound: 3 << 10, // 3 KiB
	}
	logictest.RunLogicTest(t, serverArgs, configIdx, filepath.Join(sqliteLogicTestDir, file))
}
{{ end }}
{{- end }}

{{- define "declareTestdataSetupFunctions" }}
{{- template "runLogicTest" . }}
{{- template "runCCLLogicTest" . }}
{{- template "runExecBuildLogicTest" . }}
{{- template "runSqliteLogicTest" . }}
{{- end }}

{{- define "initLogicTest" }}
{{ if .LogicTest }}	if bazel.BuiltWithBazel() {
		var err error
		logicTestDir, err = bazel.Runfile("pkg/sql/logictest/testdata/logic_test")
		if err != nil {
			panic(err)
		}
	} else {
		logicTestDir = "{{ .RelDir }}/sql/logictest/testdata/logic_test"
	}
{{ end }}
{{- end }}

{{- define "initCCLLogicTest" }}
{{- if .CclLogicTest }}	if bazel.BuiltWithBazel() {
		var err error
		cclLogicTestDir, err = bazel.Runfile("pkg/ccl/logictestccl/testdata/logic_test")
		if err != nil {
			panic(err)
		}
	} else {
		cclLogicTestDir = "{{ .RelDir }}/ccl/logictestccl/testdata/logic_test"
	}
{{ end }}
{{- end }}

{{- define "initExecBuildLogicTest" }}
{{- if .ExecBuildLogicTest }}	if bazel.BuiltWithBazel() {
		var err error
		execBuildLogicTestDir, err = bazel.Runfile("pkg/sql/opt/exec/execbuilder/testdata")
		if err != nil {
			panic(err)
		}
	} else {
		execBuildLogicTestDir = "{{ .RelDir }}/sql/opt/exec/execbuilder/testdata"
	}
{{ end }}
{{- end }}

{{- define "initFunc" }}
func init() {
{{- template "initLogicTest" . -}}
{{- template "initCCLLogicTest" . -}}
{{- template "initExecBuildLogicTest" . -}}
}
{{- end }}

{{- template "header" . }}

// Code generated by generate-logictest, DO NOT EDIT.

package test{{ .Package }}

import ({{ if .SqliteLogicTest }}
	"flag"{{ end }}
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"{{ if .Ccl }}
	"github.com/cockroachdb/cockroach/pkg/ccl"{{ end }}
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"{{ if .ExecBuildLogicTest }}
	"github.com/cockroachdb/cockroach/pkg/sql"{{ end }}
	"github.com/cockroachdb/cockroach/pkg/sql/logictest"{{ if .SqliteLogicTest }}
	"github.com/cockroachdb/cockroach/pkg/sql/sqlitelogictest"{{ end }}
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const configIdx = {{ .ConfigIdx }}

{{ template "declareTestdataPaths" . }}
{{- template "initFunc" . }}

func TestMain(m *testing.M) {
{{ if .SqliteLogicTest }}	flag.Parse()
	if *logictest.Bigtest {
		if bazel.BuiltWithBazel() {
			var err error
			sqliteLogicTestDir, err = bazel.Runfile("external/com_github_cockroachdb_sqllogictest")
			if err != nil {
				panic(err)
			}
		} else {
			var err error
			sqliteLogicTestDir, err = sqlitelogictest.FindLocalLogicTestClone()
			if err != nil {
				panic(err)
			}
		}
	}
{{ end }}{{ if .Ccl }}	defer ccl.TestingEnableEnterprise()()
{{ end }}	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

{{ template "declareTestdataSetupFunctions" . }}{{- if not .SqliteLogicTest }}
// TestLogic_tmp runs any tests that are prefixed with "_", in which a dedicated
// test is not generated for. This allows developers to create and run temporary
// test files that are not checked into the repository, without repeatedly
// regenerating and reverting changes to this file, generated_test.go.
//
// TODO(mgartner): Add file filtering so that individual files can be run,
// instead of all files with the "_" prefix.
func TestLogic_tmp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var glob string
	{{- if .LogicTest}}
	glob = filepath.Join(logicTestDir, "_*")
	logictest.RunLogicTests(t, logictest.TestServerArgs{}, configIdx, glob)
	{{- end}}
	{{- if .CclLogicTest }}
	glob = filepath.Join(cclLogicTestDir, "_*")
	logictest.RunLogicTests(t, logictest.TestServerArgs{}, configIdx, glob)
	{{- end }}
	{{- if .ExecBuildLogicTest }}
	glob = filepath.Join(execBuildLogicTestDir, "_*")
	serverArgs := logictest.TestServerArgs{
		DisableWorkmemRandomization: true,
	}
	logictest.RunLogicTests(t, serverArgs, configIdx, glob)
	{{- end }}
}
{{ end }}`

// There is probably room for optimization here. Among other things:
// some tests may declare a testdata dependency they don't actually need, and
// the sizes for some of these tests can probably be smaller than "enormous".
const buildFileTemplate = `load("//build/bazelutil/unused_checker:unused.bzl", "get_x_data")
load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "{{ .TestRuleName }}_test",
    size = "enormous",
    srcs = ["generated_test.go"],{{ if .SqliteLogicTest }}
    args = ["-test.timeout=7195s"],{{ else }}
    args = ["-test.timeout=3595s"],{{ end }}
    data = [
        "//c-deps:libgeos",  # keep{{ if .SqliteLogicTest }}
        "@com_github_cockroachdb_sqllogictest//:testfiles",  # keep{{ end }}{{ if .CockroachGoTestserverTest }}
        "//pkg/cmd/cockroach-short",  # keep{{ end }}{{ if .CclLogicTest }}
        "//pkg/ccl/logictestccl:testdata",  # keep{{ end }}{{ if .LogicTest }}
        "//pkg/sql/logictest:testdata",  # keep{{ end }}{{ if .ExecBuildLogicTest }}
        "//pkg/sql/opt/exec/execbuilder:testdata",  # keep{{ end }}
    ],
    shard_count = {{ if gt .TestCount 16 }}16{{ else }}{{ .TestCount }}{{end}},
    tags = ["cpu:{{ if gt .NumCPU 4 }}4{{ else }}{{ .NumCPU }}{{ end }}"],
    deps = [
        "//pkg/build/bazel",{{ if .Ccl }}
        "//pkg/ccl",{{ end }}
        "//pkg/security/securityassets",
        "//pkg/security/securitytest",
        "//pkg/server",{{ if .ExecBuildLogicTest }}
        "//pkg/sql",{{ end }}
        "//pkg/sql/logictest",{{ if .SqliteLogicTest }}
        "//pkg/sql/sqlitelogictest",{{ end }}
        "//pkg/testutils/serverutils",
        "//pkg/testutils/skip",
        "//pkg/testutils/testcluster",
        "//pkg/util/leaktest",
        "//pkg/util/randutil",
    ],
)

get_x_data(name = "get_x_data")
`

var (
	testFileTpl  = template.Must(template.New("source").Parse(templateText))
	buildFileTpl = template.Must(template.New("source").Parse(buildFileTemplate))
)
