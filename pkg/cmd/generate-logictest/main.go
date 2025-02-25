// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest/logictestbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlitelogictest"
)

type testFileTemplateConfig struct {
	LogicTest                     bool
	CclLogicTest                  bool
	ExecBuildLogicTest            bool
	SqliteLogicTest               bool
	CockroachGoTestserverTest     bool
	Ccl                           bool
	ForceProductionValues         bool
	SkipUnderRace                 bool
	UseHeavyPool                  useHeavyPoolCondition
	Package, TestRuleName, RelDir string
	ConfigIdx                     int
	TestCount                     int
	NumCPU                        int
}

type useHeavyPoolCondition int

const (
	useHeavyPoolNever useHeavyPoolCondition = iota
	// useHeavyPoolForExpensiveConfig is used for tests running under deadlock or race.
	useHeavyPoolForExpensiveConfig
	useHeavyPoolAlways
)

var outDir = flag.String("out-dir", "", "path to the root of the cockroach workspace")

type logicTestGenerator struct {
	cclLogicTestsGlob, execBuildLogicTestsGlob, logicTestsGlob, sqliteLogicTestsPath string
}

func (g *logicTestGenerator) testdir(rel string) (*testdir, error) {
	abs := filepath.Join(*outDir, rel)
	err := os.RemoveAll(abs)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(abs, 0777)
	if err != nil {
		return nil, err
	}
	relPathToParent := ".."
	numSlashes := strings.Count(rel, "/")
	for numSlashes > 0 {
		relPathToParent += "/.."
		numSlashes--
	}
	return &testdir{g: g, dir: abs, relPathToParent: relPathToParent}, nil
}

type testPrefixConfigPaths struct {
	testPrefix  string
	configPaths [][]string
}

type testdir struct {
	g                                                                                                            *logicTestGenerator
	dir, relPathToParent                                                                                         string
	cclLogicTestsConfigPaths, execBuildLogicTestsConfigPaths, logicTestsConfigPaths, sqliteLogicTestsConfigPaths []testPrefixConfigPaths
}

func (t *testdir) addLogicTests(testPrefix string, calc logictestbase.ConfigCalculator) error {
	paths, err := calc.Enumerate(t.g.logicTestsGlob)
	if err != nil {
		return err
	}
	t.logicTestsConfigPaths = append(t.logicTestsConfigPaths, testPrefixConfigPaths{testPrefix: testPrefix, configPaths: paths})
	return nil
}

func (t *testdir) addCclLogicTests(testPrefix string, calc logictestbase.ConfigCalculator) error {
	paths, err := calc.Enumerate(t.g.cclLogicTestsGlob)
	if err != nil {
		return err
	}
	t.cclLogicTestsConfigPaths = append(t.cclLogicTestsConfigPaths, testPrefixConfigPaths{testPrefix: testPrefix, configPaths: paths})
	return nil
}

func (t *testdir) addExecBuildLogicTests(
	testPrefix string, calc logictestbase.ConfigCalculator,
) error {
	paths, err := calc.Enumerate(t.g.execBuildLogicTestsGlob)
	if err != nil {
		return err
	}
	t.execBuildLogicTestsConfigPaths = append(t.execBuildLogicTestsConfigPaths, testPrefixConfigPaths{testPrefix: testPrefix, configPaths: paths})
	return nil
}

func (t *testdir) addSqliteLogicTests(
	testPrefix string, calc logictestbase.ConfigCalculator,
) error {
	sqliteLogicTestsGlobs := make([]string, len(sqlitelogictest.Globs))
	for i, glob := range sqlitelogictest.Globs {
		sqliteLogicTestsGlobs[i] = t.g.sqliteLogicTestsPath + glob
	}
	paths, err := calc.Enumerate(sqliteLogicTestsGlobs...)
	if err != nil {
		return err
	}
	t.sqliteLogicTestsConfigPaths = append(t.sqliteLogicTestsConfigPaths, testPrefixConfigPaths{testPrefix: testPrefix, configPaths: paths})
	return nil
}

var tmpRegexp = regexp.MustCompile(`/_(\w|\-)+$`)

func (t *testdir) dump() error {
	for configIdx := range logictestbase.LogicTestConfigs {
		tplCfg := testFileTemplateConfig{
			Ccl:                   strings.Contains(t.dir, "pkg/ccl"),
			ForceProductionValues: strings.HasSuffix(t.dir, "pkg/sql/opt/exec/execbuilder/tests"),
		}
		var testCount int
		nonTmpCount := func(paths []string) int {
			n := 0
			for i := range paths {
				if !tmpRegexp.MatchString(paths[i]) {
					n++
				}
			}
			return n
		}
		for _, configPaths := range t.logicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			testCount += nonTmpCount(paths)
			if len(paths) > 0 {
				tplCfg.LogicTest = true
			}
		}
		for _, configPaths := range t.cclLogicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			testCount += nonTmpCount(paths)
			if len(paths) > 0 {
				tplCfg.CclLogicTest = true
			}
		}
		for _, configPaths := range t.execBuildLogicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			testCount += nonTmpCount(paths)
			if len(paths) > 0 {
				tplCfg.ExecBuildLogicTest = true
			}
		}
		for _, configPaths := range t.sqliteLogicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			testCount += nonTmpCount(paths)
			if len(paths) > 0 {
				tplCfg.SqliteLogicTest = true
			}
		}
		if testCount == 0 {
			continue
		}
		cfg := logictestbase.LogicTestConfigs[configIdx]
		tplCfg.ConfigIdx = configIdx
		tplCfg.TestRuleName = strings.ReplaceAll(cfg.Name, ".", "_")
		tplCfg.Package = strings.ReplaceAll(strings.ReplaceAll(cfg.Name, "-", "_"), ".", "")
		tplCfg.RelDir = t.relPathToParent
		tplCfg.TestCount = testCount
		tplCfg.CockroachGoTestserverTest = cfg.UseCockroachGoTestserver
		// The NumCPU calculation is a guess pulled out of thin air to
		// allocate the tests which use 3-node clusters 2 vCPUs, and
		// the ones which use more a bit more.
		tplCfg.NumCPU = (cfg.NumNodes / 2) + 1
		if strings.Contains(cfg.Name, "cockroach-go-testserver") {
			tplCfg.NumCPU = 3
		}
		if cfg.Name == "3node-tenant" || strings.HasPrefix(cfg.Name, "multiregion-") {
			tplCfg.SkipUnderRace = true
		}
		tplCfg.UseHeavyPool = useHeavyPoolNever
		if strings.Contains(cfg.Name, "5node") ||
			strings.Contains(cfg.Name, "fakedist") ||
			(strings.HasPrefix(cfg.Name, "local-") && !tplCfg.Ccl) ||
			(cfg.Name == "local" && !tplCfg.Ccl) {
			tplCfg.UseHeavyPool = useHeavyPoolForExpensiveConfig
		} else if strings.Contains(cfg.Name, "cockroach-go-testserver") ||
			strings.Contains(cfg.Name, "3node-tenant") {
			tplCfg.UseHeavyPool = useHeavyPoolAlways
		}
		subdir := filepath.Join(t.dir, cfg.Name)
		f, buildF, cleanup, err := openTestSubdir(subdir)
		if err != nil {
			return err
		}
		//nolint:deferloop TODO(#137605)
		defer cleanup()
		err = testFileTpl.Execute(f, tplCfg)
		if err != nil {
			return err
		}
		for _, configPaths := range t.logicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			for _, file := range paths {
				dumpTestForFile(f, configPaths.testPrefix, filepath.Base(file), "runLogicTest")
			}
		}
		for _, configPaths := range t.cclLogicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			for _, file := range paths {
				dumpTestForFile(f, configPaths.testPrefix, filepath.Base(file), "runCCLLogicTest")
			}
		}
		for _, configPaths := range t.execBuildLogicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			for _, file := range paths {
				dumpTestForFile(f, configPaths.testPrefix, filepath.Base(file), "runExecBuildLogicTest")
			}
		}
		for _, configPaths := range t.sqliteLogicTestsConfigPaths {
			paths := configPaths.configPaths[configIdx]
			for _, file := range paths {
				dumpTestForFile(f, configPaths.testPrefix, strings.TrimPrefix(file, t.g.sqliteLogicTestsPath), "runSqliteLogicTest")
			}
		}

		err = buildFileTpl.Execute(buildF, tplCfg)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpTestForFile(f io.Writer, prefix, file string, whichFunc string) {
	if strings.HasPrefix(file, "_") {
		// Ignore test files that start with "_", which are reserved as
		// temporary files used during development and debugging. Tests in these
		// files can be tested with the auto-generated TestLogic_tmp test.
		return
	}
	mungedFile := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(file, "/", ""), "-", "_"), ".", "_")
	fmt.Fprintf(f, "\nfunc %s_%s(\n", prefix, mungedFile)
	fmt.Fprintln(f, "\tt *testing.T,\n) {")
	fmt.Fprintln(f, "\tdefer leaktest.AfterTest(t)()")
	fmt.Fprintf(f, "\t%s(t, \"%s\")\n", whichFunc, file)
	fmt.Fprintln(f, "}")
}

func openTestSubdir(dir string) (testFile, buildFile *os.File, cleanup func(), err error) {
	err = os.Mkdir(dir, 0777)
	if err != nil {
		return
	}
	testFile, err = os.Create(filepath.Join(dir, "generated_test.go"))
	if err != nil {
		return
	}
	buildFile, err = os.Create(filepath.Join(dir, "BUILD.bazel"))
	if err != nil {
		_ = testFile.Close()
		return
	}
	cleanup = func() {
		err := testFile.Close()
		if err != nil {
			panic(err)
		}
		err = buildFile.Close()
		if err != nil {
			panic(err)
		}
	}
	return
}

func generate() error {
	if *outDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		*outDir = cwd
	}
	var gen logicTestGenerator
	if bazel.BuiltWithBazel() {
		runfiles, err := bazel.Runfile("pkg/ccl/logictestccl/testdata/logic_test")
		if err != nil {
			return err
		}
		cclLogicTestsGlob := filepath.Join(runfiles, "[^.]*")
		runfiles, err = bazel.Runfile("pkg/sql/opt/exec/execbuilder/testdata/")
		if err != nil {
			return err
		}
		execBuildLogicTestsGlob := filepath.Join(runfiles, "[^.]*")
		runfiles, err = bazel.Runfile("pkg/sql/logictest/testdata/logic_test")
		if err != nil {
			return err
		}
		logicTestsGlob := filepath.Join(runfiles, "[^.]*")
		sqliteLogicTestsPath, err := bazel.Runfile("external/com_github_cockroachdb_sqllogictest")
		if err != nil {
			return err
		}
		gen = logicTestGenerator{
			cclLogicTestsGlob, execBuildLogicTestsGlob, logicTestsGlob, sqliteLogicTestsPath,
		}
	} else {
		cclLogicTestsGlob := "pkg/ccl/logictestccl/testdata/logic_test/[^.]*"
		logicTestsGlob := "pkg/sql/logictest/testdata/logic_test/[^.]*"
		execBuildLogicTestsGlob := "pkg/sql/opt/exec/execbuilder/testdata/[^.]*"
		sqliteLogicTestsPath, err := sqlitelogictest.FindLocalLogicTestClone()
		if err != nil {
			return err
		}
		gen = logicTestGenerator{
			cclLogicTestsGlob, execBuildLogicTestsGlob, logicTestsGlob, sqliteLogicTestsPath,
		}
	}

	{
		t, err := gen.testdir("pkg/sql/logictest/tests")
		if err != nil {
			return err
		}
		err = t.addLogicTests("TestLogic", logictestbase.ConfigCalculator{})
		if err != nil {
			return err
		}
		err = t.dump()
		if err != nil {
			return err
		}
	}

	{
		t, err := gen.testdir("pkg/sql/sqlitelogictest/tests")
		if err != nil {
			return err
		}
		err = t.addSqliteLogicTests("TestSqlLiteLogic", logictestbase.ConfigCalculator{RunCCLConfigs: true})
		if err != nil {
			return err
		}
		err = t.dump()
		if err != nil {
			return err
		}
	}

	{
		t, err := gen.testdir("pkg/ccl/logictestccl/tests")
		if err != nil {
			return err
		}
		err = t.addCclLogicTests("TestCCLLogic", logictestbase.ConfigCalculator{})
		if err != nil {
			return err
		}
		readCommittedCalc := logictestbase.ConfigCalculator{
			ConfigOverrides: []string{"local-read-committed"},
			RunCCLConfigs:   true,
		}
		err = t.addCclLogicTests("TestReadCommittedLogicCCL", readCommittedCalc)
		if err != nil {
			return err
		}
		err = t.addLogicTests("TestReadCommittedLogic", readCommittedCalc)
		if err != nil {
			return err
		}
		repeatableReadCalc := logictestbase.ConfigCalculator{
			ConfigOverrides: []string{"local-repeatable-read"},
			RunCCLConfigs:   true,
		}
		err = t.addCclLogicTests("TestRepeatableReadLogicCCL", repeatableReadCalc)
		if err != nil {
			return err
		}
		err = t.addLogicTests("TestRepeatableReadLogic", repeatableReadCalc)
		if err != nil {
			return err
		}
		tenantCalc := logictestbase.ConfigCalculator{
			ConfigOverrides:       []string{"3node-tenant"},
			ConfigFilterOverrides: []string{"3node-tenant-multiregion"},
			RunCCLConfigs:         true,
		}
		err = t.addCclLogicTests("TestTenantLogicCCL", tenantCalc)
		if err != nil {
			return err
		}
		err = t.addLogicTests("TestTenantLogic", tenantCalc)
		if err != nil {
			return err
		}
		err = t.addExecBuildLogicTests("TestReadCommittedExecBuild", readCommittedCalc)
		if err != nil {
			return err
		}
		err = t.addExecBuildLogicTests("TestRepeatableReadExecBuild", repeatableReadCalc)
		if err != nil {
			return err
		}
		err = t.addExecBuildLogicTests("TestTenantExecBuild", tenantCalc)
		if err != nil {
			return err
		}
		err = t.dump()
		if err != nil {
			return err
		}
	}

	{
		t, err := gen.testdir("pkg/ccl/sqlitelogictestccl/tests")
		if err != nil {
			return err
		}
		err = t.addSqliteLogicTests("TestTenantSQLLiteLogic", logictestbase.ConfigCalculator{
			RunCCLConfigs:   true,
			ConfigOverrides: []string{"3node-tenant"},
		})
		if err != nil {
			return err
		}
		err = t.dump()
		if err != nil {
			return err
		}
	}

	{
		t, err := gen.testdir("pkg/sql/opt/exec/execbuilder/tests")
		if err != nil {
			return err
		}
		err = t.addExecBuildLogicTests("TestExecBuild", logictestbase.ConfigCalculator{})
		if err != nil {
			return err
		}
		err = t.dump()
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if err := generate(); err != nil {
		panic(err)
	}
}
