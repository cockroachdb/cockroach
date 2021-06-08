// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/datadriven"
	"github.com/spf13/cobra"
)

// TestSetupLogging checks the behavior of logging flags.
func TestSetupLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reWhitespace := regexp.MustCompile(`(?ms:((\s|\n)+))`)
	reWhitespace2 := regexp.MustCompile(`{\s+`)

	reSimplify := regexp.MustCompile(`(?ms:^\s*(auditable: false|redact: false|exit-on-error: true|max-group-size: 100MiB)\n)`)

	const defaultFluentConfig = `fluent-defaults: {` +
		`filter: INFO, ` +
		`format: json-fluent-compact, ` +
		`redactable: true, ` +
		`exit-on-error: false` +
		`}`
	stdFileDefaultsRe := regexp.MustCompile(
		`file-defaults: \{dir: (?P<path>[^,]+), max-file-size: 10MiB, buffered-writes: true, filter: INFO, format: crdb-v2, redactable: true\}`)
	fileDefaultsNoMaxSizeRe := regexp.MustCompile(
		`file-defaults: \{dir: (?P<path>[^,]+), buffered-writes: true, filter: INFO, format: crdb-v2, redactable: true\}`)
	const fileDefaultsNoDir = `file-defaults: {buffered-writes: true, filter: INFO, format: crdb-v2, redactable: true}`
	const defaultLogDir = `PWD/cockroach-data/logs`
	stdCaptureFd2Re := regexp.MustCompile(
		`capture-stray-errors: \{enable: true, dir: (?P<path>[^}]+)\}`)
	fileCfgRe := regexp.MustCompile(
		`\{channels: (?P<chans>all|\[[^]]*\]), dir: (?P<path>[^,]+), max-file-size: 10MiB, buffered-writes: (?P<buf>[^,]+), filter: INFO, format: (?P<format>[^,]+), redactable: true\}`)

	stderrCfgRe := regexp.MustCompile(
		`stderr: {channels: all, filter: (?P<level>[^,]+), format: crdb-v2-tty, redactable: (?P<redactable>[^}]+)}`)

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	pwd, err := filepath.Abs(wd)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	datadriven.RunTest(t, "testdata/logflags", func(t *testing.T, td *datadriven.TestData) string {
		args := strings.Split(td.Input, "\n")

		initCLIDefaults()
		cmd, flags, err := cockroachCmd.Find(args)
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.ParseFlags(flags); err != nil {
			t.Fatal(err)
		}
		log.TestingResetActive()
		if err := setupLogging(ctx, cmd, isServerCmd(cmd), false /* applyConfig */); err != nil {
			return "error: " + err.Error()
		}

		wantAmbiguous := td.HasArg("ambiguous")
		if cliCtx.ambiguousLogDir != wantAmbiguous {
			t.Errorf("%s: config expected as ambiguous=%v for logging directory, got ambiguous=%v",
				td.Pos,
				wantAmbiguous, cliCtx.ambiguousLogDir)
		}

		actual := cliCtx.logConfig.String()
		// Make the test independent of filesystem location.
		actual = strings.ReplaceAll(actual, pwd, "PWD")
		actual = strings.ReplaceAll(actual, defaultLogDir, "<defaultLogDir>")
		// Simplify - we don't care about all the configuration details
		// in this test.
		actual = reSimplify.ReplaceAllString(actual, "")

		// Flow: take the multi-line yaml output and make it "flowed".
		var h logconfig.Holder
		if err := h.Set(actual); err != nil {
			t.Fatal(err)
		}
		actual = reWhitespace.ReplaceAllString(h.String(), " ")
		actual = reWhitespace2.ReplaceAllString(actual, "{")

		// Shorten the configuration for legibility during reviews of test changes.
		actual = strings.ReplaceAll(actual, defaultFluentConfig, "<fluentDefaults>")
		actual = stdFileDefaultsRe.ReplaceAllString(actual, "<stdFileDefaults($path)>")
		actual = fileDefaultsNoMaxSizeRe.ReplaceAllString(actual, "<fileDefaultsNoMaxSize($path)>")
		actual = strings.ReplaceAll(actual, fileDefaultsNoDir, "<fileDefaultsNoDir>")
		actual = stdCaptureFd2Re.ReplaceAllString(actual, "<stdCaptureFd2($path)>")
		actual = fileCfgRe.ReplaceAllString(actual, "<fileCfg($chans,$path,$buf,$format)>")
		actual = stderrCfgRe.ReplaceAllString(actual, "<stderrCfg($level,$redactable)>")
		actual = strings.ReplaceAll(actual, `<stderrCfg(NONE,true)>`, `<stderrDisabled>`)
		actual = strings.ReplaceAll(actual, `<stderrCfg(INFO,false)>`, `<stderrEnabledInfoNoRedaction>`)
		actual = strings.ReplaceAll(actual, `<stderrCfg(WARNING,false)>`, `<stderrEnabledWarningNoRedaction>`)

		actual = strings.ReplaceAll(actual, ", ", ",\n")

		return actual
	})
}

func isServerCmd(thisCmd *cobra.Command) bool {
	for _, cmd := range serverCmds {
		if cmd == thisCmd {
			return true
		}
	}
	return false
}

// TestLogFlagCombinations checks that --log and --log-config-file properly
// override each other.
func TestLogFlagCombinations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	// Generate some random file content for the yaml input.
	const filecontents = "filecontents"
	tmpfile, err := ioutil.TempFile("", t.Name()+".yaml")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := os.Remove(tmpfile.Name()); err != nil {
			t.Fatal(err)
		}
	}()

	if _, err := tmpfile.Write([]byte(filecontents)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	f := startCmd.Flags()
	testData := []struct {
		args           []string
		expectedLogCfg string
	}{
		{[]string{"start"}, ""},
		{[]string{"start", "--log=foo"}, "foo"},
		{[]string{"start", "--log-config-file=" + tmpfile.Name()}, filecontents},
		{[]string{"start", "--log=foo", "--log=bar"}, "bar"},
		{[]string{"start", "--log=foo", "--log-config-file=" + tmpfile.Name()}, filecontents},
		{[]string{"start", "--log-config-file=" + tmpfile.Name(), "--log=bar"}, "bar"},
	}

	for i, td := range testData {
		initCLIDefaults()
		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		if td.expectedLogCfg != cliCtx.logConfigInput.s {
			t.Errorf("%d. cliCtx.logConfigInput.s expected '%s', but got '%s'. td.args was '%#v'.",
				i, td.expectedLogCfg, cliCtx.logConfigInput.s, td.args)
		}
	}
}

func Example_logging() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `--logtostderr=false`, `-e`, `select 1 as "1"`})
	c.RunWithArgs([]string{`sql`, `--logtostderr=true`, `-e`, `select 1 as "1"`})
	c.RunWithArgs([]string{`sql`, `--vmodule=foo=1`, `-e`, `select 1 as "1"`})

	// Output:
	// sql --logtostderr=false -e select 1 as "1"
	// 1
	// 1
	// sql --logtostderr=true -e select 1 as "1"
	// 1
	// 1
	// sql --vmodule=foo=1 -e select 1 as "1"
	// 1
	// 1
}
