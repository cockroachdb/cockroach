// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetupLogging checks the behavior of logging flags.
func TestSetupLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reWhitespace := regexp.MustCompile(`(?ms:((\s|\n)+))`)
	reBracketWhitespace := regexp.MustCompile(`(?P<bracket>[{[])\s+`)

	reSimplify := regexp.MustCompile(`(?ms:^\s*(auditable: false|redact: false|exit-on-error: true|max-group-size: 100MiB)\n)`)

	const defaultFluentConfig = `fluent-defaults: {` +
		`filter: INFO, ` +
		`format: json-fluent-compact, ` +
		`redactable: true, ` +
		`exit-on-error: false, ` +
		`buffering: {max-staleness: 5s, ` +
		`flush-trigger-size: 1.0MiB, ` +
		`max-buffer-size: 50MiB, ` +
		`format: newline}}`
	const defaultHTTPConfig = `http-defaults: {` +
		`method: POST, ` +
		`unsafe-tls: false, ` +
		`timeout: 2s, ` +
		`disable-keep-alives: false, ` +
		`compression: gzip, ` +
		`filter: INFO, ` +
		`format: json-compact, ` +
		`redactable: true, ` +
		`exit-on-error: false, ` +
		`buffering: {max-staleness: 5s, ` +
		`flush-trigger-size: 1.0MiB, ` +
		`max-buffer-size: 50MiB, ` +
		`format: newline}}`
	stdFileDefaultsRe := regexp.MustCompile(
		`file-defaults: \{` +
			`dir: (?P<path>[^,]+), ` +
			`max-file-size: 10MiB, ` +
			`file-permissions: "0640", ` +
			`buffered-writes: true, ` +
			`filter: INFO, ` +
			`format: crdb-v2, ` +
			`redactable: true, ` +
			`buffering: NONE\}`)
	fileDefaultsNoMaxSizeRe := regexp.MustCompile(
		`file-defaults: \{` +
			`dir: (?P<path>[^,]+), ` +
			`file-permissions: "0640", ` +
			`buffered-writes: true, ` +
			`filter: INFO, ` +
			`format: crdb-v2, ` +
			`redactable: true, ` +
			`buffering: NONE\}`)
	const fileDefaultsNoDir = `file-defaults: {` +
		`file-permissions: "0640", ` +
		`buffered-writes: true, ` +
		`filter: INFO, ` +
		`format: crdb-v2, ` +
		`redactable: true, ` +
		`buffering: NONE}`
	const defaultLogDir = `PWD/cockroach-data/logs`
	stdCaptureFd2Re := regexp.MustCompile(
		`capture-stray-errors: \{` +
			`enable: true, ` +
			`dir: (?P<path>[^}]+)\}`)
	fileCfgRe := regexp.MustCompile(
		`\{channels: \{(?P<chans>[^}]*)\}, ` +
			`dir: (?P<path>[^,]+), ` +
			`max-file-size: 10MiB, ` +
			`file-permissions: "0640", ` +
			`buffered-writes: (?P<buf>[^,]+), ` +
			`filter: INFO, ` +
			`format: (?P<format>[^,]+), ` +
			`redactable: true, ` +
			`buffering: NONE\}`)
	telemetryFileCfgRe := regexp.MustCompile(
		`\{channels: \{INFO: \[TELEMETRY\]\}, ` +
			`dir: (?P<path>[^,]+), ` +
			`max-file-size: 100KiB, ` +
			`max-group-size: 1.0MiB, ` +
			`file-permissions: "0640", ` +
			`buffered-writes: true, ` +
			`filter: INFO, ` +
			`format: crdb-v2, ` +
			`redactable: true, ` +
			`buffering: NONE\}`)

	stderrCfgRe := regexp.MustCompile(
		`stderr: {channels: \{(?P<level>[^:]+): all\}, ` +
			`filter: [^,]+, ` +
			`format: crdb-v2-tty, ` +
			`redactable: (?P<redactable>[^}]+), ` +
			`buffering: NONE}`)

	stderrCfgNoneRe := regexp.MustCompile(
		`stderr: {filter: NONE, ` +
			`format: crdb-v2-tty, ` +
			`redactable: (?P<redactable>[^}]+), ` +
			`buffering: NONE}`)

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	pwd, err := filepath.Abs(wd)
	if err != nil {
		t.Fatal(err)
	}

	require.NoError(t, os.Setenv("HOST_IP", "1.2.3.4"))
	defer func() {
		require.NoError(t, os.Unsetenv("HOST_IP"))
	}()

	ctx := context.Background()

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "logflags"), func(t *testing.T, td *datadriven.TestData) string {
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
		if isServerCmd(cmd) {
			// Since server commands copy store options into server configs in PersistentPreRunE,
			// we need to invoke those functions manually because logging relies on paths for the
			// first declared store.
			// The expectation here is that extraStoreFlagInit will be called in PersistentPreRunE
			// which is called before PreRunE where logging is normally initialized.
			require.NoError(t, extraStoreFlagInit(cmd))
		}
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
		actual = reBracketWhitespace.ReplaceAllString(actual, "$bracket")

		// Shorten the configuration for legibility during reviews of test changes.
		actual = strings.ReplaceAll(actual, defaultFluentConfig, "<fluentDefaults>")
		actual = strings.ReplaceAll(actual, defaultHTTPConfig, "<httpDefaults>")
		actual = stdFileDefaultsRe.ReplaceAllString(actual, "<stdFileDefaults($path)>")
		actual = fileDefaultsNoMaxSizeRe.ReplaceAllString(actual, "<fileDefaultsNoMaxSize($path)>")
		actual = strings.ReplaceAll(actual, fileDefaultsNoDir, "<fileDefaultsNoDir>")
		actual = stdCaptureFd2Re.ReplaceAllString(actual, "<stdCaptureFd2($path)>")
		actual = fileCfgRe.ReplaceAllString(actual, "<fileCfg($chans,$path,$buf,$format)>")
		actual = telemetryFileCfgRe.ReplaceAllString(actual, "<telemetryCfg($path)>")
		actual = stderrCfgRe.ReplaceAllString(actual, "<stderrCfg($level,$redactable)>")
		actual = stderrCfgNoneRe.ReplaceAllString(actual, "<stderrCfg(NONE,$redactable)>")
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

func TestValidateLogConfigVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i, tc := range []struct {
		vars        []string
		expectedErr error
	}{
		{
			vars: []string{"HOST_IP"},
		},
		{
			vars:        []string{"COCKROACH_TEST"},
			expectedErr: errors.Newf(`use of COCKROACH_TEST is not allowed as a logging configuration variable`),
		},
	} {
		err := validateLogConfigVars(tc.vars)

		if !errors.Is(tc.expectedErr, err) {
			t.Errorf("%d. validateLogConfigVars err expected '%s', but got '%s'.",
				i, tc.expectedErr, err)
		}
	}
}

func TestExpandEnvironmentVariables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	require.NoError(t, os.Setenv("HOST_IP1", "1.2.3.4"))
	require.NoError(t, os.Setenv("HOST_IP2", "5.6.7.8"))
	defer func() {
		require.NoError(t, os.Unsetenv("HOST_IP1"))
		require.NoError(t, os.Unsetenv("HOST_IP2"))
	}()
	require.NoError(t, os.Unsetenv("EXPAND_ABSENT_VAR1"))
	require.NoError(t, os.Unsetenv("EXPAND_ABSENT_VAR2"))

	for _, tc := range []struct {
		in             string
		vars           []string
		expectedOut    string
		expectedErrMsg string
	}{
		{
			in:          "$HOST_IP1",
			vars:        []string{"HOST_IP1"},
			expectedOut: "1.2.3.4",
		},
		{
			in:          "${HOST_IP2}",
			vars:        []string{"HOST_IP2"},
			expectedOut: "5.6.7.8",
		},
		{
			in:             "$EXPAND_ABSENT_VAR1",
			vars:           []string{"EXPAND_ABSENT_VAR1"},
			expectedErrMsg: `variable "EXPAND_ABSENT_VAR1" is not defined in environment`,
		},
		{
			in:             "${EXPAND_ABSENT_VAR2}",
			vars:           []string{"EXPAND_ABSENT_VAR2"},
			expectedErrMsg: `variable "EXPAND_ABSENT_VAR2" is not defined in environment`,
		},
		{
			in:             "$HOST_IP3",
			expectedErrMsg: `unknown variable "HOST_IP3" used in configuration`,
		},
		{
			in:             "${HOST_IP4}",
			expectedErrMsg: `unknown variable "HOST_IP4" used in configuration`,
		},
	} {
		out, err := expandEnvironmentVariables(tc.in, tc.vars)

		if tc.expectedErrMsg != "" {
			assert.EqualError(t, err, tc.expectedErrMsg)
		} else {
			assert.Nil(t, err)
		}
		assert.Equal(t, tc.expectedOut, out)
	}
}

// TestLogFlagCombinations checks that --log and --log-config-file properly
// override each other and that --log-config-vars stores the appropriate values
// in the cliContext struct.
func TestLogFlagCombinations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	// Generate some random file content for the yaml input.
	const filecontents = "filecontents"
	tmpfile, err := os.CreateTemp("", t.Name()+".yaml")
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
		args            []string
		expectedLogCfg  string
		expectedLogVars []string
	}{
		{
			args:           []string{"start"},
			expectedLogCfg: "",
		},
		{
			args:           []string{"start", "--log=foo"},
			expectedLogCfg: "foo",
		},
		{
			args:           []string{"start", "--log-config-file=" + tmpfile.Name()},
			expectedLogCfg: filecontents,
		},
		{
			args:           []string{"start", "--log=foo", "--log=bar"},
			expectedLogCfg: "bar",
		},
		{
			args:           []string{"start", "--log=foo", "--log-config-file=" + tmpfile.Name()},
			expectedLogCfg: filecontents,
		},
		{
			args:           []string{"start", "--log-config-file=" + tmpfile.Name(), "--log=bar"},
			expectedLogCfg: "bar",
		},
		{
			args:            []string{"start", "--log-config-file=" + tmpfile.Name(), "--log-config-vars=HOST_IP"},
			expectedLogCfg:  filecontents,
			expectedLogVars: []string{"HOST_IP"},
		},
		{
			args:            []string{"start", "--log-config-file=" + tmpfile.Name(), "--log-config-vars=HOST_IP,POD_NAME"},
			expectedLogCfg:  filecontents,
			expectedLogVars: []string{"HOST_IP", "POD_NAME"},
		},
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

		if !reflect.DeepEqual(td.expectedLogVars, cliCtx.logConfigVars) {
			t.Errorf("%d. cliCtx.logConfigVars expected '%s', but got '%s'. td.args was '%#v'.",
				i, td.expectedLogCfg, cliCtx.logConfigVars, td.args)
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
