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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// TestSetupLogging checks the behavior of logging flags.
func TestSetupLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stdFileDefaults := func(path string) string {
		return `
file-defaults: { dir: ` + path + `, max-file-size: 10MiB, filter: INFO, format: crdb-v1, redactable: true }
`
	}
	fileDefaultsNoMaxSize := func(path string) string {
		return `
file-defaults: { dir: ` + path + `, filter: INFO, format: crdb-v1, redactable: true }
`
	}
	const fileDefaultsNoDir = `
file-defaults: { filter: INFO, format: crdb-v1, redactable: true }
`

	stdCaptureFd2 := func(path string) string {
		return `
capture-stray-errors: { enable: true, dir: ` + path + ` }
`
	}

	fileCfg := func(chans, path string, audit bool) string {
		sync, format := "false", "crdb-v1"
		if audit {
			sync, format = "true", "crdb-v1-count"
		}
		return `{ channels: '` + chans + `',  dir: ` + path + `,
max-file-size: 10MiB, sync-writes: ` + sync + `, filter: INFO, format: ` + format + `, redactable: true }`
	}

	defaultLogFiles := func(path string) string {
		return `default: ` + fileCfg("all except STORAGE,SENSITIVE_ACCESS,SESSIONS,SQL_EXEC,SQL_PERF,SQL_INTERNAL_PERF", path, false) + `,
pebble: ` + fileCfg("STORAGE", path, false) + `,
sql-audit: ` + fileCfg("SENSITIVE_ACCESS", path, true) + `,
sql-auth: ` + fileCfg("SESSIONS", path, true) + `,
sql-exec: ` + fileCfg("SQL_EXEC", path, false) + `,
sql-slow: ` + fileCfg("SQL_PERF", path, false) + `,
sql-slow-internal-only: ` + fileCfg("SQL_INTERNAL_PERF", path, false) + `
`
	}
	defaultLogFileNoMaxSize := func(path string) string {
		return `
default: { channels: all,  dir: ` + path + `,
sync-writes: false, filter: INFO, format: crdb-v1, redactable: true }
`
	}

	stderrCfg := func(level string, redactable string) string {
		return `
stderr: { channels: all, filter: ` + level + `, format: crdb-v1-tty, redactable: ` + redactable + ` }`
	}
	stderrDisabled := stderrCfg("NONE", "true")
	stderrEnabledInfoNoRedaction := stderrCfg("INFO", "false")
	stderrEnabledWarningNoRedaction := stderrCfg("WARNING", "false")

	const defaultLogDir = `PWD/cockroach-data/logs`

	testData := []struct {
		args              []string
		expectedAmbiguous bool
		expected          string
	}{
		// Legacy flags and --log cannot be used together.
		{[]string{"start", "--logtostderr=INFO", "--log=file-defaults: dir: /tmp"},
			false,
			"error: --log is incompatible with legacy discrete logging flags"},

		// Default parameters for server commands.
		{[]string{"start"}, false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(defaultLogDir)},
		{[]string{"start-single-node"}, false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(defaultLogDir)},

		// Default parameters for client commands.
		{[]string{"sql"}, false,
			fileDefaultsNoDir + "sinks: {" + stderrEnabledWarningNoRedaction + "}"},
		{[]string{"init"}, false,
			fileDefaultsNoDir + "sinks: {" + stderrEnabledWarningNoRedaction + "}"},
		// Special case is "workload" and its sub-commands. It logs to stderr
		// with level INFO by default. (Legacy behavior)
		{[]string{"workload", "run", "bank"}, false,
			fileDefaultsNoDir + "sinks: {" + stderrEnabledInfoNoRedaction + "}"},
		// Special case is "demo" and its sub-commands. It disables
		// logging to stderr.
		{[]string{"demo"}, false,
			fileDefaultsNoDir + "sinks: {" + stderrCfg("NONE", "false") + "}"},

		// When "start" is used with no on-disk store, derive no file logging.
		// Also for server commands the default stderr level is INFO.
		{[]string{"start", "--store=type=mem,size=3g"},
			false,
			fileDefaultsNoDir + "sinks: {" + stderrEnabledInfoNoRedaction + "}"},
		// If there are multiple on-disk stores, the first one is used.
		{[]string{"start", "--store=path=/pathA", "--store=path=/pathB"},
			true, /* configuration is ambiguous */
			stdFileDefaults(`/pathA/logs`) +
				"sinks: { file-groups: {" + defaultLogFiles(`/pathA/logs`) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(`/pathA/logs`)},

		// It is possible to override the output directory also via --log.
		{[]string{"start", "--log=file-defaults: {dir: /mypath}"},
			false,
			stdFileDefaults(`/mypath`) +
				"sinks: { file-groups: {" + defaultLogFiles(`/mypath`) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(`/mypath`)},
		// If there were multiple stores, configuring via --log disambiguates.
		{[]string{"start", "--store=path=/pathA", "--store=path=/pathB",
			"--log=file-defaults: {dir: /pathA}"},
			false, /* not ambiguous any more */
			stdFileDefaults(`/pathA`) +
				"sinks: { file-groups: {" + defaultLogFiles(`/pathA`) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(`/pathA`)},
		// If the specified log directory is completely different, the
		// configuration is not ambiguous either. We need the different
		// test case because the ambiguity condition is detected
		// differenty whether we use the same directory name as the first
		// store or not.
		{[]string{"start", "--store=path=/pathA", "--store=path=/pathB",
			"--log=file-defaults: {dir: /mypath}"},
			false, /* not ambiguous any more */
			stdFileDefaults(`/mypath`) +
				"sinks: { file-groups: {" + defaultLogFiles(`/mypath`) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(`/mypath`)},

		// It's possible to override the stderr threshold.
		{[]string{"start", "--log=sinks: {stderr: {filter: ERROR}}"},
			false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrCfg("ERROR", "true") + "}" + stdCaptureFd2(defaultLogDir)},

		// It's possible to disable the stderr capture.
		{[]string{"start", "--log=capture-stray-errors: {enable: false}"},
			false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrDisabled + "}"},

		// Logging to stderr without stderr capture causes an error in the default config.
		{[]string{"start", "--log=capture-stray-errors: {enable: false}\nsinks: {stderr: {filter: INFO}}"},
			false,
			"error: stderr.redactable cannot be set if capture-stray-errors.enable is unset"},
		// This configuration becomes possible if redactability is explicitly retracted.
		{[]string{"start", "--log=capture-stray-errors: {enable: false}\nsinks: {stderr: {filter: INFO, redactable: false}}"},
			false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrCfg("INFO", "false") + "}"},

		// Legacy config flags follow.
		// TODO(knz): Deprecated in v21.1. Remove in v21.2.

		// Server commands have a logging directory by
		// default. --log-dir="" removes it.
		{[]string{"start", "--log-dir="}, false,
			fileDefaultsNoDir + "sinks: {" + stderrEnabledInfoNoRedaction + "}"},
		// For server commands, --log-dir can also override the directory.
		{[]string{"start", "--log-dir=/mypath"}, false,
			stdFileDefaults(`/mypath`) +
				"sinks: { file-groups: {" + defaultLogFiles(`/mypath`) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(`/mypath`)},
		// If there were multiple stores, this disambiguates.
		{[]string{"start", "--store=path=/pathA", "--store=path=/pathB",
			"--log-dir=/pathA"},
			false, /* not ambiguous any more */
			stdFileDefaults(`/pathA`) +
				"sinks: { file-groups: {" + defaultLogFiles(`/pathA`) + "}," +
				stderrDisabled + "}" + stdCaptureFd2(`/pathA`)},

		// Client commands have no logging directory by default.
		// --log-dir adds one. However, the file configurations are not server-like:
		// the max sizes don't apply.
		{[]string{"init", "--log-dir=/mypath"}, false,
			fileDefaultsNoMaxSize(`/mypath`) +
				"sinks: { file-groups: {" + defaultLogFileNoMaxSize(`/mypath`) + "}," +
				stderrEnabledWarningNoRedaction + "}"},

		// For servers, --logtostderr overrides the threshold and keeps
		// redaction markers.
		{[]string{"start", "--logtostderr=INFO"}, false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrCfg("INFO", "true") + "}" + stdCaptureFd2(defaultLogDir)},
		// Default when no severity is specified is WARNING.
		{[]string{"start", "--logtostderr"}, false,
			stdFileDefaults(defaultLogDir) +
				"sinks: { file-groups: {" + defaultLogFiles(defaultLogDir) + "}," +
				stderrCfg("INFO", "true") + "}" + stdCaptureFd2(defaultLogDir)},

		// For clients, --logtostderr overrides the threshold.
		{[]string{"init", "--logtostderr=INFO"}, false,
			fileDefaultsNoDir + "sinks: {" + stderrCfg("INFO", "false") + "}"},
		// Default when no severity is specified is WARNING.
		{[]string{"init", "--logtostderr"}, false,
			fileDefaultsNoDir + "sinks: {" + stderrCfg("WARNING", "false") + "}"},
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	pwd, err := filepath.Abs(wd)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for _, tc := range testData {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			initCLIDefaults()
			cmd, flags, err := cockroachCmd.Find(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.ParseFlags(flags); err != nil {
				t.Fatal(err)
			}
			var actual string
			expected := tc.expected
			log.TestingResetActive()
			if err := setupLogging(ctx, cmd, isServerCmd(cmd), false /* applyConfig */); err != nil {
				actual = "error: " + err.Error()
			} else {
				actual = cliCtx.logConfig.String()
				// Make the test independent of filesystem location.
				actual = strings.ReplaceAll(actual, pwd, "PWD")
				// Simplify - we don't care about all the configuration details
				// in this test.
				actual = strings.ReplaceAll(actual, defaultFluentConfig, "")
				actual = reSimplify.ReplaceAllString(actual, "")
				expected = reflowYAML(t, expected)
			}
			require.Equal(t, tc.expectedAmbiguous, cliCtx.ambiguousLogDir)
			require.Equal(t, expected, actual)
		})
	}
}

var reSimplify = regexp.MustCompile(`(?ms:^\s*(auditable: false|redact: false|exit-on-error: true|max-group-size: 100MiB)\n)`)

const defaultFluentConfig = `fluent-defaults:
  filter: INFO
  format: json-fluent-compact
  redact: false
  redactable: true
  exit-on-error: false
  auditable: false
`

// reflowYAML takes a yaml config in "flow" form and
// restructures it unflowed.
func reflowYAML(t *testing.T, s string) string {
	var x logconfig.Config
	if err := yaml.Unmarshal([]byte(s), &x); err != nil {
		t.Fatal(err)
	}
	b, err := yaml.Marshal(&x)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func isServerCmd(thisCmd *cobra.Command) bool {
	for _, cmd := range serverCmds {
		if cmd == thisCmd {
			return true
		}
	}
	return false
}
