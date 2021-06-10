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
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// setupLogging configures logging.
//
// The applyConfig argument, if unset, causes the function to merely
// validate the configuration but does not actually enable it. This is
// used by 'debug check-log-config' to display the result of
// validation.
//
// The command then further distinguishes between server (e.g. start)
// and non-server commands (e.g. 'node ls').
func setupLogging(ctx context.Context, cmd *cobra.Command, isServerCmd, applyConfig bool) error {
	// Compatibility check for command-line usage.
	if cliCtx.deprecatedLogOverrides.anySet() &&
		cliCtx.logConfigInput.isSet {
		return errors.Newf("--%s is incompatible with legacy discrete logging flags", cliflags.Log.Name)
	}

	// Sanity check to prevent misuse of API.
	if active, firstUse := log.IsActive(); active {
		panic(errors.Newf("logging already active; first used at:\n%s", firstUse))
	}

	// Try to derive a default directory from the first store,
	// if we have a server command.
	var firstStoreDir *string
	var ambiguousLogDirs bool
	if isServerCmd {
		firstStoreDir, ambiguousLogDirs = getDefaultLogDirFromStores()
	}
	defaultLogDir := firstStoreDir

	// Legacy log directory override.
	// TODO(knz): Deprecated in v21.1. Remove in v21.2.
	forceDisableLogDir := cliCtx.deprecatedLogOverrides.logDir.isSet &&
		cliCtx.deprecatedLogOverrides.logDir.s == ""
	if forceDisableLogDir {
		defaultLogDir = nil
	}
	forceSetLogDir := cliCtx.deprecatedLogOverrides.logDir.isSet &&
		cliCtx.deprecatedLogOverrides.logDir.s != ""
	if forceSetLogDir {
		ambiguousLogDirs = false
		defaultLogDir = &cliCtx.deprecatedLogOverrides.logDir.s
	}

	// Set up a base configuration template.
	h := logconfig.Holder{Config: logconfig.DefaultConfig()}
	if !isServerCmd || defaultLogDir == nil {
		// Client commands, as well as servers without an on-disk store directory,
		// default to output to stderr, with no capture of stray errors.
		h.Config = logconfig.DefaultStderrConfig()
	}

	// commandSpecificDefaultLegacyStderrOverride is used when --logtostderr (legacy
	// flag) is passed without argument, see below.
	commandSpecificDefaultLegacyStderrOverride := severity.INFO

	if isDemoCmd(cmd) {
		// `cockroach demo` is special: it starts a server, but without
		// disk and interactively. We don't want to litter the console
		// with warning or error messages unless overridden.
		h.Config.Sinks.Stderr.Filter = severity.NONE
	} else if !isServerCmd && !isWorkloadCmd(cmd) {
		// Client commands don't typically have a log directory so they
		// naturally default to stderr logging. However, we don't want
		// anything less serious than a warning to be displayed unless
		// the user gives an override.
		//
		// The special case for 'workload' is legacy behavior: INFO
		// verbosity on stderr.
		//
		// TODO(knz): This behavior is deprecated in v21.1. Remove the conditional
		// and treat as regular client command.

		// This default applies when neither --log nor --logtostderr is applied.
		h.Config.Sinks.Stderr.Filter = severity.WARNING
		// This default applies below when --logtostderr is applied without argument.
		commandSpecificDefaultLegacyStderrOverride = severity.WARNING
	}

	// If some overrides were specified via the discrete flags,
	// apply them.
	//
	// NB: this is for backward-compatibility and is deprecated in
	// v21.1.
	// TODO(knz): Remove this.
	cliCtx.deprecatedLogOverrides.propagate(&h.Config, commandSpecificDefaultLegacyStderrOverride)

	// If a configuration was specified via --log, load it.
	if cliCtx.logConfigInput.isSet {
		if err := h.Set(cliCtx.logConfigInput.s); err != nil {
			return err
		}
		if h.Config.FileDefaults.Dir != nil {
			ambiguousLogDirs = false
		}
	}

	// Legacy behavior: if no files were specified by the configuration,
	// add some pre-defined files in servers.
	// TODO(knz): Deprecated in v21.1. Remove this.
	if isServerCmd && len(h.Config.Sinks.FileGroups) == 0 {
		addPredefinedLogFiles(&h.Config)
	}

	// Our configuration is complete. Validate it.
	// This ensures that all optional fields are populated and
	// non-specified flags are inherited from defaults.
	if err := h.Config.Validate(defaultLogDir); err != nil {
		return err
	}

	// Store the result configuration so that the start code and debug
	// check-log-config can see it.
	cliCtx.logConfig = h.Config

	// Was the default directory used in a context where there were
	// multiple stores defined?
	if ambiguousLogDirs && firstStoreDir != nil {
		firstStoreDirUsed := false
		if firstStoreAbs, err := filepath.Abs(*firstStoreDir); err == nil {
			_ = h.Config.IterateDirectories(func(logDir string) error {
				firstStoreDirUsed = firstStoreDirUsed || logDir == firstStoreAbs
				return nil
			})
		}
		if firstStoreDirUsed {
			cliCtx.ambiguousLogDir = true
		}
	}

	// Configuration is complete and valid. If we are not applying
	// (debug check-log-config), stop here.
	if !applyConfig {
		return nil
	}

	// Configuration is ready to be applied. Ensure that the output log
	// directories exist.
	if err := h.Config.IterateDirectories(func(logDir string) error {
		return os.MkdirAll(logDir, 0755)
	}); err != nil {
		return errors.Wrap(err, "unable to create log directory")
	}

	// Configuration ready and directories exist; apply it.
	if _, err := log.ApplyConfig(h.Config); err != nil {
		return err
	}

	// If using a custom config, report the configuration at the start of the logging stream.
	if cliCtx.logConfigInput.isSet {
		log.Ops.Infof(ctx, "using explicit logging configuration:\n%s", cliCtx.logConfigInput.s)
	}

	if cliCtx.ambiguousLogDir {
		// Note that we can't report this message earlier, because the log directory
		// may not have been ready before the call to MkdirAll() above.
		log.Ops.Shout(ctx, severity.WARNING,
			"multiple stores configured, "+
				"you may want to specify --log='file-defaults: {dir: ...}' to disambiguate.")
	}

	// Use the file sink for the DEV channel to generate goroutine dumps
	// and heap profiles.
	//
	// We want to be careful to still produce useful debug dumps if the
	// server configuration has disabled logging to files. In that case,
	// we use the store directory if there is an on-disk store, or
	// the current directory if there is no store.
	outputDirectory := "."
	if firstStoreDir != nil {
		outputDirectory = *firstStoreDir
	}
	for _, fc := range h.Config.Sinks.FileGroups {
		if fc.Channels.HasChannel(channel.DEV) && fc.Dir != nil && *fc.Dir != "" {
			outputDirectory = *fc.Dir
			break
		}
	}
	serverCfg.GoroutineDumpDirName = filepath.Join(outputDirectory, base.GoroutineDumpDir)
	serverCfg.HeapProfileDirName = filepath.Join(outputDirectory, base.HeapProfileDir)
	serverCfg.CPUProfileDirName = filepath.Join(outputDirectory, base.CPUProfileDir)

	return nil
}

// getDefaultLogDirFromStores derives a log directory path from the
// configure first on-disk store. If more than one on-disk store is
// defined, the ambiguousLogDirs return value is true.
func getDefaultLogDirFromStores() (dir *string, ambiguousLogDirs bool) {
	// Default the log directory to the "logs" subdirectory of the first
	// non-memory store.
	for _, spec := range serverCfg.Stores.Specs {
		if spec.InMemory {
			continue
		}
		if dir != nil {
			ambiguousLogDirs = true
			break
		}
		s := filepath.Join(spec.Path, "logs")
		dir = &s
	}

	return
}

// logConfigFlags makes it possible to override the
// configuration with discrete command-line flags.
//
// This struct interfaces between the YAML-based configuration
// mechanism in package 'logconfig', and the logic that initializes
// the logging system.
//
// TODO(knz): Deprecated in v21.1. Remove this.
type logConfigFlags struct {
	// Override value of file-defaults:dir.
	logDir settableString

	// Override value for sinks:sql-audit:dir.
	sqlAuditLogDir settableString

	// Override value of file-defaults:max-file-size.
	fileMaxSize    int64
	fileMaxSizeVal *humanizeutil.BytesValue

	// Override value of file-defaults:max-group-size.
	maxGroupSize    int64
	maxGroupSizeVal *humanizeutil.BytesValue

	// Override value of file-defaults:threshold.
	fileThreshold log.Severity

	// Override value of sinks:stderr:threshold.
	stderrThreshold log.Severity

	// Override value of sinks:stderr:no-color.
	stderrNoColor settableBool

	// Override value of file-defaults:redactable.
	redactableLogs settableBool
}

// newLogConfigOverrides defines a new logConfigFlags
// from the default logging configuration.
//
// TODO(knz): Deprecated in v21.1. Remove this.
func newLogConfigOverrides() *logConfigFlags {
	l := &logConfigFlags{}
	l.fileMaxSizeVal = humanizeutil.NewBytesValue(&l.fileMaxSize)
	l.maxGroupSizeVal = humanizeutil.NewBytesValue(&l.maxGroupSize)
	l.reset()
	return l
}

func (l *logConfigFlags) reset() {
	d := logconfig.DefaultConfig()

	l.logDir = settableString{}
	l.sqlAuditLogDir = settableString{}
	*l.fileMaxSizeVal = *humanizeutil.NewBytesValue(&l.fileMaxSize)
	*l.maxGroupSizeVal = *humanizeutil.NewBytesValue(&l.maxGroupSize)
	l.fileMaxSize = int64(*d.FileDefaults.MaxFileSize)
	l.maxGroupSize = int64(*d.FileDefaults.MaxGroupSize)
	l.fileThreshold = severity.UNKNOWN
	l.stderrThreshold = severity.UNKNOWN
	l.stderrNoColor = settableBool{}
	l.redactableLogs = settableBool{}
}

func (l *logConfigFlags) anySet() bool {
	return l.logDir.isSet ||
		l.sqlAuditLogDir.isSet ||
		l.fileMaxSizeVal.IsSet() ||
		l.maxGroupSizeVal.IsSet() ||
		l.fileThreshold.IsSet() ||
		l.stderrThreshold.IsSet() ||
		l.stderrNoColor.isSet ||
		l.redactableLogs.isSet
}

// propagate applies the overrides into a configuration. This must be
// called on the logconfig.Config object before it undergoes
// Validate().
func (l *logConfigFlags) propagate(
	c *logconfig.Config, commandSpecificDefaultLegacyStderrOverride log.Severity,
) {
	if l.logDir.isSet && l.logDir.s != "" {
		// Note: the case where .s == "" is handled in setupLogging()
		// above.
		c.FileDefaults.Dir = &l.logDir.s
	}
	if l.fileMaxSizeVal.IsSet() {
		s := logconfig.ByteSize(l.fileMaxSize)
		c.FileDefaults.MaxFileSize = &s
	}
	if l.maxGroupSizeVal.IsSet() {
		s := logconfig.ByteSize(l.maxGroupSize)
		c.FileDefaults.MaxGroupSize = &s
	}
	if l.fileThreshold.IsSet() {
		c.FileDefaults.Filter = l.fileThreshold
	}
	if l.stderrThreshold.IsSet() {
		if l.stderrThreshold == severity.DEFAULT {
			c.Sinks.Stderr.Filter = commandSpecificDefaultLegacyStderrOverride
		} else {
			c.Sinks.Stderr.Filter = l.stderrThreshold
		}
	}
	if l.stderrNoColor.isSet {
		c.Sinks.Stderr.NoColor = l.stderrNoColor.val
	}
	if l.redactableLogs.isSet {
		c.FileDefaults.Redactable = &l.redactableLogs.val
		c.Sinks.Stderr.Redactable = &l.redactableLogs.val
	}
}

// settableBool represents a string that can be set from the command line.
type settableString struct {
	s     string
	isSet bool
}

type stringValue struct {
	*settableString
}

var _ flag.Value = (*stringValue)(nil)
var _ flag.Value = (*fileContentsValue)(nil)

// Set implements the pflag.Value interface.
func (l *stringValue) Set(s string) error {
	l.s = s
	l.isSet = true
	return nil
}

// Type implements the pflag.Value interface.
func (l stringValue) Type() string { return "<string>" }

// String implements the pflag.Value interface.
func (l stringValue) String() string { return l.s }

type fileContentsValue struct {
	*settableString
	fileName string
}

// Set implements the pflag.Value interface.
func (l *fileContentsValue) Set(s string) error {
	l.fileName = s
	b, err := ioutil.ReadFile(s)
	if err != nil {
		return err
	}
	l.s = string(b)
	l.isSet = true
	return nil
}

// Type implements the pflag.Value interface.
func (l fileContentsValue) Type() string { return "<file>" }

// String implements the pflag.Value interface.
func (l fileContentsValue) String() string { return l.fileName }

// settableBool represents a boolean that can be set from the command line.
type settableBool struct {
	val   bool
	isSet bool
}

// String implements the pflag.Value interface.
func (s settableBool) String() string { return strconv.FormatBool(s.val) }

// Type implements the pflag.Value interface.
func (s settableBool) Type() string { return "bool" }

// Set implements the pflag.Value interface.
func (s *settableBool) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return err
	}
	s.val = b
	s.isSet = true
	return nil
}

func addPredefinedLogFiles(c *logconfig.Config) {
	if c.Sinks.FileGroups == nil {
		c.Sinks.FileGroups = make(map[string]*logconfig.FileSinkConfig)
	}
	m := c.Sinks.FileGroups
	for ch, prefix := range predefinedLogFiles {
		b := predefinedAuditFiles[ch]
		// Legacy behavior: the --sql-audit-dir overrides the directory
		// for just this sink.
		var dir *string
		if prefix == "sql-audit" && cliCtx.deprecatedLogOverrides.sqlAuditLogDir.isSet {
			dir = &cliCtx.deprecatedLogOverrides.sqlAuditLogDir.s
		}
		m[prefix] = &logconfig.FileSinkConfig{
			Channels: logconfig.ChannelList{Channels: []logpb.Channel{ch}},
			FileDefaults: logconfig.FileDefaults{
				Dir: dir,
				CommonSinkConfig: logconfig.CommonSinkConfig{
					Auditable: &b,
				},
			},
		}
	}
}

// predefinedLogFiles are the files defined when the --log flag
// does not otherwise override the file sinks.
var predefinedLogFiles = map[logpb.Channel]string{
	channel.STORAGE:           "pebble",
	channel.SESSIONS:          "sql-auth",
	channel.SENSITIVE_ACCESS:  "sql-audit",
	channel.SQL_EXEC:          "sql-exec",
	channel.SQL_PERF:          "sql-slow",
	channel.SQL_INTERNAL_PERF: "sql-slow-internal-only",
}

// predefinedAuditFiles indicate which channel-specific files are
// preconfigured as "audit" logs: with event numbering and with
// synchronous writes. Audit logs are configured this way to ensure
// non-repudiability.
var predefinedAuditFiles = map[logpb.Channel]bool{
	channel.SESSIONS: true,
	// TODO(knz): add the PRIVILEGES channel.
	//  channel.PRIVILEGES:       true,
	channel.SENSITIVE_ACCESS: true,
}
