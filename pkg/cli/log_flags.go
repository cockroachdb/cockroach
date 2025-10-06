// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
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
	if cliCtx.logOverrides.anySet() &&
		cliCtx.logConfigInput.isSet {
		return errors.Newf("--%s is incompatible with legacy discrete logging flags", cliflags.Log.Name)
	}

	if err := validateLogConfigVars(cliCtx.logConfigVars); err != nil {
		return errors.Wrap(err, "invalid logging configuration")
	}

	// Sanity check to prevent misuse of API.
	if active, firstUse := log.IsActive(); active {
		logcrash.ReportOrPanic(ctx, nil /* sv */, "logging already active; first used at:\n%s", firstUse)
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
	forceDisableLogDir := cliCtx.logOverrides.logDir.isSet &&
		cliCtx.logOverrides.logDir.s == ""
	if forceDisableLogDir {
		defaultLogDir = nil
	}
	forceSetLogDir := cliCtx.logOverrides.logDir.isSet &&
		cliCtx.logOverrides.logDir.s != ""
	if forceSetLogDir {
		ambiguousLogDirs = false
		defaultLogDir = &cliCtx.logOverrides.logDir.s
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

	if isDemoCmd(cmd) || cmd == genMetricListCmd {
		// `cockroach demo` and `cockroach gen metric-list` are special:
		// they start a server, but without
		// disk and interactively. We don't want to litter the console
		// with warning or error messages unless overridden; however,
		// should the command encounter a log.Fatal event, we want
		// to inform the user (who is guaranteed to be looking at the screen).
		//
		// NB: this can be overridden from the command line as usual.
		h.Config.Sinks.Stderr.Filter = severity.FATAL
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
	cliCtx.logOverrides.propagate(&h.Config, commandSpecificDefaultLegacyStderrOverride)

	// If a configuration was specified via --log, load it.
	if cliCtx.logConfigInput.isSet {
		s := cliCtx.logConfigInput.s

		if len(cliCtx.logConfigVars) > 0 {
			var err error
			s, err = expandEnvironmentVariables(s, cliCtx.logConfigVars)
			if err != nil {
				return errors.Wrap(err, "unable to expand environment variables")
			}
		}

		if err := h.Set(s); err != nil {
			return err
		}
		if h.Config.FileDefaults.Dir != nil {
			ambiguousLogDirs = false
		}
	}

	// Legacy behavior: if no files were specified by the configuration,
	// add some pre-defined files in servers.
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
	// directories exist. We also initialize write metrics for each directory.
	fileSinkMetricsForDir := make(map[string]log.FileSinkMetrics)
	if err := h.Config.IterateDirectories(func(logDir string) error {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return errors.Wrap(err, "unable to create log directory")
		}
		writeStatsCollector, err := serverCfg.DiskWriteStats.GetOrCreateCollector(logDir)
		if err != nil {
			return errors.Wrap(err, "unable to get stats collector for log directory")
		}
		if _, ok := fileSinkMetricsForDir[logDir]; !ok {
			logBytesWritten := writeStatsCollector.CreateStat(fs.CRDBLogWriteCategory)
			metric := log.FileSinkMetrics{LogBytesWritten: logBytesWritten}
			fileSinkMetricsForDir[logDir] = metric
		}
		return nil
	}); err != nil {
		return err
	}

	// Configuration ready and directories exist; apply it.
	fatalOnLogStall := func() bool {
		return fs.MaxSyncDurationFatalOnExceeded.Get(&serverCfg.Settings.SV)
	}
	logShutdownFn, err := log.ApplyConfig(h.Config, fileSinkMetricsForDir, fatalOnLogStall)
	if err != nil {
		return err
	}
	cliCtx.logShutdownFn = logShutdownFn

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
		if fc.Channels.AllChannels.HasChannel(channel.DEV) && fc.Dir != nil && *fc.Dir != "" {
			outputDirectory = *fc.Dir
			break
		}
	}
	serverCfg.GoroutineDumpDirName = filepath.Join(outputDirectory, base.GoroutineDumpDir)
	serverCfg.HeapProfileDirName = filepath.Join(outputDirectory, base.HeapProfileDir)
	serverCfg.CPUProfileDirName = filepath.Join(outputDirectory, base.CPUProfileDir)
	serverCfg.InflightTraceDirName = filepath.Join(outputDirectory, base.InflightTraceDir)

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
	b, err := os.ReadFile(s)
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
	h := logconfig.Holder{Config: *c}
	if err := h.Set(predefinedLogFiles); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "programming error: incorrect config"))
	}
	*c = h.Config
	if cliCtx.logOverrides.sqlAuditLogDir.isSet {
		c.Sinks.FileGroups["sql-audit"].Dir = &cliCtx.logOverrides.sqlAuditLogDir.s
	}
}

// predefinedLogFiles are the files defined when the --log flag
// does not otherwise override the file sinks.
const predefinedLogFiles = `
sinks:
 file-groups:
  kv-distribution:        { channels: KV_DISTRIBUTION }
  default:
    channels:
      INFO: [DEV, OPS]
      WARNING: all except [DEV, OPS]
  health:                 { channels: HEALTH  }
  pebble:                 { channels: STORAGE }
  security:               { channels: [PRIVILEGES, USER_ADMIN], auditable: true  }
  sql-auth:               { channels: SESSIONS, auditable: true }
  sql-audit:              { channels: SENSITIVE_ACCESS, auditable: true }
  sql-exec:               { channels: SQL_EXEC }
  sql-schema:             { channels: SQL_SCHEMA }
  sql-slow:               { channels: SQL_PERF }
  sql-slow-internal-only: { channels: SQL_INTERNAL_PERF }
  telemetry:
    channels: TELEMETRY
    max-file-size: 102400
    max-group-size: 1048576
`

// validateLogConfigVars return an error if any of the passed logging
// configuration variables are are not permissible. For security, variables
// that start with COCKROACH_ are explicitly disallowed. See #81146 for more.
func validateLogConfigVars(vars []string) error {
	for _, v := range vars {
		if strings.HasPrefix(strings.ToUpper(v), "COCKROACH_") {
			return errors.Newf("use of %s is not allowed as a logging configuration variable", v)
		}
	}
	return nil
}

// expandEnvironmentVariables replaces variables used in string with their
// values pulled from the environment. If there are variables in the string
// that are not contained in vars, they will be replaced with the empty string.
func expandEnvironmentVariables(s string, vars []string) (string, error) {
	var err error

	m := map[string]string{}
	// Only pull variable values from the environment if their key is present
	// in vars to create an allow list.
	for _, k := range vars {
		v, ok := envutil.ExternalEnvString(k, 1)
		if !ok {
			err = errors.CombineErrors(err, errors.Newf("variable %q is not defined in environment", k))
			continue
		}
		m[k] = v
	}

	s = os.Expand(s, func(k string) string {
		v, ok := m[k]
		if !ok {
			err = errors.CombineErrors(err, errors.Newf("unknown variable %q used in configuration", k))
			return ""
		}
		return v
	})

	return s, err
}
