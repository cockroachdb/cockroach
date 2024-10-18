// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logconfig

import (
	"bytes"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

// Validate checks the configuration and propagates defaults.
func (c *Config) Validate(defaultLogDir *string) (resErr error) {
	var errBuf bytes.Buffer
	defer func() {
		s := errBuf.String()
		if s != "" {
			resErr = errors.Newf("%s", s)
		}
	}()

	bt, bf := true, false
	zeroDuration := time.Duration(0)
	zeroByteSize := ByteSize(0)
	defaultBufferedStaleness := 5 * time.Second
	defaultFlushTriggerSize := ByteSize(1024 * 1024)   // 1mib
	defaultMaxBufferSize := ByteSize(50 * 1024 * 1024) // 50mib
	bufferFmt := BufferFmtNewline

	baseCommonSinkConfig := CommonSinkConfig{
		Filter:      logpb.Severity_INFO,
		Auditable:   &bf,
		Redactable:  &bt,
		Redact:      &bf,
		Criticality: &bf,
		// Buffering is configured to "NONE". This is different from a zero value
		// which buffers infinitely.
		Buffering: CommonBufferSinkConfigWrapper{
			CommonBufferSinkConfig: CommonBufferSinkConfig{
				MaxStaleness:     &zeroDuration,
				FlushTriggerSize: &zeroByteSize,
				MaxBufferSize:    &zeroByteSize,
				Format:           &bufferFmt,
			},
		},
	}
	baseFileDefaults := FileDefaults{
		Dir:             defaultLogDir,
		BufferedWrites:  &bt,
		MaxFileSize:     &zeroByteSize,
		MaxGroupSize:    &zeroByteSize,
		FilePermissions: func() *FilePermissions { s := DefaultFilePerms; return &s }(),
		CommonSinkConfig: CommonSinkConfig{
			Format:      func() *string { s := DefaultFileFormat; return &s }(),
			Criticality: &bt,
			// Buffering is configured to "NONE". We're setting this to something so
			// that the defaults are not propagated from baseCommonSinkConfig because
			// buffering is not supported on file sinks at the moment (#72452).
			Buffering: CommonBufferSinkConfigWrapper{
				CommonBufferSinkConfig: CommonBufferSinkConfig{
					MaxStaleness:     &zeroDuration,
					FlushTriggerSize: &zeroByteSize,
					MaxBufferSize:    &zeroByteSize,
					Format:           &bufferFmt,
				},
			},
		},
	}
	baseFluentDefaults := FluentDefaults{
		CommonSinkConfig: CommonSinkConfig{
			Format: func() *string { s := DefaultFluentFormat; return &s }(),
			Buffering: CommonBufferSinkConfigWrapper{
				CommonBufferSinkConfig: CommonBufferSinkConfig{
					MaxStaleness:     &defaultBufferedStaleness,
					FlushTriggerSize: &defaultFlushTriggerSize,
					MaxBufferSize:    &defaultMaxBufferSize,
					Format:           &bufferFmt,
				},
			},
		},
	}
	baseHTTPDefaults := HTTPDefaults{
		CommonSinkConfig: CommonSinkConfig{
			Format: func() *string { s := DefaultHTTPFormat; return &s }(),
			Buffering: CommonBufferSinkConfigWrapper{
				CommonBufferSinkConfig: CommonBufferSinkConfig{
					MaxStaleness:     &defaultBufferedStaleness,
					FlushTriggerSize: &defaultFlushTriggerSize,
					MaxBufferSize:    &defaultMaxBufferSize,
					Format:           &bufferFmt,
				},
			},
		},
		UnsafeTLS:         &bf,
		DisableKeepAlives: &bf,
		Method:            func() *HTTPSinkMethod { m := HTTPSinkMethod(http.MethodPost); return &m }(),
		Timeout: func() *time.Duration {
			twoS := 2 * time.Second
			return &twoS
		}(),
		Compression: &GzipCompression,
	}

	propagateCommonDefaults(&baseFileDefaults.CommonSinkConfig, baseCommonSinkConfig)
	propagateCommonDefaults(&baseFluentDefaults.CommonSinkConfig, baseCommonSinkConfig)
	propagateCommonDefaults(&baseHTTPDefaults.CommonSinkConfig, baseCommonSinkConfig)

	propagateFileDefaults(&c.FileDefaults, baseFileDefaults)
	propagateFluentDefaults(&c.FluentDefaults, baseFluentDefaults)
	propagateHTTPDefaults(&c.HTTPDefaults, baseHTTPDefaults)

	// Normalize the directory.
	if err := normalizeDir(&c.FileDefaults.Dir); err != nil {
		fmt.Fprintf(&errBuf, "file-defaults: %v\n", err)
	}

	// Validate and fill in defaults for file sinks.
	for prefix, fc := range c.Sinks.FileGroups {
		if fc == nil {
			fc = c.newFileSinkConfig(prefix)
		} else {
			fc.prefix = prefix
		}
		if err := c.validateFileSinkConfig(fc); err != nil {
			fmt.Fprintf(&errBuf, "file group %q: %v\n", prefix, err)
		}
	}

	// Validate and defaults for fluent.
	for serverName, fc := range c.Sinks.FluentServers {
		if fc == nil {
			fc = &FluentSinkConfig{Channels: SelectChannels()}
			c.Sinks.FluentServers[serverName] = fc
		}
		fc.serverName = serverName
		if err := c.validateFluentSinkConfig(fc); err != nil {
			fmt.Fprintf(&errBuf, "fluent server %q: %v\n", serverName, err)
		}
	}

	for sinkName, fc := range c.Sinks.HTTPServers {
		if fc == nil {
			fc = &HTTPSinkConfig{Channels: SelectChannels()}
			c.Sinks.HTTPServers[sinkName] = fc
		}
		fc.sinkName = sinkName
		if err := c.validateHTTPSinkConfig(fc); err != nil {
			fmt.Fprintf(&errBuf, "http server %q: %v\n", sinkName, err)
		}
	}

	// Defaults for stderr.
	if c.Sinks.Stderr.Filter == logpb.Severity_UNKNOWN {
		c.Sinks.Stderr.Filter = logpb.Severity_NONE
	}
	// We need to know if format-options were specifically defined on the stderr sink later on,
	// since this information is lost once propagateCommonDefaults is called.
	stdErrFormatOptionsOriginallySet := len(c.Sinks.Stderr.FormatOptions) > 0
	propagateCommonDefaults(&c.Sinks.Stderr.CommonSinkConfig, c.FileDefaults.CommonSinkConfig)
	if c.Sinks.Stderr.Auditable != nil && *c.Sinks.Stderr.Auditable {
		c.Sinks.Stderr.Criticality = &bt
	}
	c.Sinks.Stderr.Auditable = nil

	// FormatOptions are format-specific. We should only copy them over to StdErr from
	// FileDefaults if FileDefaults specifies the same format as the stderr sink. Otherwise,
	// we are likely to error when trying to apply an unsupported format option.
	if c.FileDefaults.CommonSinkConfig.Format != nil &&
		!canShareFormatOptions(*c.FileDefaults.CommonSinkConfig.Format, *c.Sinks.Stderr.Format) &&
		!stdErrFormatOptionsOriginallySet {
		c.Sinks.Stderr.CommonSinkConfig.FormatOptions = map[string]string{}
	}
	if err := c.ValidateCommonSinkConfig(c.Sinks.Stderr.CommonSinkConfig); err != nil {
		fmt.Fprintf(&errBuf, "stderr sink: %v\n", err)
	}

	// Propagate the sink-wide default filter to all channels that don't
	// have a filter yet.
	if err := c.Sinks.Stderr.Channels.Validate(c.Sinks.Stderr.Filter); err != nil {
		fmt.Fprintf(&errBuf, "stderr sink: %v\n", err)
	}

	fileSinks := make(map[logpb.Channel][]*FileSinkConfig)
	// remember the file sink names for deterministic traversals.
	fileNames := make([]string, 0, len(c.Sinks.FileGroups))

	// Check that every file has at least one channel.
	for fname, fc := range c.Sinks.FileGroups {
		if len(fc.Channels.Filters) == 0 {
			fmt.Fprintf(&errBuf, "file group %q: no channel selected\n", fc.prefix)
			continue
		}
		// Propagate the sink-wide default filter to all channels that don't
		// have a filter yet.
		if err := fc.Channels.Validate(fc.Filter); err != nil {
			fmt.Fprintf(&errBuf, "file group %q: %v\n", fc.prefix, err)
			continue
		}

		fileNames = append(fileNames, fname)
	}
	sort.Strings(fileNames)
	for _, fname := range fileNames {
		fc := c.Sinks.FileGroups[fname]
		for _, ch := range fc.Channels.AllChannels.Channels {
			// Remember which file sink captures which channel.
			fileSinks[ch] = append(fileSinks[ch], fc)
		}
	}

	// Check that every sink has at least one channel.
	for serverName, fc := range c.Sinks.FluentServers {
		if len(fc.Channels.Filters) == 0 {
			fmt.Fprintf(&errBuf, "fluent server %q: no channel selected\n", serverName)
			continue
		}
		// Propagate the sink-wide default filter to all channels that don't
		// have a filter yet.
		if err := fc.Channels.Validate(fc.Filter); err != nil {
			fmt.Fprintf(&errBuf, "fluent server %q: %v\n", serverName, err)
			continue
		}
	}

	for sinkName, fc := range c.Sinks.HTTPServers {
		if len(fc.Channels.Filters) == 0 {
			fmt.Fprintf(&errBuf, "http server %q: no channel selected\n", sinkName)
		}
		// Propagate the sink-wide default filter to all channels that don't
		// have a filter yet.
		if err := fc.Channels.Validate(fc.Filter); err != nil {
			fmt.Fprintf(&errBuf, "http server %q: %v\n", sinkName, err)
			continue
		}
	}

	// If capture-stray-errors was enabled, then perform some additional
	// validation on it.
	if c.CaptureFd2.Enable {
		if c.CaptureFd2.MaxGroupSize == nil {
			c.CaptureFd2.MaxGroupSize = c.FileDefaults.MaxGroupSize
		}
		if c.CaptureFd2.Dir == nil {
			// No directory specified; inherit defaults.
			c.CaptureFd2.Dir = c.FileDefaults.Dir
		} else {
			// Directory was specified: normalize it.
			if err := normalizeDir(&c.CaptureFd2.Dir); err != nil {
				fmt.Fprintf(&errBuf, "stray error capture: %v\n", err)
			}
		}
		if c.CaptureFd2.Dir == nil {
			// After normalization, no directory was left: disable the fd2
			// capture.
			c.CaptureFd2.Enable = false
		}
	}
	if !c.CaptureFd2.Enable {
		if c.Sinks.Stderr.Filter != logpb.Severity_NONE && *c.Sinks.Stderr.Redactable {
			fmt.Fprintln(&errBuf,
				"stderr.redactable cannot be set if capture-stray-errors.enable is unset")
		}
		// Not enabled: erase all fields so it gets omitted when pretty-printing.
		c.CaptureFd2 = CaptureFd2Config{}
	}

	// If there is no file group for DEV yet, create one.
	// We'll target the "default" group.
	// If the "default" group already exists, we'll use that. Otherwise, we create it.
	devch := logpb.Channel_DEV
	if def := fileSinks[devch]; len(def) == 0 {
		// If there is a default group already, use it.
		var fc *FileSinkConfig
		if def, ok := c.Sinks.FileGroups["default"]; ok {
			fc = def
		} else {
			// "default" did not exist yet. Create it.
			fc = c.newFileSinkConfig("default")
			if err := c.validateFileSinkConfig(fc); err != nil {
				fmt.Fprintln(&errBuf, err)
			}
		}
		// Add the DEV channel to the sink.
		// The call to Update() below fills in the default severity.
		fc.Channels.AddChannel(devch, logpb.Severity_UNKNOWN)
		if err := fc.Channels.Validate(fc.Filter); err != nil {
			// Should never happen.
			return errors.NewAssertionErrorWithWrappedErrf(err, "programming error: invalid extension of DEV sink")
		}
		// Remember this new sink as a sink that captures DEV.
		fileSinks[devch] = append(fileSinks[devch], fc)
	}

	// For every remaining channel without a sink, add it to the first DEV
	// sink at its default filter.
	//
	// The "first" DEV sink is the "default" sink if that exists and
	// captures DEV; otherwise the first file sink that's a DEV sink in
	// lexicographic order.
	var devFile *FileSinkConfig
	if fc, ok := c.Sinks.FileGroups["default"]; ok && fc.Channels.AllChannels.HasChannel(devch) {
		// There's a "default" sink and it captures DEV. Use that.
		devFile = fc
	} else {
		// Use the first DEV sink. We know there is one because we've created at least one above.
		devFile = fileSinks[devch][0]
	}
	for _, ch := range channelValues {
		if fileSinks[ch] == nil {
			devFile.Channels.AddChannel(ch, logpb.Severity_UNKNOWN)
		}
	}
	if err := devFile.Channels.Validate(devFile.Filter); err != nil {
		// Should never happen.
		return errors.NewAssertionErrorWithWrappedErrf(err, "programming error: invalid extension of DEV sink")
	}

	// Elide all the file sinks without a directory or where all
	// channels have severity set to NONE.
	for prefix, fc := range c.Sinks.FileGroups {
		if fc.Dir == nil || fc.Channels.noChannelsSelected() {
			delete(c.Sinks.FileGroups, prefix)
		}
	}

	// Elide all the fluent sinks where all channels have
	// severity set to NONE.
	for serverName, fc := range c.Sinks.FluentServers {
		if fc.Channels.noChannelsSelected() {
			delete(c.Sinks.FluentServers, serverName)
		}
	}

	// Elide all the HTTP sinks where all channels have
	// severity set to NONE.
	for serverName, fc := range c.Sinks.HTTPServers {
		if fc.Channels.noChannelsSelected() {
			delete(c.Sinks.HTTPServers, serverName)
		}
	}

	return nil
}

func (c *Config) newFileSinkConfig(groupName string) *FileSinkConfig {
	fc := &FileSinkConfig{
		Channels: SelectChannels(),
		prefix:   groupName,
	}
	if c.Sinks.FileGroups == nil {
		c.Sinks.FileGroups = make(map[string]*FileSinkConfig)
	}
	c.Sinks.FileGroups[groupName] = fc
	return fc
}

func (c *Config) validateFileSinkConfig(fc *FileSinkConfig) error {
	propagateFileDefaults(&fc.FileDefaults, c.FileDefaults)
	if !fc.Buffering.IsNone() {
		if fc.BufferedWrites != nil && *fc.BufferedWrites {
			return errors.Newf(`Unable to use "buffered-writes" in conjunction with a "buffering" configuration. ` +
				`These configuration options are mutually exclusive.`)
		}
		if *fc.Auditable {
			return errors.Newf(`File-based audit logging cannot coexist with buffering configuration. ` +
				`Disable either the buffering configuration ("buffering") or auditable log ("auditable") configuration.`)
		}
		// To preserve the format of log files, avoid additional formatting in the
		// buffering configuration.
		fmtNone := BufferFmtNone
		fc.Buffering.Format = &fmtNone
	}
	if fc.Dir != c.FileDefaults.Dir {
		// A directory was specified explicitly. Normalize it.
		if err := normalizeDir(&fc.Dir); err != nil {
			return err
		}
	}
	if fc.Dir == nil {
		// After normalization, the remaining directory is empty.  Make
		// this sink filter everything, so we don't spend time computing
		// message strings in logging.
		fc.Filter = logpb.Severity_NONE
	}

	// Apply the auditable flag if set.
	if *fc.Auditable {
		bf, bt := false, true
		fc.BufferedWrites = &bf
		fc.Criticality = &bt
		if *fc.Format == "crdb-v1" {
			s := "crdb-v1-count"
			fc.Format = &s
		}
	}
	fc.Auditable = nil

	return c.ValidateCommonSinkConfig(fc.CommonSinkConfig)
}

// ValidateCommonSinkConfig validates a CommonSinkConfig.
func (c *Config) ValidateCommonSinkConfig(conf CommonSinkConfig) error {
	b := conf.Buffering
	if b.IsNone() {
		return nil
	}

	const minSlackBytes = 1 << 20 // 1MB

	if b.FlushTriggerSize != nil && b.MaxBufferSize != nil {
		if *b.FlushTriggerSize > *b.MaxBufferSize-minSlackBytes {
			// See comments on newBufferSink.
			return errors.Newf(
				"not enough slack between flush-trigger-size (%s) and max-buffer-size (%s); "+
					"flush-trigger-size needs to be <= max-buffer-size - %s (= %s) to ensure that"+
					"a large message does not cause the buffer to overflow without triggering a flush",
				b.FlushTriggerSize, b.MaxBufferSize, humanizeutil.IBytes(minSlackBytes), *b.MaxBufferSize-minSlackBytes,
			)
		}
	}
	return nil
}

func (c *Config) validateFluentSinkConfig(fc *FluentSinkConfig) error {
	propagateFluentDefaults(&fc.FluentDefaults, c.FluentDefaults)
	fc.Net = strings.ToLower(strings.TrimSpace(fc.Net))
	switch fc.Net {
	case "tcp", "tcp4", "tcp6":
	case "udp", "udp4", "udp6":
	case "unix":
	case "":
		fc.Net = "tcp"
	default:
		return errors.Newf("unknown protocol: %q", fc.Net)
	}
	fc.Address = strings.TrimSpace(fc.Address)
	if fc.Address == "" {
		return errors.New("address cannot be empty")
	}

	// Apply the auditable flag if set.
	if *fc.Auditable {
		bt := true
		fc.Criticality = &bt
	}
	fc.Auditable = nil

	return c.ValidateCommonSinkConfig(fc.CommonSinkConfig)
}

func (c *Config) validateHTTPSinkConfig(hsc *HTTPSinkConfig) error {
	propagateHTTPDefaults(&hsc.HTTPDefaults, c.HTTPDefaults)
	if hsc.Address == nil || len(*hsc.Address) == 0 {
		return errors.New("address cannot be empty")
	}
	if *hsc.Compression != GzipCompression && *hsc.Compression != NoneCompression {
		return errors.New("compression must be 'gzip' or 'none'")
	}
	// If both header types are populated, make sure theres no duplicate keys
	if hsc.Headers != nil && hsc.FileBasedHeaders != nil {
		for key := range hsc.Headers {
			if _, exists := hsc.FileBasedHeaders[key]; exists {
				return errors.Newf("headers and file-based-headers have the same key %s", key)
			}
		}
	}
	return c.ValidateCommonSinkConfig(hsc.CommonSinkConfig)
}

func normalizeDir(dir **string) error {
	if *dir == nil {
		return nil
	}
	if len(**dir) == 0 {
		return errors.New("log directory cannot be empty; specify '.' for current directory")
	}
	if strings.HasPrefix(**dir, "~") {
		return errors.Newf("log directory cannot start with '~': %s\n", **dir)
	}
	absDir, err := filepath.Abs(**dir)
	if err != nil {
		return err
	}
	*dir = &absDir
	return nil
}

func propagateCommonDefaults(target *CommonSinkConfig, source CommonSinkConfig) {
	propagateDefaults(target, source)
}

func propagateFileDefaults(target *FileDefaults, source FileDefaults) {
	propagateDefaults(target, source)
}

func propagateFluentDefaults(target *FluentDefaults, source FluentDefaults) {
	propagateDefaults(target, source)
}

func propagateHTTPDefaults(target *HTTPDefaults, source HTTPDefaults) {
	propagateDefaults(target, source)
}

// propagateDefaults takes (target *T, source T) where T is a struct
// and sets zero-valued exported fields in target to the values
// from source (recursively for struct-valued fields).
// Wrap for static type-checking, as unexpected types will panic.
//
// (Consider making this a common utility if it gets some maturity here.)
func propagateDefaults(target, source interface{}) {
	s := reflect.ValueOf(source)
	t := reflect.Indirect(reflect.ValueOf(target)) // *target

	for i := 0; i < t.NumField(); i++ {
		tf := t.Field(i)
		sf := s.Field(i)
		if tf.Kind() == reflect.Struct {
			propagateDefaults(tf.Addr().Interface(), sf.Interface())
		} else if tf.CanSet() && tf.IsZero() {
			tf.Set(s.Field(i))
		}
	}
}

// canShareFormatOptions returns true if f1 and f2 can share format options.
// See log.FormatParsers for full list of supported formats.
// Examples:
//
//	canShareFormatOptions("json", "json") => true
//	canShareFormatOptions("crdb-v2", "crdb-v2-tty") => true
//	canShareFormatOptions("crdb-v2", "json") => false
func canShareFormatOptions(f1, f2 string) bool {
	return strings.HasPrefix(f1, f2) || strings.HasPrefix(f2, f1)
}
