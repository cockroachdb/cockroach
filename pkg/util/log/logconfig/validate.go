// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logconfig

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

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

	// If the default directory was not specified, use the one
	// provided by the environment.
	if c.FileDefaults.Dir == nil {
		c.FileDefaults.Dir = defaultLogDir
	}
	// Normalize the directory.
	if err := normalizeDir(&c.FileDefaults.Dir); err != nil {
		fmt.Fprintf(&errBuf, "file-defaults: %v\n", err)
	}
	// No severity -> default INFO.
	if c.FileDefaults.Filter == logpb.Severity_UNKNOWN {
		c.FileDefaults.Filter = logpb.Severity_INFO
	}
	if c.FluentDefaults.Filter == logpb.Severity_UNKNOWN {
		c.FluentDefaults.Filter = logpb.Severity_INFO
	}
	// Sinks are not auditable by default.
	if c.FileDefaults.Auditable == nil {
		c.FileDefaults.Auditable = &bf
	}
	if c.FluentDefaults.Auditable == nil {
		c.FluentDefaults.Auditable = &bf
	}
	// File sinks are buffered by default.
	if c.FileDefaults.BufferedWrites == nil {
		c.FileDefaults.BufferedWrites = &bt
	}
	// No format -> populate defaults.
	if c.FileDefaults.Format == nil {
		s := DefaultFileFormat
		c.FileDefaults.Format = &s
	}
	if c.FluentDefaults.Format == nil {
		s := DefaultFluentFormat
		c.FluentDefaults.Format = &s
	}
	// No redaction markers -> default keep them.
	if c.FileDefaults.Redactable == nil {
		c.FileDefaults.Redactable = &bt
	}
	if c.FluentDefaults.Redactable == nil {
		c.FluentDefaults.Redactable = &bt
	}
	// No redaction specification -> default false.
	if c.FileDefaults.Redact == nil {
		c.FileDefaults.Redact = &bf
	}
	if c.FluentDefaults.Redact == nil {
		c.FluentDefaults.Redact = &bf
	}
	// No criticality -> default true for files, false for fluent.
	if c.FileDefaults.Criticality == nil {
		c.FileDefaults.Criticality = &bt
	}
	if c.FluentDefaults.Criticality == nil {
		c.FluentDefaults.Criticality = &bf
	}

	// Validate and fill in defaults for file sinks.
	for prefix, fc := range c.Sinks.FileGroups {
		if fc == nil {
			fc = &FileSinkConfig{}
			c.Sinks.FileGroups[prefix] = fc
		}
		fc.prefix = prefix
		if err := c.validateFileSinkConfig(fc, defaultLogDir); err != nil {
			fmt.Fprintf(&errBuf, "file group %q: %v\n", prefix, err)
		}
	}

	// Validate and defaults for fluent.
	for serverName, fc := range c.Sinks.FluentServers {
		if fc == nil {
			fc = &FluentSinkConfig{}
			c.Sinks.FluentServers[serverName] = fc
		}
		fc.serverName = serverName
		if err := c.validateFluentSinkConfig(fc); err != nil {
			fmt.Fprintf(&errBuf, "fluent server %q: %v\n", serverName, err)
		}
	}

	// Defaults for stderr.
	c.inheritCommonDefaults(&c.Sinks.Stderr.CommonSinkConfig, &c.FileDefaults.CommonSinkConfig)
	if c.Sinks.Stderr.Filter == logpb.Severity_UNKNOWN {
		c.Sinks.Stderr.Filter = logpb.Severity_NONE
	}
	if c.Sinks.Stderr.Auditable != nil {
		if *c.Sinks.Stderr.Auditable {
			if *c.Sinks.Stderr.Format == "crdb-v1-tty" {
				f := "crdb-v1-tty-count"
				c.Sinks.Stderr.Format = &f
			}
			c.Sinks.Stderr.Criticality = &bt
		}
		c.Sinks.Stderr.Auditable = nil
	}
	c.Sinks.Stderr.Channels.Sort()

	// fileSinks maps channels to files.
	fileSinks := make(map[logpb.Channel]*FileSinkConfig)
	// fluentSinks maps channels to fluent servers.
	fluentSinks := make(map[logpb.Channel]*FluentSinkConfig)

	// Check that no channel is listed by more than one file sink,
	// and every file has at least one channel.
	for _, fc := range c.Sinks.FileGroups {
		if len(fc.Channels.Channels) == 0 {
			fmt.Fprintf(&errBuf, "file group %q: no channel selected\n", fc.prefix)
		}
		fc.Channels.Sort()
		for _, ch := range fc.Channels.Channels {
			if prev := fileSinks[ch]; prev != nil {
				prevPrefix := prev.prefix
				if prevPrefix == "" {
					prevPrefix = "debug"
				}
				fmt.Fprintf(&errBuf, "file group %q: channel %s already captured by group %q\n",
					fc.prefix, ch, prevPrefix)
			} else {
				fileSinks[ch] = fc
			}
		}
	}

	// Check that no channel is listed by more than one fluent sink, and
	// every sink has at least one channel.
	for _, fc := range c.Sinks.FluentServers {
		if len(fc.Channels.Channels) == 0 {
			fmt.Fprintf(&errBuf, "fluent server %q: no channel selected\n", fc.serverName)
		}
		fc.Channels.Sort()
		for _, ch := range fc.Channels.Channels {
			if prev := fluentSinks[ch]; prev != nil {
				fmt.Fprintf(&errBuf, "fluent server %q: channel %s already captured by server %q\n",
					fc.serverName, ch, prev.serverName)
			} else {
				fluentSinks[ch] = fc
			}
		}
	}

	// If capture-stray-errors was enabled, then perform some additional
	// validation on it.
	if c.CaptureFd2.Enable {
		if c.CaptureFd2.MaxGroupSize == nil {
			c.CaptureFd2.MaxGroupSize = &c.FileDefaults.MaxGroupSize
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
	devch := logpb.Channel_DEV
	if def := fileSinks[devch]; def == nil {
		fc := &FileSinkConfig{
			Channels: ChannelList{Channels: []logpb.Channel{devch}},
		}
		fc.prefix = "default"
		if err := c.validateFileSinkConfig(fc, defaultLogDir); err != nil {
			fmt.Fprintln(&errBuf, err)
		}
		if c.Sinks.FileGroups == nil {
			c.Sinks.FileGroups = make(map[string]*FileSinkConfig)
		}
		c.Sinks.FileGroups[fc.prefix] = fc
		fileSinks[devch] = fc
	}

	// For every remaining channel without a sink, add it to the DEV sink.
	devFile := fileSinks[devch]
	for _, ch := range channelValues {
		if fileSinks[ch] == nil {
			devFile.Channels.Channels = append(devFile.Channels.Channels, ch)
		}
	}
	devFile.Channels.Sort()

	// fileGroupNames collects the names of file groups. We need this to
	// store this sorted in c.Sinks.sortedFileGroupNames later.
	fileGroupNames := make([]string, 0, len(c.Sinks.FileGroups))
	// Elide all the file sinks without a directory or with severity set
	// to NONE. Also collect the remaining names for sorting below.
	for prefix, fc := range c.Sinks.FileGroups {
		if fc.Dir == nil || fc.Filter == logpb.Severity_NONE {
			delete(c.Sinks.FileGroups, prefix)
		} else {
			fileGroupNames = append(fileGroupNames, prefix)
		}
	}

	// serverNames collects the names of the servers. We need this to
	// store this sorted in c.Sinks.sortedServerNames later.
	serverNames := make([]string, 0, len(c.Sinks.FluentServers))
	// Elide all the file sinks without a directory or with severity set
	// to NONE. Also collect the remaining names for sorting below.
	for serverName, fc := range c.Sinks.FluentServers {
		if fc.Filter == logpb.Severity_NONE {
			delete(c.Sinks.FluentServers, serverName)
		} else {
			serverNames = append(serverNames, serverName)
		}
	}

	// Remember the sorted names, so we get deterministic output in
	// export.
	sort.Strings(fileGroupNames)
	c.Sinks.sortedFileGroupNames = fileGroupNames
	sort.Strings(serverNames)
	c.Sinks.sortedServerNames = serverNames

	return nil
}

func (c *Config) inheritCommonDefaults(fc, defaults *CommonSinkConfig) {
	if fc.Filter == logpb.Severity_UNKNOWN {
		fc.Filter = defaults.Filter
	}
	if fc.Format == nil {
		fc.Format = defaults.Format
	}
	if fc.Redact == nil {
		fc.Redact = defaults.Redact
	}
	if fc.Redactable == nil {
		fc.Redactable = defaults.Redactable
	}
	if fc.Criticality == nil {
		fc.Criticality = defaults.Criticality
	}
	if fc.Auditable == nil {
		fc.Auditable = defaults.Auditable
	}
}

func (c *Config) validateFileSinkConfig(fc *FileSinkConfig, defaultLogDir *string) error {
	c.inheritCommonDefaults(&fc.CommonSinkConfig, &c.FileDefaults.CommonSinkConfig)

	// Inherit file-specific defaults.
	if fc.MaxFileSize == nil {
		fc.MaxFileSize = &c.FileDefaults.MaxFileSize
	}
	if fc.MaxGroupSize == nil {
		fc.MaxGroupSize = &c.FileDefaults.MaxGroupSize
	}
	if fc.BufferedWrites == nil {
		fc.BufferedWrites = c.FileDefaults.BufferedWrites
	}

	// Set up the directory.
	if fc.Dir == nil {
		// If the specific group does not specify its directory,
		// inherit the default.
		fc.Dir = c.FileDefaults.Dir
	} else {
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

	return nil
}

func (c *Config) validateFluentSinkConfig(fc *FluentSinkConfig) error {
	c.inheritCommonDefaults(&fc.CommonSinkConfig, &c.FluentDefaults.CommonSinkConfig)

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

	return nil
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
