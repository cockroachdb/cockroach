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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
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

	if c.FileDefaults.Dir == nil {
		// If the default directory was not specified, use the one
		// provided by the environment.
		c.FileDefaults.Dir = defaultLogDir
	}

	if c.FileDefaults.Filter == logpb.Severity_UNKNOWN {
		c.FileDefaults.Filter = logpb.Severity_INFO
	}

	// Validate and fill in defaults for file sinks.
	for prefix, fc := range c.Sinks.FileGroups {
		if fc == nil {
			fc = &FileConfig{}
			c.Sinks.FileGroups[prefix] = fc
		}
		fc.prefix = prefix
		if err := c.validateFileConfig(fc, defaultLogDir); err != nil {
			fmt.Fprintln(&errBuf, err)
		}
	}

	// Validate and defaults for fluent.
	for serverName, fc := range c.Sinks.FluentServers {
		if fc == nil {
			fc = &FluentConfig{}
			c.Sinks.FluentServers[serverName] = fc
		}
		fc.serverName = serverName
		if err := c.validateFluentConfig(fc); err != nil {
			fmt.Fprintln(&errBuf, err)
		}
	}

	// Validate and defaults for stderr.
	if c.Sinks.Stderr.Filter == logpb.Severity_UNKNOWN {
		c.Sinks.Stderr.Filter = logpb.Severity_INFO
	}

	// Check that no channel is listed by more than one fluent sink,
	// or more than one file sink.
	fileSinks := make(map[logpb.Channel]*FileConfig)
	fluentSinks := make(map[logpb.Channel]*FluentConfig)

	for _, fc := range c.Sinks.FileGroups {
		if len(fc.Channels.Channels) == 0 {
			fmt.Fprintf(&errBuf, "file group %q: no channel selected", fc.prefix)
		}
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

	for _, fc := range c.Sinks.FluentServers {
		if len(fc.Channels.Channels) == 0 {
			fmt.Fprintf(&errBuf, "fluent server %q: no channel selected\n", fc.serverName)
		}
		for _, ch := range fc.Channels.Channels {
			if prev := fluentSinks[ch]; prev != nil {
				fmt.Fprintf(&errBuf, "fluent server %q: channel %s already captured by server %q\n",
					fc.serverName, ch, prev.serverName)
			} else {
				fluentSinks[ch] = fc
			}
		}
	}

	// If fd2 capture is requested, do some additional validation on that sink.
	if c.CaptureFd2.Enable {
		if c.CaptureFd2.Dir == nil {
			c.CaptureFd2.Dir = c.FileDefaults.Dir
		}
	} else {
		if c.Sinks.Stderr.Filter != logpb.Severity_NONE && c.Sinks.Stderr.Redactable {
			fmt.Fprintln(&errBuf, "stderr.redactable cannot be set if capture-stray-errors.enable is unset")
		}
		// Erase fields so it gets omitted when pretty-printing.
		c.CaptureFd2 = CaptureFd2Config{}
	}

	// If there is no file group for DEV yet, create one.
	devch := logpb.Channel_DEV
	if def := fileSinks[devch]; def == nil {
		fc := &FileConfig{
			Channels: ChannelList{Channels: []logpb.Channel{devch}},
		}
		fc.prefix = "default"
		if err := c.validateFileConfig(fc, defaultLogDir); err != nil {
			fmt.Fprintln(&errBuf, err)
		}
		if c.Sinks.FileGroups == nil {
			c.Sinks.FileGroups = make(map[string]*FileConfig)
		}
		c.Sinks.FileGroups[fc.prefix] = fc
		fileSinks[devch] = fc
	}

	// For every channel with a pre-defined name, add its sink if it
	// does not exist already.
	for ch, prefix := range predefinedLogFiles {
		if fileSinks[ch] != nil {
			// Channel already has a file. Ignore.
			continue
		}
		fc := &FileConfig{
			Channels: ChannelList{Channels: []logpb.Channel{ch}},
		}
		fc.prefix = prefix
		if err := c.validateFileConfig(fc, defaultLogDir); err != nil {
			fmt.Fprintln(&errBuf, err)
		}
		if c.Sinks.FileGroups == nil {
			c.Sinks.FileGroups = make(map[string]*FileConfig)
		}
		c.Sinks.FileGroups[fc.prefix] = fc
		fileSinks[ch] = fc
	}

	// For every remaining channel without a sink, add it to the DEV sink.
	devFile := fileSinks[devch]
	for _, ch := range channelValues {
		if fileSinks[ch] == nil {
			devFile.Channels.Channels = append(devFile.Channels.Channels, ch)
		}
	}

	// For every file sink, verify that a log directory is defined if
	// the filter is not NONE. Also verify that the directory does not
	// start with "~".
	for prefix, fc := range c.Sinks.FileGroups {
		if fc.Dir != nil && len(*fc.Dir) > 0 {
			if strings.HasPrefix(*fc.Dir, "~") {
				fmt.Fprintf(&errBuf,
					"file group %q: log directory cannot start with '~': %s\n", prefix, *fc.Dir)
			}
			absDir, err := filepath.Abs(*fc.Dir)
			if err != nil {
				fmt.Fprintf(&errBuf,
					"file group %q: %v\n", prefix, err)
			} else {
				fc.Dir = &absDir
			}
		}

		if fc.Filter != logpb.Severity_NONE {
			if fc.Dir == nil || *fc.Dir == "" {
				fmt.Fprintf(&errBuf, "file group %q: no directory defined\n", prefix)
			}
		}
	}

	return nil
}

var predefinedLogFiles = map[logpb.Channel]string{
	channel.STORAGE:           "storage",
	channel.SESSIONS:          "sql-auth",
	channel.SENSITIVE_ACCESS:  "sql-audit",
	channel.SQL_EXEC:          "sql-exec",
	channel.SQL_PERF:          "sql-slow",
	channel.SQL_INTERNAL_PERF: "sql-slow-internal-only",
}

func (c *Config) validateFluentConfig(fc *FluentConfig) error {
	fc.Net = strings.ToLower(strings.TrimSpace(fc.Net))
	switch fc.Net {
	case "tcp", "tcp4", "tcp6":
	case "udp", "udp4", "udp6":
	case "unix":
	case "":
		fc.Net = "tcp"
	}
	fc.Address = strings.TrimSpace(fc.Address)
	if fc.Address == "" {
		return errors.Newf("fluent server %q: address cannot be empty", fc.serverName)
	}
	if fc.Filter == logpb.Severity_UNKNOWN {
		fc.Filter = logpb.Severity_INFO
	}
	tb := true
	if fc.Redactable == nil {
		fc.Redactable = &tb
	}
	return nil
}

func (c *Config) validateFileConfig(fc *FileConfig, defaultLogDir *string) error {
	// Set up the directory.
	if fc.Dir == nil {
		// If the specific group does not specify its directory,
		// inherit the default.
		fc.Dir = c.FileDefaults.Dir
	}

	// Inherit more defaults.
	if fc.Filter == logpb.Severity_UNKNOWN {
		fc.Filter = c.FileDefaults.Filter
	}
	if fc.MaxFileSize == nil {
		fc.MaxFileSize = &c.FileDefaults.MaxFileSize
	}
	if fc.MaxGroupSize == nil {
		fc.MaxGroupSize = &c.FileDefaults.MaxGroupSize
	}
	if fc.Redact == nil {
		fc.Redact = &c.FileDefaults.Redact
	}
	if fc.Redactable == nil {
		fc.Redactable = &c.FileDefaults.Redactable
	}
	return nil
}
