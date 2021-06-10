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
	"reflect"
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

	baseCommonSinkConfig := CommonSinkConfig{
		Filter:      logpb.Severity_INFO,
		Auditable:   &bf,
		Redactable:  &bt,
		Redact:      &bf,
		Criticality: &bf,
	}
	baseFileDefaults := FileDefaults{
		Dir:            defaultLogDir,
		BufferedWrites: &bt,
		MaxFileSize:    func() *ByteSize { s := ByteSize(0); return &s }(),
		MaxGroupSize:   func() *ByteSize { s := ByteSize(0); return &s }(),
		CommonSinkConfig: CommonSinkConfig{
			Format:      func() *string { s := DefaultFileFormat; return &s }(),
			Criticality: &bt,
		},
	}
	baseFluentDefaults := FluentDefaults{
		CommonSinkConfig: CommonSinkConfig{
			Format: func() *string { s := DefaultFluentFormat; return &s }(),
		},
	}

	progagateCommonDefaults(&baseFileDefaults.CommonSinkConfig, baseCommonSinkConfig)
	progagateCommonDefaults(&baseFluentDefaults.CommonSinkConfig, baseCommonSinkConfig)

	progagateFileDefaults(&c.FileDefaults, baseFileDefaults)
	progagateFluentDefaults(&c.FluentDefaults, baseFluentDefaults)

	// Normalize the directory.
	if err := normalizeDir(&c.FileDefaults.Dir); err != nil {
		fmt.Fprintf(&errBuf, "file-defaults: %v\n", err)
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
	if c.Sinks.Stderr.Filter == logpb.Severity_UNKNOWN {
		c.Sinks.Stderr.Filter = logpb.Severity_NONE
	}
	progagateCommonDefaults(&c.Sinks.Stderr.CommonSinkConfig, c.FileDefaults.CommonSinkConfig)
	if c.Sinks.Stderr.Auditable != nil && *c.Sinks.Stderr.Auditable {
		if *c.Sinks.Stderr.Format == "crdb-v1-tty" {
			f := "crdb-v1-tty-count"
			c.Sinks.Stderr.Format = &f
		}
		c.Sinks.Stderr.Criticality = &bt
	}
	c.Sinks.Stderr.Auditable = nil

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
	devch := logpb.Channel_DEV
	if def := fileSinks[devch]; def == nil {
		fc := &FileSinkConfig{
			Channels: ChannelList{Channels: []logpb.Channel{devch}},
		}
		fc.prefix = "default"
		progagateFileDefaults(&fc.FileDefaults, c.FileDefaults)
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

func (c *Config) validateFileSinkConfig(fc *FileSinkConfig, defaultLogDir *string) error {
	progagateFileDefaults(&fc.FileDefaults, c.FileDefaults)
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

	return nil
}

func (c *Config) validateFluentSinkConfig(fc *FluentSinkConfig) error {
	progagateFluentDefaults(&fc.FluentDefaults, c.FluentDefaults)
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

func progagateCommonDefaults(target *CommonSinkConfig, source CommonSinkConfig) {
	progagateDefaults(target, source)
}

func progagateFileDefaults(target *FileDefaults, source FileDefaults) {
	progagateDefaults(target, source)
}

func progagateFluentDefaults(target *FluentDefaults, source FluentDefaults) {
	progagateDefaults(target, source)
}

// progagateDefaults takes (target *T, source T) where T is a struct
// and sets zero-valued exported fields in target to the values
// from source (recursively for struct-valued fields).
// Wrap for static type-checking, as unexpected types will panic.
//
// (Consider making this a common utility if it gets some maturity here.)
func progagateDefaults(target, source interface{}) {
	s := reflect.ValueOf(source)
	t := reflect.Indirect(reflect.ValueOf(target)) // *target

	for i := 0; i < t.NumField(); i++ {
		tf := t.Field(i)
		sf := s.Field(i)
		if tf.Kind() == reflect.Struct {
			progagateDefaults(tf.Addr().Interface(), sf.Interface())
		} else if tf.CanSet() && tf.IsZero() {
			tf.Set(s.Field(i))
		}
	}
}
