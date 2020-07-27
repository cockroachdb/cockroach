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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"gopkg.in/yaml.v2"
)

// DefaultConfig returns a suitable default configuration.
func DefaultConfig() (c Config) {
	const defaultConfig = `
file-defaults:
    filter: INFO
    redactable: true
    max-file-size: 10mib
    max-group-size: 100mib
sinks:
  stderr:
    filter: NONE
    redactable: true
    channels: all
capture-stray-errors:
  enable: true
`
	if err := yaml.UnmarshalStrict([]byte(defaultConfig), &c); err != nil {
		panic(err)
	}
	return c
}

// Config represents the top-level logging configuration.
type Config struct {
	FileDefaults FileDefaults     `yaml:"file-defaults"`
	Sinks        SinkConfig       `yaml:",omitempty"`
	CaptureFd2   CaptureFd2Config `yaml:"capture-stray-errors,omitempty"`
}

// CaptureFd2Config represents the configuration for the fd2 capture sink.
type CaptureFd2Config struct {
	Enable bool
	Dir    *string `yaml:",omitempty"`
}

// SinkConfig represents the sink configurations.
type SinkConfig struct {
	FileGroups    map[string]*FileConfig   `yaml:"file-groups,omitempty"`
	FluentServers map[string]*FluentConfig `yaml:"fluent-servers,omitempty"`
	Stderr        StderrConfig             `yaml:",omitempty"`
}

// StderrConfig represents the configuration for the stderr sink.
type StderrConfig struct {
	NoColor    bool           `yaml:"no-color,omitempty'`
	Filter     logpb.Severity `yaml:",omitempty"`
	Redact     bool           `yaml:",omitempty"`
	Redactable bool           `yaml:",omitempty"`
	Channels   ChannelList    `yaml:",omitempty"`
}

// FluentConfig represents the configuration for one fluentd sink.
type FluentConfig struct {
	Filter     logpb.Severity `yaml:",omitempty"`
	Net        string         `yaml:",omitempty"`
	Address    string
	Redact     bool        `yaml:",omitempty"`
	Redactable *bool       `yaml:",omitempty"`
	Channels   ChannelList `yaml:",omitempty"`

	// used during validation.
	serverName string
}

// FileDefaults represent configuration defaults for file sinks.
type FileDefaults struct {
	Dir          *string `yaml:",omitempty"`
	Filter       logpb.Severity
	Redact       bool
	Redactable   bool
	MaxFileSize  ByteSize `yaml:"max-file-size"`
	MaxGroupSize ByteSize `yaml:"max-group-size"`
}

// FileConfig represents the configuration for one file sink.
type FileConfig struct {
	Dir          *string        `yaml:",omitempty"`
	Filter       logpb.Severity `yaml:",omitempty"`
	Redact       *bool          `yaml:",omitempty"`
	Redactable   *bool          `yaml:",omitempty"`
	MaxFileSize  *ByteSize      `yaml:"max-file-size,omitempty"`
	MaxGroupSize *ByteSize      `yaml:"max-group-size,omitempty"`
	Channels     ChannelList    `yaml:",omitempty"`

	// prefix is populated during validation.
	prefix string
}

// Prefix returns the file prefix. This is only available after
// Validate() has been called.
func (f FileConfig) Prefix() string { return f.prefix }

var _ yaml.Marshaler = (*logpb.Severity)(nil)
var _ yaml.Marshaler = (*ByteSize)(nil)
var _ yaml.Marshaler = (*ChannelList)(nil)
var _ yaml.Unmarshaler = (*logpb.Severity)(nil)
var _ yaml.Unmarshaler = (*ByteSize)(nil)
var _ yaml.Unmarshaler = (*ChannelList)(nil)

// ChannelList represents a list of channels.
type ChannelList struct {
	Channels []logpb.Channel
}

// String implements the fmt.Strinter interface.
func (c ChannelList) String() string {
	var buf strings.Builder
	comma := ""
	for _, cl := range c.Channels {
		buf.WriteString(comma)
		buf.WriteString(cl.String())
		comma = ","
	}
	return buf.String()
}

// HasChannel returns true iff the list contains the given channel.
func (c ChannelList) HasChannel(ch logpb.Channel) bool {
	for _, cl := range c.Channels {
		if cl == ch {
			return true
		}
	}
	return false
}

// MarshalYAML implements the yaml.Marshaler interface.
func (c ChannelList) MarshalYAML() (interface{}, error) {
	return c.String(), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *ChannelList) UnmarshalYAML(fn func(interface{}) error) error {
	// Expand the YAML input into a raw string.
	var s string
	if err := fn(&s); err != nil {
		return err
	}

	// We accept mixed case -- normalize everything.
	s = strings.ToUpper(strings.TrimSpace(s))

	// Special case: "ALL" selects all channels.
	if s == "ALL" {
		c.Channels = channelValues
		return nil
	}

	// If channels starts with "all except", we invert the selection.
	invert := false
	if strings.HasPrefix(s, "ALL EXCEPT ") {
		invert = true
		s = strings.TrimPrefix(s, "ALL EXCEPT ")
	}

	// Extract the selection.
	parts := strings.Split(s, ",")
	chans := make([]logpb.Channel, 0, len(parts))
	for _, p := range parts {
		// Remove stray spaces around the separating comma.
		p = strings.TrimSpace(p)
		// Verify the channel name is known.
		c, ok := logpb.Channel_value[p]
		if !ok {
			return errors.Newf("unknown channel name: %q", p)
		}
		// Reject duplicates.
		for _, existing := range chans {
			if logpb.Channel(c) == existing {
				return errors.Newf("duplicate channel name: %q", p)
			}
		}
		chans = append(chans, logpb.Channel(c))
	}

	if !invert {
		c.Channels = chans
	} else {
		// "ALL EXCEPT" was specified: select all channels except those
		// specified.
		selected := make([]logpb.Channel, 0, len(channelValues))
		for _, ch := range channelValues {
			rejected := false
			for _, rej := range chans {
				if rej == ch {
					rejected = true
					break
				}
			}
			if !rejected {
				selected = append(selected, ch)
			}
		}
		c.Channels = selected
	}
	return nil
}

// channelValues contains the sorted list of channel identifiers. We
// use a sorted list to ensure that reference log configurations are
// deterministic.
var channelValues = func() []logpb.Channel {
	is := make([]int, 0, len(logpb.Channel_name))
	for c := range logpb.Channel_name {
		is = append(is, int(c))
	}
	sort.Ints(is)
	ns := make([]logpb.Channel, len(is))
	for i, c := range is {
		ns[i] = logpb.Channel(c)
	}
	return ns
}()

// ByteSize represents a size in bytes.
type ByteSize uint64

// String implements the fmt.Stringer interface.
func (s ByteSize) String() string {
	return strings.ReplaceAll(humanize.IBytes(uint64(s)), " ", "")
}

// IsZero implements the yaml.IsZeroer interface.
func (x ByteSize) IsZero() bool { return x == 0 }

// MarshalYAML implements the yaml.Marshaler interface.
func (x ByteSize) MarshalYAML() (interface{}, error) {
	return x.String(), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (x *ByteSize) UnmarshalYAML(fn func(interface{}) error) error {
	var s string
	if err := fn(&s); err != nil {
		return err
	}
	i, err := humanize.ParseBytes(s)
	if err != nil {
		return err
	}
	*x = ByteSize(i)
	return nil
}

// Holder is a configuration holder to interface with pflag.
type Holder struct {
	Config Config `yaml:",flow"`
}

// String implements the pflag.Value interface.
func (h *Holder) String() string {
	b, err := yaml.Marshal(h)
	if err != nil {
		return fmt.Sprintf("<INVALID CONFIG: %v>", err)
	}
	return string(b)
}

// Type implements the pflag.Value interface.
func (*Holder) Type() string { return "yaml" }

// Set implements the pflag.Value interface.
func (h *Holder) Set(value string) error {
	return yaml.UnmarshalStrict([]byte(value), &h.Config)
}
