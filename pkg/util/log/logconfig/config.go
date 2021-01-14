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
	"reflect"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"gopkg.in/yaml.v2"
)

// DefaultFileFormat is the entry format for file sinks when not
// specified in a configuration.
const DefaultFileFormat = `crdb-v1`

// DefaultStderrFormat is the entry format for stderr sinks
// when not specified in a configuration.
const DefaultStderrFormat = `crdb-v1-tty`

// DefaultConfig returns a suitable default configuration when logging
// is meant to primarily go to files.
func DefaultConfig() (c Config) {
	// TODO(knz): The default for 'stderr:exit-on-error' should probably be 'false'.
	const defaultConfig = `
file-defaults:
    filter: INFO
    format: ` + DefaultFileFormat + `
    redact: false
    redactable: true
    max-file-size: 10mib
    max-group-size: 100mib
    exit-on-error: true
    buffered-writes: true
sinks:
  stderr:
    filter: NONE
    format: ` + DefaultStderrFormat + `
    redactable: true
    channels: all
    exit-on-error: true
capture-stray-errors:
  enable: true
`
	if err := yaml.UnmarshalStrict([]byte(defaultConfig), &c); err != nil {
		panic(err)
	}
	return c
}

// DefaultStderrConfig returns a suitable default configuration when logging
// is meant to primarily go to stderr.
func DefaultStderrConfig() (c Config) {
	const defaultConfig = `
sinks:
  stderr:
    filter: INFO
    format: ` + DefaultStderrFormat + `
    redactable: false
    channels: all
    exit-on-error: true
capture-stray-errors:
  enable: false
`
	if err := yaml.UnmarshalStrict([]byte(defaultConfig), &c); err != nil {
		panic(err)
	}
	return c
}

// Config represents the top-level logging configuration.
type Config struct {
	// FileDefaults represents the default configuration for file sinks,
	// inherited when a specific file sink config does not provide a
	// configuration value.
	FileDefaults FileDefaults `yaml:"file-defaults,omitempty"`

	// Sinks represents the sink configurations.
	Sinks SinkConfig `yaml:",omitempty"`

	// CaptureFd2 represents the configuration for the redirection of
	// internal writes to file descriptor 2 (incl that done internally
	// by the go runtime).
	CaptureFd2 CaptureFd2Config `yaml:"capture-stray-errors,omitempty"`
}

// CaptureFd2Config represents the configuration for the fd2 capture sink.
type CaptureFd2Config struct {
	// Enable determine whether the fd2 capture is enabled.
	Enable bool

	// Dir indicates where to generate the cockroach-stderr.log file
	// when the capture is enabled.
	Dir *string `yaml:",omitempty"`

	// MaxGroupSize stores the default approximate maximum combined size
	// of all fd2 capture files.
	// Garbage collection removes files that cause the file set to grow
	// beyond this specified size.
	MaxGroupSize *ByteSize `yaml:"max-group-size,omitempty"`
}

// SinkConfig represents the sink configurations.
type SinkConfig struct {
	// FileGroups represents the list of configured file sinks.
	FileGroups map[string]*FileConfig `yaml:"file-groups,omitempty"`
	// Stderr represents the configuration for the stderr sink.
	Stderr StderrConfig `yaml:",omitempty"`

	// sortedFileGroupNames is used internally to
	// make the Export() function deterministic.
	sortedFileGroupNames []string
}

// StderrConfig represents the configuration for the stderr sink.
type StderrConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelList `yaml:",omitempty"`

	// NoColor forces the omission of VT color codes in the output even
	// when stderr is a terminal.
	NoColor bool `yaml:"no-color,omitempty"`

	// CommonSinkConfig is the configuration common to all sinks. Note
	// that although the idiom in Go is to place embedded fields at the
	// beginning of a struct, we purposefully deviate from the idiom
	// here to ensure that "general" options appear after the
	// sink-specific options in YAML config dumps.
	CommonSinkConfig `yaml:",inline"`
}

// CommonSinkConfig represents the common configuration shared across all sinks.
type CommonSinkConfig struct {
	// Filter indicates the minimum severity for log events to be
	// emitted to this sink. This can be set to NONE to disable the
	// sink.
	Filter logpb.Severity `yaml:",omitempty"`

	// Format indicates the entry format to use.
	Format *string `yaml:",omitempty"`

	// Redact indicates whether to strip sensitive information before
	// log events are emitted to this sink.
	Redact *bool `yaml:",omitempty"`

	// Redactable indicates whether to keep redaction markers in the
	// sink's output. The presence of redaction markers makes it possible
	// to strip sensitive data reliably.
	Redactable *bool `yaml:",omitempty"`

	// Criticality indicates whether the logging system should terminate
	// the process if an error is encountered while writing to this
	// sink.
	Criticality *bool `yaml:"exit-on-error,omitempty"`

	// Auditable is translated to tweaks to the other settings
	// for this sink during validation. For example,
	// it enables exit-on-error and changes the format of files
	// from crdb-v1 to crdb-v1-count.
	Auditable *bool `yaml:",omitempty"`
}

// FileDefaults represent configuration defaults for file sinks.
type FileDefaults struct {
	// Dir stores the default output directory for file sinks.
	Dir *string `yaml:",omitempty"`

	// MaxFileSize stores the default approximate maximum size of
	// individual files generated by file sinks.
	// If zero, there is no maximum size.
	MaxFileSize ByteSize `yaml:"max-file-size,omitempty"`

	// MaxGroupSize stores the default approximate maximum combined size
	// of all files to be preserved for file sinks. An asynchronous
	// garbage collection removes files that cause the file set to grow
	// beyond this specified size.
	// If zero, old files are not removed.
	MaxGroupSize ByteSize `yaml:"max-group-size,omitempty"`

	// BufferedWrites stores the default setting for buffered-writes on
	// file sinks, which implies keeping a buffer of log entries in memory.
	// Conversely, setting this to false flushes log writes upon every entry.
	BufferedWrites *bool `yaml:"buffered-writes,omitempty"`

	// CommonSinkConfig is the configuration common to all sinks. Note
	// that although the idiom in Go is to place embedded fields at the
	// beginning of a struct, we purposefully deviate from the idiom
	// here to ensure that "general" options appear after the
	// sink-specific options in YAML config dumps.
	CommonSinkConfig `yaml:",inline"`
}

// FileConfig represents the configuration for one file sink.
type FileConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelList `yaml:",omitempty"`

	// Dir specifies the output directory for files generated by this
	// sink. Inherited from FileDefaults.Dir if not specified.
	Dir *string `yaml:",omitempty"`

	// MaxFileSize indicates the approximate maximum size of
	// individual files generated by this sink.
	// Inherited from FileDefaults.MaxFileSize if not specified.
	MaxFileSize *ByteSize `yaml:"max-file-size,omitempty"`

	// MaxGroupSize indicates the approximate maximum combined size
	// of all files to be preserved for this sink. An asynchronous
	// garbage collection removes files that cause the file set to grow
	// beyond this specified size.
	MaxGroupSize *ByteSize `yaml:"max-group-size,omitempty"`

	// BufferedWrites specifies whether to flush on every log write.
	BufferedWrites *bool `yaml:"buffered-writes,omitempty"`

	// CommonSinkConfig is the configuration common to all sinks. Note
	// that although the idiom in Go is to place embedded fields at the
	// beginning of a struct, we purposefully deviate from the idiom
	// here to ensure that "general" options appear after the
	// sink-specific options in YAML config dumps.
	CommonSinkConfig `yaml:",inline"`

	// prefix is populated during validation.
	prefix string
}

// IterateDirectories calls the provided fn on every directory linked to
// by the configuration.
func (c *Config) IterateDirectories(fn func(d string) error) error {
	dirs := make(map[string]struct{})
	if c.CaptureFd2.Enable && c.CaptureFd2.Dir != nil {
		dirs[*c.CaptureFd2.Dir] = struct{}{}
	}
	for _, fc := range c.Sinks.FileGroups {
		if fc.Dir != nil {
			dirs[*fc.Dir] = struct{}{}
		}
	}
	for d := range dirs {
		if err := fn(d); err != nil {
			return err
		}
	}
	return nil
}

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

// String implements the fmt.Stringer and pflag.Value interfaces.
func (c ChannelList) String() string {
	if reflect.DeepEqual(c.Channels, channelValues) {
		return "all"
	}
	var buf strings.Builder
	comma := ""
	for _, cl := range c.Channels {
		buf.WriteString(comma)
		buf.WriteString(cl.String())
		comma = ","
	}
	return buf.String()
}

// Type implements the pflag.Value interface.
func (c ChannelList) Type() string { return "<channel list>" }

// Set implements the pflag.Value interface.
func (c *ChannelList) Set(v string) error {
	ch, err := parseChannelList(v)
	if err != nil {
		return err
	}
	c.Channels = ch
	return nil
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

	return c.Set(s)
}

// Sort ensures a channel list is sorted, for stability.
func (c *ChannelList) Sort() {
	is := make([]int, len(c.Channels))
	for i, ch := range c.Channels {
		is[i] = int(ch)
	}
	sort.Ints(is)
	for i, ci := range is {
		c.Channels[i] = logpb.Channel(ci)
	}
}

func parseChannelList(s string) ([]logpb.Channel, error) {
	// We accept mixed case -- normalize everything.
	s = strings.ToUpper(strings.TrimSpace(s))

	// Special case: "ALL" selects all channels.
	if s == "ALL" {
		// Copy the default in case the code that uses a Config overwrites
		// its channel list in-place.
		chans := make([]logpb.Channel, 0, len(channelValues))
		return append(chans, channelValues...), nil
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
			return nil, errors.Newf("unknown channel name: %q", p)
		}
		// Reject duplicates.
		for _, existing := range chans {
			if logpb.Channel(c) == existing {
				return nil, errors.Newf("duplicate channel name: %q", p)
			}
		}
		chans = append(chans, logpb.Channel(c))
	}

	if !invert {
		return chans, nil
	}

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
	return selected, nil
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
func (x ByteSize) String() string {
	return strings.ReplaceAll(humanize.IBytes(uint64(x)), " ", "")
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

// String implements the fmt.Stringer interface.
func (c *Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintf("<INVALID CONFIG: %v>", err)
	}
	return string(b)
}

// Holder is a configuration holder to interface with pflag,
// which pretty-prints the configuration using the "flow" format.
// The "flow" format is useful for the output of --help.
type Holder struct {
	Config Config `yaml:",flow"`
}

// String implements the pflag.Value interface.
func (h *Holder) String() string {
	b, err := yaml.Marshal(h)
	if err != nil {
		return fmt.Sprintf("<INVALID CONFIG: %v>", err)
	}
	return strings.TrimSpace(string(b))
}

// Type implements the pflag.Value interface.
func (*Holder) Type() string { return "yaml" }

// Set implements the pflag.Value interface.
func (h *Holder) Set(value string) error {
	return yaml.UnmarshalStrict([]byte(value), &h.Config)
}
