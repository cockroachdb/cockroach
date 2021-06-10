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
const DefaultFileFormat = `crdb-v2`

// DefaultStderrFormat is the entry format for stderr sinks
// when not specified in a configuration.
const DefaultStderrFormat = `crdb-v2-tty`

// DefaultFluentFormat is the entry format for fluent sinks
// when not specified in a configuration.
const DefaultFluentFormat = `json-fluent-compact`

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
fluent-defaults:
    filter: INFO
    format: ` + DefaultFluentFormat + `
    redactable: true
    exit-on-error: false
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

	// FluentDefaults represents the default configuration for fluent sinks,
	// inherited when a specific fluent sink config does not provide a
	// configuration value.
	FluentDefaults FluentDefaults `yaml:"fluent-defaults,omitempty"`

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
	// it enables `exit-on-error` and changes the format of files
	// from `crdb-v1` to `crdb-v1-count`.
	Auditable *bool `yaml:",omitempty"`
}

// SinkConfig represents the sink configurations.
type SinkConfig struct {
	// FileGroups represents the list of configured file sinks.
	FileGroups map[string]*FileSinkConfig `yaml:"file-groups,omitempty"`
	// FluentServer represents the list of configured fluent sinks.
	FluentServers map[string]*FluentSinkConfig `yaml:"fluent-servers,omitempty"`
	// Stderr represents the configuration for the stderr sink.
	Stderr StderrSinkConfig `yaml:",omitempty"`

	// sortedFileGroupNames and sortedServerNames are used internally to
	// make the Export() function deterministic.
	sortedFileGroupNames []string
	sortedServerNames    []string
}

// StderrSinkConfig represents the configuration for the stderr sink.
//
// User-facing documentation follows.
// TITLE: Standard error stream
//
// The standard error output stream of the running `cockroach`
// process.
//
// The configuration key under the `sinks` key in the YAML configuration
// is `stderr`. Example configuration:
//
//     sinks:
//        stderr:           # standard error sink configuration starts here
//           channels: DEV
//
// {{site.data.alerts.callout_info}}
// The server start-up messages are still emitted at the start of the standard error
// stream even when logging to `stderr` is enabled. This makes it generally difficult
// to automate integration of `stderr` with log analyzers. Generally, we recommend using
// [file logging](#output-to-files) or [network logging](#output-to-fluentd-compatible-log-collectors)
// instead of `stderr` when integrating with automated monitoring software.
// {{site.data.alerts.end}}
//
// It is not possible to enable the `redactable` parameter on the `stderr` sink if
// `capture-stray-errors` (i.e., capturing stray error information to files) is disabled.
// This is because when `capture-stray-errors` is disabled, the process's standard error stream
// can contain an arbitrary interleaving of [logging events](eventlog.html) and stray
// errors. It is possible for stray error output to interfere with redaction markers
// and remove the guarantees that information outside of redaction markers does not
// contain sensitive information.
//
// For a similar reason, no guarantee of parsability of the output format is available
// when `capture-stray-errors` is disabled, since the standard error stream can then
// contain an arbitrary interleaving of non-formatted error data.
//
type StderrSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelList `yaml:",omitempty,flow"`

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

// FluentDefaults represent configuration defaults for fluent sinks.
type FluentDefaults struct {
	CommonSinkConfig `yaml:",inline"`
}

// FluentSinkConfig represents the configuration for one fluentd sink.
//
// User-facing documentation follows.
// TITLE: Output to Fluentd-compatible log collectors
//
// This sink type causes logging data to be sent over the network, to
// a log collector that can ingest log data in a
// [Fluentd](https://www.fluentd.org)-compatible protocol.
//
// {{site.data.alerts.callout_danger}}
// TLS is not supported yet: the connection to the log collector is neither
// authenticated nor encrypted. Given that logging events may contain sensitive
// information, care should be taken to keep the log collector and the CockroachDB
// node close together on a private network, or connect them using a secure VPN.
// TLS support may be added at a later date.
// {{site.data.alerts.end}}
//
// At the time of this writing, a Fluent sink buffers at most one log
// entry and retries sending the event at most one time if a network
// error is encountered. This is just sufficient to tolerate a restart
// of the Fluentd collector after a configuration change under light
// logging activity. If the server is unavailable for too long, or if
// more than one error is encountered, an error is reported to the
// process's standard error output with a copy of the logging event and
// the logging event is dropped.
//
// The configuration key under the `sinks` key in the YAML
// configuration is `fluent-servers`. Example configuration:
//
//     sinks:
//        fluent-servers:        # fluent configurations start here
//           health:             # defines one sink called "health"
//              channels: HEALTH
//              address: 127.0.0.1:5170
//
// Every new server sink configured automatically inherits the configurations set in the `fluent-defaults` section.
//
// For example:
//
//      fluent-defaults:
//          redactable: false # default: disable redaction markers
//      sinks:
//        fluent-servers:
//          health:
//             channels: HEALTH
//             # This sink has redactable set to false,
//             # as the setting is inherited from fluent-defaults
//             # unless overridden here.
//
// The default output format for Fluent sinks is
// `json-fluent-compact`. The `fluent` variants of the JSON formats
// include a `tag` field as required by the Fluentd protocol, which
// the non-`fluent` JSON [format variants](log-formats.html) do not include.
//
// {{site.data.alerts.callout_info}}
// Run `cockroach debug check-log-config` to verify the effect of defaults inheritance.
// {{site.data.alerts.end}}
//
type FluentSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelList `yaml:",omitempty,flow"`

	// Net is the protocol for the fluent server. Can be "tcp", "udp",
	// "tcp4", etc.
	Net string `yaml:",omitempty"`

	// Address is the network address of the fluent server. The
	// host/address and port parts are separated with a colon. IPv6
	// numeric addresses should be included within square brackets,
	// e.g.: [::1]:1234.
	Address string `yaml:""`

	// FluentDefaults contains the defaultable fields of the config.
	FluentDefaults `yaml:",inline"`

	// serverName is populated/used during validation.
	serverName string
}

// FileDefaults represent configuration defaults for file sinks.
type FileDefaults struct {
	// Dir specifies the output directory for files generated by this sink.
	Dir *string `yaml:",omitempty"`

	// MaxFileSize is the approximate maximum size of individual files
	// generated by this sink. If zero, there is no maximum size.
	MaxFileSize *ByteSize `yaml:"max-file-size,omitempty"`

	// MaxGroupSize is the approximate maximum combined size of all files
	// to be preserved for this sink. An asynchronous garbage collection
	// removes files that cause the file set to grow beyond this specified
	// size. If zero, old files are not removed.
	MaxGroupSize *ByteSize `yaml:"max-group-size,omitempty"`

	// BufferedWrites specifies whether to buffer log entries.
	// Setting this to false flushes log writes upon every entry.
	BufferedWrites *bool `yaml:"buffered-writes,omitempty"`

	// CommonSinkConfig is the configuration common to all sinks. Note
	// that although the idiom in Go is to place embedded fields at the
	// beginning of a struct, we purposefully deviate from the idiom
	// here to ensure that "general" options appear after the
	// sink-specific options in YAML config dumps.
	CommonSinkConfig `yaml:",inline"`
}

// FileSinkConfig represents the configuration for one file sink.
//
// User-facing documentation follows.
// TITLE: Output to files
//
// This sink type causes logging data to be captured into log files in a
// configurable logging directory.
//
// The configuration key under the `sinks` key in the YAML
// configuration is `file-groups`. Example configuration:
//
//     sinks:
//        file-groups:           # file group configurations start here
//           health:             # defines one group called "health"
//              channels: HEALTH
//
// Each generated log file is prefixed by the name of the process,
// followed by the name of the group, separated by a hyphen. For example,
// the group `health` will generate files named `cockroach-health.XXX.log`,
// assuming the process is named `cockroach`. (A user can influence the
// prefix by renaming the program executable.)
//
// The files are named so that a lexicographical sort of the
// directory contents presents the file in creation order.
//
// A symlink (e.g. `cockroach-health.log`) for each group points to the latest generated log file.
//
// Every new file group sink configured automatically inherits
// the configurations set in the `file-defaults` section.
//
// For example:
//
//      file-defaults:
//          redactable: false # default: disable redaction markers
//          dir: logs
//      sinks:
//        file-groups:
//          health:
//             channels: HEALTH
//             # This sink has redactable set to false,
//             # as the setting is inherited from file-defaults
//             # unless overridden here.
//             #
//             # Example override:
//             dir: health-logs # override the default 'logs'
//
// {{site.data.alerts.callout_success}}
// Run `cockroach debug check-log-config` to verify the effect of defaults inheritance.
// {{site.data.alerts.end}}
//
type FileSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelList `yaml:",omitempty,flow"`

	// FileDefaults contains the defaultable fields of the config.
	FileDefaults `yaml:",inline"`

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
	if reflect.DeepEqual(c.Channels, channelValues) {
		return "all", nil
	}
	a := make([]string, len(c.Channels))
	for i, ch := range c.Channels {
		a[i] = ch.String()
	}
	return a, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *ChannelList) UnmarshalYAML(fn func(interface{}) error) error {
	// We recognize two formats here: YAML arrays,
	// and a simple string-based format.
	var a []string
	if err := fn(&a); err == nil /* no error: it's an array */ {
		ch, err := selectChannels(false /* invert */, a)
		if err != nil {
			return err
		}
		c.Channels = ch
		return nil
	}

	// It was not an array. Is it a string?
	var s string
	if err := fn(&s); err != nil {
		return err
	}
	// It was a string: use the string configuration format.
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

// parseChannelList recognizes the following formats:
//     all
//     X,Y,Z
//     [all]
//     [X,Y,Z]
//     all except X,Y,Z
//     all except [X,Y,Z]
func parseChannelList(s string) ([]logpb.Channel, error) {
	// We accept mixed case -- normalize everything.
	s = strings.ToUpper(strings.TrimSpace(s))

	// Special case: "ALL" selects all channels.
	if s == "ALL" {
		return SelectAllChannels(), nil
	}

	// If channels starts with "all except", we invert the selection.
	invert := false
	if strings.HasPrefix(s, "ALL EXCEPT ") {
		invert = true
		s = strings.TrimSpace(strings.TrimPrefix(s, "ALL EXCEPT "))
	}

	// Strip the enclosing [...] if present.
	if len(s) > 0 && s[0] == '[' {
		if s[len(s)-1] != ']' {
			return nil, errors.New("unbalanced brackets")
		}
		s = s[1 : len(s)-1]
	}

	// Extract the selection.
	parts := strings.Split(s, ",")
	return selectChannels(invert, parts)
}

func selectChannels(invert bool, parts []string) ([]logpb.Channel, error) {
	chans := make([]logpb.Channel, 0, len(parts))
	for _, p := range parts {
		// Remove stray spaces around the separating comma.
		p = strings.ToUpper(strings.TrimSpace(p))

		if p == "ALL" {
			if len(parts) != 1 {
				return nil, errors.New("cannot use ALL if there are other channel names present in the list")
			}
			return SelectAllChannels(), nil
		}

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

// SelectAllChannels returns a copy of channelValues,
// for use in the ALL configuration.
func SelectAllChannels() []logpb.Channel {
	// Copy the default in case the code that uses a Config overwrites
	// its channel list in-place.
	chans := make([]logpb.Channel, 0, len(channelValues))
	return append(chans, channelValues...)
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
