// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logconfig

import (
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
	yaml "gopkg.in/yaml.v2"
)

// DefaultFileFormat is the entry format for file sinks when not
// specified in a configuration.
const DefaultFileFormat = `crdb-v2`

// DefaultStderrFormat is the entry format for stderr sinks.
const DefaultStderrFormat = `crdb-v2-tty`

// DefaultFluentFormat is the entry format for fluent sinks
// when not specified in a configuration.
const DefaultFluentFormat = `json-fluent-compact`

// DefaultHTTPFormat is the entry format for HTTP sinks
// when not specified in a configuration.
const DefaultHTTPFormat = `json-compact`

// DefaultFilePerms is the default permissions used in file-defaults. It
// is applied literally via os.Chmod, without considering the umask.
const DefaultFilePerms = FilePermissions(0o640)

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
    buffering:
      max-staleness: 5s
      flush-trigger-size: 1mib
      max-buffer-size: 50mib
http-defaults:
    filter: INFO
    format: ` + DefaultHTTPFormat + `
    redactable: true
    exit-on-error: false
    timeout: 2s
    buffering:
      max-staleness: 5s	
      flush-trigger-size: 1mib
      max-buffer-size: 50mib
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

	// HTTPDefaults represents the default configuration for HTTP sinks,
	// inherited when a specific HTTP sink config does not provide a
	// configuration value.
	HTTPDefaults HTTPDefaults `yaml:"http-defaults,omitempty"`

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

// CommonBufferSinkConfig represents the common buffering configuration for sinks.
//
// User-facing documentation follows.
// TITLE: Common buffering configuration
// Buffering may be configured with the following fields. It may also be explicitly
// set to "NONE" to disable buffering. Example configuration:
//
//	file-defaults:
//	   dir: logs
//	   buffering:
//	      max-staleness: 20s
//	      flush-trigger-size: 25KB
//	      max-buffer-size: 10MB
//	sinks:
//	   file-groups:
//	      health:
//	         channels: HEALTH
//	         buffering:
//	            max-staleness: 5s  # Override max-staleness for this sink.
//	      ops:
//	         channels: OPS
//	         buffering: NONE  # Disable buffering for this sink.
type CommonBufferSinkConfig struct {
	// MaxStaleness is the maximum time a log message will sit in the buffer
	// before a flush is triggered.
	MaxStaleness *time.Duration `yaml:"max-staleness"`

	// FlushTriggerSize is the number of bytes that will trigger the buffer
	// to flush.
	FlushTriggerSize *ByteSize `yaml:"flush-trigger-size"`

	// MaxBufferSize is the limit on the size of the messages that are buffered.
	// If this limit is exceeded, messages are dropped. The limit is expected to
	// be higher than FlushTriggerSize. A buffer is flushed as soon as
	// FlushTriggerSize is reached, and a new buffer is created once the flushing
	// is started. Only one flushing operation is active at a time.
	MaxBufferSize *ByteSize `yaml:"max-buffer-size"`

	// Format describes how the buffer output should be formatted.
	// Currently 2 options:
	// newline: default option - separates buffer entries with newline char
	// json-array: separates entries with ',' and wraps buffer contents in square brackets
	Format *BufferFormat `yaml:",omitempty"`
}

// CommonBufferSinkConfigWrapper is a BufferSinkConfig with a special value represented in YAML by
// the string "NONE", which actively disables buffering (in the sense that it overrides
// buffering that is enabled by default).
// This is a separate type so that marshaling and unmarshaling of the inner BufferSinkConfig
// can be handled by the library without causing infinite recursion.
type CommonBufferSinkConfigWrapper struct {
	CommonBufferSinkConfig
}

// CommonSinkConfig represents the common configuration shared across all sinks.
type CommonSinkConfig struct {
	// Filter specifies the default minimum severity for log events to
	// be emitted to this sink, when not otherwise specified by the
	// 'channels' sink attribute.
	Filter logpb.Severity `yaml:",omitempty"`

	// Format indicates the entry format to use.
	Format *string `yaml:",omitempty"`

	// FormatOptions indicates additional options for the format.
	FormatOptions map[string]string `yaml:"format-options,omitempty"`

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

	// Buffering configures buffering for this log sink, or NONE to explicitly disable.
	Buffering CommonBufferSinkConfigWrapper `yaml:",omitempty"`
}

// SinkConfig represents the sink configurations.
type SinkConfig struct {
	// FileGroups represents the list of configured file sinks.
	FileGroups map[string]*FileSinkConfig `yaml:"file-groups,omitempty"`
	// FluentServer represents the list of configured fluent sinks.
	FluentServers map[string]*FluentSinkConfig `yaml:"fluent-servers,omitempty"`
	// HTTPServers represents the list of configured http sinks.
	HTTPServers map[string]*HTTPSinkConfig `yaml:"http-servers,omitempty"`
	// Stderr represents the configuration for the stderr sink.
	Stderr StderrSinkConfig `yaml:",omitempty"`
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
//	sinks:
//	   stderr:           # standard error sink configuration starts here
//	      channels: DEV
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
type StderrSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelFilters `yaml:",omitempty,flow"`

	// NoColor forces the omission of VT color codes in the output even
	// when stderr is a terminal. This option is deprecated; its effects
	// are equivalent to 'format-options: {colors: none}'.
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
// This sink type causes logging data to be sent over the network to
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
//	sinks:
//	   fluent-servers:        # fluent configurations start here
//	      health:             # defines one sink called "health"
//	         channels: HEALTH
//	         address: 127.0.0.1:5170
//
// Every new server sink configured automatically inherits the configurations set in the `fluent-defaults` section.
//
// For example:
//
//	fluent-defaults:
//	    redactable: false # default: disable redaction markers
//	sinks:
//	  fluent-servers:
//	    health:
//	       channels: HEALTH
//	       # This sink has redactable set to false,
//	       # as the setting is inherited from fluent-defaults
//	       # unless overridden here.
//
// The default output format for Fluent sinks is
// `json-fluent-compact`. The `fluent` variants of the JSON formats
// include a `tag` field as required by the Fluentd protocol, which
// the non-`fluent` JSON [format variants](log-formats.html) do not include.
//
// {{site.data.alerts.callout_info}}
// Run `cockroach debug check-log-config` to verify the effect of defaults inheritance.
// {{site.data.alerts.end}}
type FluentSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelFilters `yaml:",omitempty,flow"`

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

	// FilePermissions is the "chmod-style" permissions the log files are
	// created with as a 3-digit octal number. The executable bit must not
	// be set. Defaults to 644 (readable by all, writable by owner).
	FilePermissions *FilePermissions `yaml:"file-permissions,omitempty"`

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
//	sinks:
//	   file-groups:           # file group configurations start here
//	      health:             # defines one group called "health"
//	         channels: HEALTH
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
//	file-defaults:
//	    redactable: false # default: disable redaction markers
//	    dir: logs
//	sinks:
//	  file-groups:
//	    health:
//	       channels: HEALTH
//	       # This sink has redactable set to false,
//	       # as the setting is inherited from file-defaults
//	       # unless overridden here.
//	       #
//	       # Example override:
//	       dir: health-logs # override the default 'logs'
//
// {{site.data.alerts.callout_success}}
// Run `cockroach debug check-log-config` to verify the effect of defaults inheritance.
// {{site.data.alerts.end}}
type FileSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelFilters `yaml:",omitempty,flow"`

	// FileDefaults contains the defaultable fields of the config.
	FileDefaults `yaml:",inline"`

	// prefix is populated during validation.
	prefix string
}

var GzipCompression = "gzip"
var NoneCompression = "none"

// HTTPDefaults refresents the configuration defaults for HTTP sinks.
type HTTPDefaults struct {
	// Address is the network address of the http server. The
	// host/address and port parts are separated with a colon. IPv6
	// numeric addresses should be included within square brackets,
	// e.g.: [::1]:1234.
	Address *string `yaml:",omitempty"`

	// Method is the HTTP method to be used.  POST and GET are
	// supported; defaults to POST.
	Method *HTTPSinkMethod `yaml:",omitempty"`

	// UnsafeTLS enables certificate authentication to be bypassed.
	// Defaults to false.
	UnsafeTLS *bool `yaml:"unsafe-tls,omitempty"`

	// Timeout is the HTTP timeout.
	// Defaults to 0 for no timeout.
	Timeout *time.Duration `yaml:",omitempty"`

	// DisableKeepAlives causes the logging sink to re-establish a new
	// connection for every outgoing log message. This option is
	// intended for testing only and can cause excessive network
	// overhead in production systems.
	DisableKeepAlives *bool `yaml:"disable-keep-alives,omitempty"`

	// Headers is a list of headers to attach to each HTTP request
	Headers map[string]string `yaml:",omitempty,flow"`

	// FileBasedHeaders is a list of headers with filepaths whose contents are
	// attached to each HTTP request
	FileBasedHeaders map[string]string `yaml:"file-based-headers,omitempty,flow"`

	// Compression can be "none" or "gzip" to enable gzip compression.
	// Set to "gzip" by default.
	Compression *string `yaml:",omitempty"`

	CommonSinkConfig `yaml:",inline"`
}

// HTTPSinkConfig represents the configuration for one http sink.
//
// User-facing documentation follows.
// TITLE: Output to HTTP servers.
//
// This sink type causes logging data to be sent over the network
// as requests to an HTTP server.
//
// The configuration key under the `sinks` key in the YAML
// configuration is `http-servers`. Example configuration:
//
//	sinks:
//	   http-servers:
//	      health:
//	         channels: HEALTH
//	         address: http://127.0.0.1
//
// Every new server sink configured automatically inherits the configuration set in the `http-defaults` section.
//
// For example:
//
//	http-defaults:
//	    redactable: false # default: disable redaction markers
//	sinks:
//	  http-servers:
//	    health:
//	       channels: HEALTH
//	       # This sink has redactable set to false,
//	       # as the setting is inherited from fluent-defaults
//	       # unless overridden here.
//
// The default output format for HTTP sinks is
// `json-compact`. [Other supported formats.](log-formats.html)
//
// {{site.data.alerts.callout_info}}
// Run `cockroach debug check-log-config` to verify the effect of defaults inheritance.
// {{site.data.alerts.end}}
type HTTPSinkConfig struct {
	// Channels is the list of logging channels that use this sink.
	Channels ChannelFilters `yaml:",omitempty,flow"`

	HTTPDefaults `yaml:",inline"`

	// sinkName is populated during validation.
	sinkName string
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
var _ yaml.Marshaler = (*ChannelFilters)(nil)
var _ yaml.Unmarshaler = (*logpb.Severity)(nil)
var _ yaml.Unmarshaler = (*ByteSize)(nil)
var _ yaml.Unmarshaler = (*ChannelList)(nil)
var _ yaml.Unmarshaler = (*ChannelFilters)(nil)

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
	err := fn(&a)
	if err == nil /* no error: it's an array */ {
		ch, err := selectChannels(false /* invert */, a)
		if err != nil {
			return err
		}
		c.Channels = ch
		return nil
	} else if !errors.HasType(err, (*yaml.TypeError)(nil)) {
		// Another error than a structural error which we can cover
		// below. Abort early.
		return err
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
//
//	all
//	X,Y,Z
//	[all]
//	[X,Y,Z]
//	all except X,Y,Z
//	all except [X,Y,Z]
func parseChannelList(s string) ([]logpb.Channel, error) {
	// We accept mixed case -- normalize everything.
	s = strings.ToUpper(strings.TrimSpace(s))

	// Special case: "ALL" selects all channels.
	if s == "ALL" {
		return AllChannels(), nil
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
			return AllChannels(), nil
		}

		// Verify the channel name is known.
		c, ok := logpb.Channel_value[p]
		if !ok || c >= int32(logpb.Channel_CHANNEL_MAX) {
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

// AllChannels returns a copy of channelValues,
// for use in the ALL configuration.
func AllChannels() []logpb.Channel {
	// Copy the default in case the code that uses a Config overwrites
	// its channel list in-place.
	chans := make([]logpb.Channel, 0, len(channelValues))
	return append(chans, channelValues...)
}

// channelValues contains the sorted list of channel identifiers. We
// use a sorted list to ensure that reference log configurations are
// deterministic.
var channelValues = func() []logpb.Channel {
	is := make([]int, 0, len(logpb.Channel_name)-1)
	for c := range logpb.Channel_name {
		if c == int32(logpb.Channel_CHANNEL_MAX) {
			continue
		}
		is = append(is, int(c))
	}
	sort.Ints(is)
	ns := make([]logpb.Channel, len(is))
	for i, c := range is {
		ns[i] = logpb.Channel(c)
	}
	return ns
}()

// ChannelFilters represents a map of severities to channels.
type ChannelFilters struct {
	// Filters represents the input configuration.
	Filters map[logpb.Severity]ChannelList

	// AllChannels lists all the channels listed in the configuration.
	// Populated/overwritten by Update().
	AllChannels ChannelList
	// ChannelFilters inverts the configured filters, for
	// use internally when instantiating the configuration.
	// Populated/overwritten by Update().
	ChannelFilters map[logpb.Channel]logpb.Severity
}

// SelectChannels is a constructor for ChannelFilters that
// helps in tests.
func SelectChannels(chs ...logpb.Channel) ChannelFilters {
	return ChannelFilters{
		Filters: map[logpb.Severity]ChannelList{
			logpb.Severity_UNKNOWN: {Channels: chs}}}
}

// AddChannel adds the given channel at the specified severity.
func (c *ChannelFilters) AddChannel(ch logpb.Channel, sev logpb.Severity) {
	if c.Filters == nil {
		c.Filters = make(map[logpb.Severity]ChannelList)
	}
	list := c.Filters[sev]
	list.Channels = append(list.Channels, ch)
	c.Filters[sev] = list
}

// MarshalYAML implements the yaml.Marshaler interface.
func (c ChannelFilters) MarshalYAML() (interface{}, error) {
	// If there is just one filter at unknown severity, this means we
	// are observing the result of a config read from a simple string.
	// Simplify to the same on the way out.
	if cfg, ok := c.Filters[logpb.Severity_UNKNOWN]; ok && len(c.Filters) == 1 {
		return &cfg, nil
	}
	// General form: produce a map.
	return c.Filters, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *ChannelFilters) UnmarshalYAML(fn func(interface{}) error) error {
	// We recognize two formats here: either a map of
	// severity to channel lists, or a single channel list.
	var a ChannelList
	err := fn(&a)
	if err == nil /* no error: it's a simple channel list */ {
		c.Filters = map[logpb.Severity]ChannelList{
			logpb.Severity_UNKNOWN: a,
		}
		return nil
	} else if !errors.HasType(err, (*yaml.TypeError)(nil)) {
		// Not a structural error which we handle with the fallback
		// below. Return early.
		return err
	}

	// It was not a simple channel list. Assume a map.
	return fn(&c.Filters)
}

// fillDefaultSeverityAndPrune replaces instances of severity UNKNOWN
// by the specified default (typically coming from the separate
// Filter field on the sink). It also deletes any entry at severity
// NONE. This also ensures the Filter map exists if it was not created
// yet.
func (c *ChannelFilters) fillDefaultSeverityAndPrune(defSev logpb.Severity) {
	// We're going to modify c.Filters below. Create the map
	// if it does not exist yet.
	if c.Filters == nil {
		c.Filters = make(map[logpb.Severity]ChannelList)
	}
	// Fill in the default into the user-supplied configuration.
	if cfg, ok := c.Filters[logpb.Severity_UNKNOWN]; ok {
		// If there is already a config at the default severity,
		// add the unknown channels to it. This is needed
		// for the special case of adding leftover channels
		// to the DEV sink in Validate().
		defCfg := c.Filters[defSev]
		for _, ch := range cfg.Channels {
			if defCfg.HasChannel(ch) {
				// Channel already in default config. Skip it to avoid
				// duplicates.
				continue
			}
			defCfg.Channels = append(defCfg.Channels, ch)
		}
		c.Filters[defSev] = defCfg
		delete(c.Filters, logpb.Severity_UNKNOWN)
	}

	// If anything was specified at severity NONE, remove it.  This
	// means "no channel connected". This also takes care of removing
	// all entries after the check above if there were multiple channels
	// at severity UNKNOWN initially and defSev is NONE.
	delete(c.Filters, logpb.Severity_NONE)
}

// propagateFilters translates the specification in Filters into
// the helper fields AllChannels and ChannelFilters.
func (c *ChannelFilters) propagateFilters() error {
	c.ChannelFilters = map[logpb.Channel]logpb.Severity{}

	// We use a sorted array of severities, to ensure that the list is
	// traversed deterministically. This ensures that tests that check
	// the error condition above have a stable output.
	allUsedSeverities := make([]int, 0, len(c.Filters))
	for sev := range c.Filters {
		allUsedSeverities = append(allUsedSeverities, int(sev))
	}
	sort.Ints(allUsedSeverities)

	// Translate the Filters to ChannelFilters.
	for _, iSev := range allUsedSeverities {
		sev := logpb.Severity(iSev)
		cl := c.Filters[sev]
		for _, ch := range cl.Channels {
			if prevSev, ok := c.ChannelFilters[ch]; ok {
				return errors.Newf("cannot use channel %s at severity %s: already listed at severity %s", ch, sev, prevSev)
			}
			c.ChannelFilters[ch] = sev
		}
	}

	// Extract a sorted list of all channels from ChannelFilters into AllChannels.
	c.AllChannels.Channels = make([]logpb.Channel, 0, len(c.ChannelFilters))
	for ch := range c.ChannelFilters {
		c.AllChannels.Channels = append(c.AllChannels.Channels, ch)
	}
	c.AllChannels.Sort()

	return nil
}

// Validate changes the expressed filters to substitute the UNKNOWN
// severity by the proposed default; it also propagates the user
// configuration into the other accessor fields.
func (c *ChannelFilters) Validate(defSev logpb.Severity) error {
	c.fillDefaultSeverityAndPrune(defSev)

	// Ensure the configurations are stable to make tests deterministic.
	for sev, cfg := range c.Filters {
		cfg.Sort()
		c.Filters[sev] = cfg
	}

	return c.propagateFilters()
}

// noChannelsSelected returns true if there are no channels listed or
// all of them are filtered at level NONE.
func (c ChannelFilters) noChannelsSelected() bool {
	filtered := true
	for sev := range c.Filters {
		if sev != logpb.Severity_NONE {
			filtered = false
			break
		}
	}
	return filtered
}

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

// FilePermissions is a 9-bit number corresponding to the fs.FileMode
// a logfile is created with. It's written and read from the YAML as
// an octal string, regardless of leading zero or "0o" prefix.
type FilePermissions uint

// IsZero implements the yaml.IsZeroer interface.
func (x FilePermissions) IsZero() bool { return x == 0 }

// MarshalYAML implements the yaml.Marshaler interface.
func (x FilePermissions) MarshalYAML() (r interface{}, err error) {
	return fmt.Sprintf("%04o", x), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (x *FilePermissions) UnmarshalYAML(fn func(interface{}) error) (err error) {
	defer func() {
		if err != nil {
			err = errors.WithHint(err, "This value should consist of three even digits.")
		}
	}()

	var in string
	if err = fn(&in); err != nil {
		return err
	}

	in = strings.TrimPrefix(in, "0o")
	val, err := strconv.ParseInt(in, 8, 0)
	if err != nil {
		return errors.Errorf("file-permissions unparsable: %v", in)
	}
	if val > 0o777 || val < 0 {
		return errors.Errorf("file-permissions out-of-range: %v", in)
	}
	if val&0o111 != 0 {
		return errors.Errorf("file-permissions must not be executable: %v", in)
	}

	*x = FilePermissions(val)
	return nil
}

func init() {
	// Use FutureLineWrap to avoid wrapping long lines. This is required for cases
	// where one of the logging or zone config fields is longer than 80
	// characters. In that case, without FutureLineWrap, the output will have `\n`
	// characters interspersed every 80 characters. FutureLineWrap ensures that
	// the whole field shows up as a single line.
	yaml.FutureLineWrap()
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

// MarshalYAML implements yaml.Marshaler interface.
func (w CommonBufferSinkConfigWrapper) MarshalYAML() (interface{}, error) {
	if w.IsNone() {
		return "NONE", nil
	}
	return w.CommonBufferSinkConfig, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (w *CommonBufferSinkConfigWrapper) UnmarshalYAML(fn func(interface{}) error) error {
	var v string
	if err := fn(&v); err == nil {
		if strings.ToUpper(v) == "NONE" {
			d := time.Duration(0)
			s := ByteSize(0)
			format := BufferFmtNewline
			w.CommonBufferSinkConfig = CommonBufferSinkConfig{
				MaxStaleness:     &d,
				FlushTriggerSize: &s,
				MaxBufferSize:    &s,
				Format:           &format,
			}
			return nil
		}
	}
	return fn(&w.CommonBufferSinkConfig)
}

// IsNone before default propagation indicates that the config explicitly disables
// buffering, such that no propagated defaults can actiate them.
// After default propagation, buffering is disabled iff IsNone().
func (w CommonBufferSinkConfigWrapper) IsNone() bool {
	return (w.MaxStaleness != nil && *w.MaxStaleness == 0) &&
		(w.FlushTriggerSize != nil && *w.FlushTriggerSize == 0) &&
		(w.MaxBufferSize != nil && *w.MaxBufferSize == 0)
}

// HTTPSinkMethod is a string restricted to "POST" and "GET"
type HTTPSinkMethod string

var _ constrainedString = (*HTTPSinkMethod)(nil)

// Accept implements the constrainedString interface.
func (hsm *HTTPSinkMethod) Accept(s string) {
	*hsm = HTTPSinkMethod(s)
}

// Canonicalize implements the constrainedString interface.
func (HTTPSinkMethod) Canonicalize(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}

// AllowedSet implements the constrainedString interface.
func (HTTPSinkMethod) AllowedSet() []string {
	return []string{
		http.MethodGet,
		http.MethodPost,
	}
}

// MarshalYAML implements yaml.Marshaler interface.
func (hsm HTTPSinkMethod) MarshalYAML() (interface{}, error) {
	return string(hsm), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (hsm *HTTPSinkMethod) UnmarshalYAML(fn func(interface{}) error) error {
	return unmarshalYAMLConstrainedString(hsm, fn)
}

// constrainedString is an interface to make it easy to unmarshal
// a string constrained to a small set of accepted values.
type constrainedString interface {
	Accept(string)
	Canonicalize(string) string
	AllowedSet() []string
}

const (
	BufferFmtJsonArray BufferFormat = "json-array"
	BufferFmtNewline   BufferFormat = "newline"
	BufferFmtNone      BufferFormat = "none"
)

// BufferFormat is a string restricted to "json-array" and "newline".
type BufferFormat string

var _ constrainedString = (*BufferFormat)(nil)

// Accept implements the constrainedString interface.
func (hsm *BufferFormat) Accept(s string) {
	*hsm = BufferFormat(s)
}

// Canonicalize implements the constrainedString interface.
func (BufferFormat) Canonicalize(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// AllowedSet implements the constrainedString interface.
func (BufferFormat) AllowedSet() []string {
	return []string{string(BufferFmtJsonArray), string(BufferFmtNewline), "unknown"}
}

// MarshalYAML implements yaml.Marshaler interface.
func (hsm BufferFormat) MarshalYAML() (interface{}, error) {
	return string(hsm), nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (hsm *BufferFormat) UnmarshalYAML(fn func(interface{}) error) error {
	return unmarshalYAMLConstrainedString(hsm, fn)
}

// unmarshalYAMLConstrainedString is a utility function to unmarshal
// a type satisfying the constrainedString interface.
func unmarshalYAMLConstrainedString(cs constrainedString, fn func(interface{}) error) error {
	var s string
	if err := fn(&s); err != nil {
		return err
	}
	s = cs.Canonicalize(s)
	for _, candidate := range cs.AllowedSet() {
		if s == candidate {
			cs.Accept(s)
			return nil
		}
	}
	return errors.Newf("Unexpected value: %v", s)
}
