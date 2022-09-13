// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"io/fs"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type config struct {
	// showLogs reflects the use of -show-logs on the command line and is
	// used for testing.
	showLogs bool

	// testingLogConfig reflects the use of -test-log-config on the
	// command line and is used for testing.
	testLogConfig string

	// flushWrites can be set asynchronously to force all file output to
	// be flushed to disk immediately. This is set via SetAlwaysFlush()
	// and used e.g. in start.go upon encountering errors.
	flushWrites syncutil.AtomicBool
}

var debugLog *loggerT

// redactionPolicyManaged is the env var used to indicate that the node is being
// run as part of a managed service (e.g. CockroachCloud). Certain logged information
// such as filepaths, network addresses, and CLI argument lists are considered
// sensitive information in on-premises deployments. However, when the node is being
// run as part of a managed service (e.g. CockroachCloud), this type of information is
// no longer considered sensitive, and should be logged in an unredacted form to aid
// in support escalations.
const redactionPolicyManagedEnvVar = "COCKROACH_REDACTION_POLICY_MANAGED"

func init() {
	logflags.InitFlags(
		&logging.showLogs,
		&logging.testLogConfig,
		&logging.vmoduleConfig.mu.vmodule,
	)

	// By default, we use and apply the test configuration.
	// This can be overridden to use output to file in tests
	// using TestLogScope.
	cfg := getTestConfig(nil /* output to files disabled */, true /* mostly inline */)

	if _, err := ApplyConfig(cfg); err != nil {
		panic(err)
	}

	// Reset the "active' flag so that the main commands can reset the
	// configuration.
	logging.mu.active = false
}

// IsActive returns true iff the main logger already has some events
// logged, or some secondary logger was created with configuration
// taken from the main logger.
//
// This is used to assert that configuration is performed
// before logging has been used for the first time.
func IsActive() (active bool, firstUse string) {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	return logging.mu.active, logging.mu.firstUseStack
}

// ApplyConfig applies the given configuration.
//
// The returned logShutdownFn can be used to gracefully shut down logging facilities.
func ApplyConfig(config logconfig.Config) (logShutdownFn func(), err error) {
	// Sanity check.
	if active, firstUse := IsActive(); active {
		panic(errors.Newf("logging already active; first use:\n%s", firstUse))
	}

	// Our own cancellable context to stop the secondary loggers below.
	//
	// Note: we don't want to take a cancellable context from the
	// caller, because in the usual case we don't want to stop the
	// logger when the remainder of the process stops. See the
	// discussion on cancel at the top of the function.
	secLoggersCtx, secLoggersCancel := context.WithCancel(context.Background())

	// secLoggers collects the secondary loggers derived by the configuration.
	var secLoggers []*loggerT
	// sinkInfos collects the sinkInfos derived by the configuration.
	var sinkInfos []*sinkInfo
	// fd2CaptureCleanupFn is the cleanup function for the fd2 capture,
	// which is populated if fd2 capture is enabled, below.
	fd2CaptureCleanupFn := func() {}

	closer := newBufferedSinkCloser()
	// logShutdownFn is the returned cleanup function, whose purpose
	// is to tear down the work we are doing here.
	logShutdownFn = func() {
		// Reset the logging channels to default.
		si := logging.stderrSinkInfoTemplate
		logging.setChannelLoggers(make(map[Channel]*loggerT), &si)
		fd2CaptureCleanupFn()
		secLoggersCancel()
		if err := closer.Close(defaultCloserTimeout); err != nil {
			fmt.Printf("# WARNING: %s\n", err.Error())
		}
		for _, l := range secLoggers {
			logging.allLoggers.del(l)
		}
		for _, l := range sinkInfos {
			logging.allSinkInfos.del(l)
		}
	}

	// Call the final value of logShutdownFn immediately if returning with error.
	defer func() {
		if err != nil {
			logShutdownFn()
		}
	}()

	// We're going to re-define loggers and sinks, so start with a fresh
	// registry.
	logging.allLoggers.clear()
	logging.allSinkInfos.clear()

	// Indicate whether we're running in a managed environment. Impacts redaction policies.
	logging.setManagedRedactionPolicy(envutil.EnvOrDefaultBool(redactionPolicyManagedEnvVar, false))

	// If capture of internal fd2 writes is enabled, set it up here.
	if config.CaptureFd2.Enable {
		if logging.testingFd2CaptureLogger != nil {
			return nil, errors.New("fd2 capture already set up. Maybe use TestLogScope?")
		}
		// We use a secondary logger, even though no logging *event* will ever
		// be logged to it, for the convenience of getting a standard log
		// file header at the beginning of the file (which will contain
		// a timestamp, command-line arguments, etc.).
		secLogger := &loggerT{}
		logging.allLoggers.put(secLogger)
		secLoggers = append(secLoggers, secLogger)

		// A pseudo file sink. Again, for convenience, so we don't need
		// to implement separate file management.
		bt, bf := true, false
		mf := logconfig.ByteSize(math.MaxInt64)
		f := logconfig.DefaultFileFormat
		fm := logconfig.FilePermissions(0o644)
		fakeConfig := logconfig.FileSinkConfig{
			FileDefaults: logconfig.FileDefaults{
				CommonSinkConfig: logconfig.CommonSinkConfig{
					Filter:      severity.INFO,
					Criticality: &bt,
					Format:      &f,
					Redact:      &bf,
					// Be careful about stripping the redaction markers from log
					// entries. The captured fd2 writes are inherently unsafe, so
					// we don't want the header entry to give a mistaken
					// impression to the entry parser.
					Redactable: &bf,
				},
				Dir:             config.CaptureFd2.Dir,
				MaxGroupSize:    config.CaptureFd2.MaxGroupSize,
				MaxFileSize:     &mf,
				BufferedWrites:  &bf,
				FilePermissions: &fm,
			},
			Channels: logconfig.SelectChannels(channel.DEV),
		}
		if err := fakeConfig.Channels.Validate(fakeConfig.CommonSinkConfig.Filter); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err, "programming error: incorrect filter config")
		}
		fileSinkInfo, fileSink, err := newFileSinkInfo("stderr", fakeConfig)
		if err != nil {
			return nil, err
		}
		sinkInfos = append(sinkInfos, fileSinkInfo)
		logging.allSinkInfos.put(fileSinkInfo)

		if fileSink.logFilesCombinedMaxSize > 0 {
			// Do a start round of GC, so clear up past accumulated files.
			fileSink.gcOldFiles()
			// Start the GC process. This ensures that old capture files get
			// erased as new files get created.
			go fileSink.gcDaemon(secLoggersCtx)
		}

		// Connect the sink to the logger.
		secLogger.sinkInfos = []*sinkInfo{fileSinkInfo}

		// Force a log entry. This does two things: it forces the creation
		// of a file and it also introduces a timestamp marker.
		entry := makeUnstructuredEntry(secLoggersCtx, severity.INFO, channel.DEV, 0,
			// Note: we need this entry to be marked as non-redactable since
			// it's going to be followed by junk printed by the go runtime.
			false, /* redactable */
			"stderr capture started")
		secLogger.outputLogEntry(entry)

		// Now tell this logger to capture internal stderr writes.
		if err := fileSink.takeOverInternalStderr(secLogger); err != nil {
			// Oof, it turns out we can't use this logger after all. Give up
			// on everything we did.
			return nil, err
		}

		// Now inform the other functions using stderrLog that we
		// have a new logger for it.
		logging.testingFd2CaptureLogger = secLogger

		fd2CaptureCleanupFn = func() {
			// Relinquish the stderr redirect.
			if err := secLogger.getFileSink().relinquishInternalStderr(); err != nil {
				// This should not fail. If it does, some caller messed up by
				// switching over stderr redirection to a different logger
				// without our involvement. That's invalid API usage.
				panic(err)
			}

			// Restore the apparent stderr logger used by Shout() and tests.
			logging.testingFd2CaptureLogger = nil

			// Note: the remainder of the code in cleanupFn() will remove
			// the logger and close it. No need to also do it here.
		}
	}

	// Apply the stderr sink configuration.
	logging.stderrSink.noColor.Set(config.Sinks.Stderr.NoColor)
	if err := logging.stderrSinkInfoTemplate.applyConfig(config.Sinks.Stderr.CommonSinkConfig); err != nil {
		return nil, err
	}
	logging.stderrSinkInfoTemplate.applyFilters(config.Sinks.Stderr.Channels)

	// Create the per-channel loggers.
	chans := make(map[Channel]*loggerT, len(logpb.Channel_name)-1)
	for chi := range logpb.Channel_name {
		ch := Channel(chi)
		if ch == logpb.Channel_CHANNEL_MAX {
			continue
		}
		chans[ch] = &loggerT{}
		if ch == channel.DEV {
			debugLog = chans[ch]
		}
	}

	// Make a copy of the template so that any subsequent config
	// changes don't race with logging operations.
	stderrSinkInfo := logging.stderrSinkInfoTemplate

	// Connect the stderr channels.
	for _, ch := range config.Sinks.Stderr.Channels.AllChannels.Channels {
		// Note: we connect stderr even if the severity is NONE
		// so that tests can raise the severity after configuration.
		l := chans[ch]
		l.sinkInfos = append(l.sinkInfos, &stderrSinkInfo)
	}

	attachSinkInfo := func(si *sinkInfo, chs *logconfig.ChannelFilters) {
		sinkInfos = append(sinkInfos, si)
		logging.allSinkInfos.put(si)

		// Connect the channels for this sink.
		for _, ch := range chs.AllChannels.Channels {
			l := chans[ch]
			l.sinkInfos = append(l.sinkInfos, si)
		}
	}

	// Create the file sinks.
	for fileGroupName, fc := range config.Sinks.FileGroups {
		if fc.Filter == severity.NONE || fc.Dir == nil {
			continue
		}
		if fileGroupName == "default" {
			fileGroupName = ""
		}
		fileSinkInfo, fileSink, err := newFileSinkInfo(fileGroupName, *fc)
		if err != nil {
			return nil, err
		}
		attachBufferWrapper(fileSinkInfo, fc.CommonSinkConfig.Buffering, closer)
		attachSinkInfo(fileSinkInfo, &fc.Channels)

		// Start the GC process. This ensures that old capture files get
		// erased as new files get created.
		go fileSink.gcDaemon(secLoggersCtx)
	}

	// Create the fluent sinks.
	for _, fc := range config.Sinks.FluentServers {
		if fc.Filter == severity.NONE {
			continue
		}
		fluentSinkInfo, err := newFluentSinkInfo(*fc)
		if err != nil {
			return nil, err
		}
		attachBufferWrapper(fluentSinkInfo, fc.CommonSinkConfig.Buffering, closer)
		attachSinkInfo(fluentSinkInfo, &fc.Channels)
	}

	// Create the HTTP sinks.
	for _, fc := range config.Sinks.HTTPServers {
		if fc.Filter == severity.NONE {
			continue
		}
		httpSinkInfo, err := newHTTPSinkInfo(*fc)
		if err != nil {
			return nil, err
		}
		attachBufferWrapper(httpSinkInfo, fc.CommonSinkConfig.Buffering, closer)
		attachSinkInfo(httpSinkInfo, &fc.Channels)
	}

	// Prepend the interceptor sink to all channels.
	// We prepend it because we want the interceptors
	// to see every event before they make their way to disk/network.
	interceptorSinkInfo := logging.newInterceptorSinkInfo()
	for _, l := range chans {
		l.sinkInfos = append([]*sinkInfo{interceptorSinkInfo}, l.sinkInfos...)
	}

	logging.setChannelLoggers(chans, &stderrSinkInfo)
	setActive()

	return logShutdownFn, nil
}

// newFileSinkInfo creates a new fileSink and its accompanying sinkInfo
// from the provided configuration.
func newFileSinkInfo(
	fileGroupName string, c logconfig.FileSinkConfig,
) (*sinkInfo, *fileSink, error) {
	info := &sinkInfo{}
	if err := info.applyConfig(c.CommonSinkConfig); err != nil {
		return nil, nil, err
	}
	info.applyFilters(c.Channels)
	fileSink := newFileSink(
		*c.Dir,
		fileGroupName,
		*c.BufferedWrites,
		int64(*c.MaxFileSize),
		int64(*c.MaxGroupSize),
		info.getStartLines,
		fs.FileMode(*c.FilePermissions),
	)
	info.sink = fileSink
	return info, fileSink, nil
}

// newFluentSinkInfo creates a new fluentSink and its accompanying sinkInfo
// from the provided configuration.
func newFluentSinkInfo(c logconfig.FluentSinkConfig) (*sinkInfo, error) {
	info := &sinkInfo{}
	if err := info.applyConfig(c.CommonSinkConfig); err != nil {
		return nil, err
	}
	info.applyFilters(c.Channels)
	fluentSink := newFluentSink(c.Net, c.Address)
	info.sink = fluentSink
	return info, nil
}

func newHTTPSinkInfo(c logconfig.HTTPSinkConfig) (*sinkInfo, error) {
	info := &sinkInfo{}

	if err := info.applyConfig(c.CommonSinkConfig); err != nil {
		return nil, err
	}
	info.applyFilters(c.Channels)

	httpSink, err := newHTTPSink(c)
	if err != nil {
		return nil, err
	}
	info.sink = httpSink
	return info, nil
}

// applyFilters applies the channel filters to a sinkInfo.
func (l *sinkInfo) applyFilters(chs logconfig.ChannelFilters) {
	for ch, threshold := range chs.ChannelFilters {
		l.threshold.set(ch, threshold)
	}
}

// attachBufferWrapper modifies s, wrapping its sink in a bufferedSink unless
// bufConfig.IsNone().
//
// The provided closer needs to be closed to stop the bufferedSink internal goroutines.
func attachBufferWrapper(
	s *sinkInfo, bufConfig logconfig.CommonBufferSinkConfigWrapper, closer *bufferedSinkCloser,
) {
	if bufConfig.IsNone() {
		return
	}
	bs := newBufferedSink(
		s.sink,
		*bufConfig.MaxStaleness,
		uint64(*bufConfig.FlushTriggerSize),
		uint64(*bufConfig.MaxBufferSize),
		s.criticality /* crashOnAsyncFlushErr */)
	bs.Start(closer)
	s.sink = bs
}

// applyConfig applies a common sink configuration to a sinkInfo.
func (l *sinkInfo) applyConfig(c logconfig.CommonSinkConfig) error {
	l.threshold.setAll(severity.NONE)
	l.redact = *c.Redact
	l.redactable = *c.Redactable
	l.editor = getEditor(SelectEditMode(*c.Redact, *c.Redactable))
	l.criticality = *c.Criticality
	f, ok := formatters[*c.Format]
	if !ok {
		return errors.Newf("unknown format: %q", *c.Format)
	}
	l.formatter = f
	return nil
}

// describeAppliedConfig reports a sinkInfo's configuration as a
// CommonSinkConfig. Note that the returned config object
// holds into the sinkInfo parameters by reference and thus should
// not be reused if the configuration can change asynchronously.
func (l *sinkInfo) describeAppliedConfig() (c logconfig.CommonSinkConfig) {
	c.Redact = &l.redact
	c.Redactable = &l.redactable
	c.Criticality = &l.criticality
	f := l.formatter.formatterName()
	c.Format = &f
	bufferedSink, ok := l.sink.(*bufferedSink)
	if ok {
		c.Buffering.MaxStaleness = &bufferedSink.maxStaleness
		triggerSize := logconfig.ByteSize(bufferedSink.triggerSize)
		c.Buffering.FlushTriggerSize = &triggerSize
		bufferedSink.mu.Lock()
		maxBufferSize := logconfig.ByteSize(bufferedSink.mu.buf.maxSizeBytes)
		c.Buffering.MaxBufferSize = &maxBufferSize
		bufferedSink.mu.Unlock()
	}
	return c
}

// TestingResetActive clears the active bit. This is for use in tests
// that use stderr redirection alongside other tests that use
// logging.
func TestingResetActive() {
	logging.mu.Lock()
	defer logging.mu.Unlock()
	logging.mu.active = false
}

// DescribeAppliedConfig describes the current setup as effected by
// ApplyConfig(). This is useful in tests and also to check
// when something may be wrong with the logging configuration.
func DescribeAppliedConfig() string {
	var config logconfig.Config
	// Describe the fd2 capture, if installed.
	if logging.testingFd2CaptureLogger != nil {
		config.CaptureFd2.Enable = true
		fs := logging.testingFd2CaptureLogger.sinkInfos[0].sink.(*fileSink)
		fs.mu.Lock()
		dir := fs.mu.logDir
		fs.mu.Unlock()
		config.CaptureFd2.Dir = &dir
		m := logconfig.ByteSize(fs.logFilesCombinedMaxSize)
		config.CaptureFd2.MaxGroupSize = &m
	}

	// Describe the stderr sink.
	config.Sinks.Stderr.NoColor = logging.stderrSink.noColor.Get()
	config.Sinks.Stderr.CommonSinkConfig = logging.stderrSinkInfoTemplate.describeAppliedConfig()

	describeConnections := func(l *loggerT, ch Channel,
		target *sinkInfo, filters *logconfig.ChannelFilters) {
		for _, s := range l.sinkInfos {
			if s == target {
				sev := s.threshold.get(ch)
				filters.AddChannel(ch, sev)
			}
		}
		_ = filters.Validate(logpb.Severity_UNKNOWN)
	}

	// Describe the connections to the stderr sink.
	logging.rmu.RLock()
	chans := logging.rmu.channels
	stderrSinkInfo := logging.rmu.currentStderrSinkInfo
	logging.rmu.RUnlock()
	for ch, logger := range chans {
		describeConnections(logger, ch,
			stderrSinkInfo, &config.Sinks.Stderr.Channels)
	}

	// Describe the file sinks.
	config.Sinks.FileGroups = make(map[string]*logconfig.FileSinkConfig)
	_ = logging.allSinkInfos.iter(func(l *sinkInfo) error {
		if cl := logging.testingFd2CaptureLogger; cl != nil && cl.sinkInfos[0] == l {
			// Not a real sink. Omit.
			return nil
		}
		fileSink, ok := l.sink.(*fileSink)
		if !ok {
			return nil
		}

		fc := &logconfig.FileSinkConfig{}
		fc.CommonSinkConfig = l.describeAppliedConfig()
		mf := logconfig.ByteSize(fileSink.logFileMaxSize)
		fc.MaxFileSize = &mf
		mg := logconfig.ByteSize(fileSink.logFilesCombinedMaxSize)
		fc.MaxGroupSize = &mg
		fileSink.mu.Lock()
		dir := fileSink.mu.logDir
		fileSink.mu.Unlock()
		fc.Dir = &dir
		fc.BufferedWrites = &fileSink.bufferedWrites

		// Describe the connections to this file sink.
		for ch, logger := range chans {
			describeConnections(logger, ch, l, &fc.Channels)
		}

		prefix := fileSink.groupName
		if prefix == "" {
			prefix = "default"
		}
		if prev, ok := config.Sinks.FileGroups[prefix]; ok {
			fmt.Fprintf(OrigStderr,
				"warning: multiple file loggers with prefix %q, previous: %+v\n",
				prefix, prev)
		}
		config.Sinks.FileGroups[prefix] = fc
		return nil
	})

	// Describe the fluent sinks.
	config.Sinks.FluentServers = make(map[string]*logconfig.FluentSinkConfig)
	sIdx := 1
	_ = logging.allSinkInfos.iter(func(l *sinkInfo) error {
		flSink, ok := l.sink.(*fluentSink)
		if !ok {
			// Check to see if it's a fluentSink wrapped in a bufferedSink.
			bufferedSink, ok := l.sink.(*bufferedSink)
			if !ok {
				return nil
			}
			flSink, ok = bufferedSink.child.(*fluentSink)
			if !ok {
				return nil
			}
		}

		fc := &logconfig.FluentSinkConfig{}
		fc.CommonSinkConfig = l.describeAppliedConfig()
		fc.Net = flSink.network
		fc.Address = flSink.addr

		// Describe the connections to this fluent sink.
		for ch, logger := range chans {
			describeConnections(logger, ch, l, &fc.Channels)
		}
		skey := fmt.Sprintf("s%d", sIdx)
		sIdx++
		config.Sinks.FluentServers[skey] = fc
		return nil
	})

	// Describe the http sinks.
	config.Sinks.HTTPServers = make(map[string]*logconfig.HTTPSinkConfig)
	sIdx = 1
	_ = logging.allSinkInfos.iter(func(l *sinkInfo) error {
		netSink, ok := l.sink.(*httpSink)
		if !ok {
			// Check to see if it's a httpSink wrapped in a bufferedSink.
			bufferedSink, ok := l.sink.(*bufferedSink)
			if !ok {
				return nil
			}
			netSink, ok = bufferedSink.child.(*httpSink)
			if !ok {
				return nil
			}
		}
		skey := fmt.Sprintf("s%d", sIdx)
		sIdx++
		config.Sinks.HTTPServers[skey] = netSink.config
		return nil
	})

	// Note: we cannot return 'config' directly, because this captures
	// certain variables from the loggers by reference and thus could be
	// invalidated by concurrent uses of ApplyConfig().
	return config.String()
}
