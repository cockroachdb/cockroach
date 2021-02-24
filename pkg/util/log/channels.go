// Copyright 2020 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
)

//go:generate go run gen/main.go logpb/log.proto logging.md ../../../docs/generated/logging.md
//go:generate go run gen/main.go logpb/log.proto severity.go severity/severity_generated.go
//go:generate go run gen/main.go logpb/log.proto channel.go channel/channel_generated.go
//go:generate go run gen/main.go logpb/log.proto log_channels.go log_channels_generated.go

// Channel aliases a type.
type Channel = logpb.Channel

// logfDepth emits a log entry on the specified channel at specified
// severity.
func logfDepth(
	ctx context.Context, depth int, sev Severity, ch Channel, format string, args ...interface{},
) {
	if sev == severity.FATAL {
		if MaybeSendCrashReport != nil {
			err := errors.NewWithDepthf(depth+1, "log.Fatal: "+format, args...)
			MaybeSendCrashReport(ctx, err)
		}
		if ch != channel.OPS {
			// Tell the OPS channel about this termination.
			logfDepth(ctx, depth+1, severity.INFO, channel.OPS,
				"the server is terminating due to a fatal error (see the %s channel for details)", ch)
		}
	}

	logger := logging.getLogger(ch)
	entry := makeUnstructuredEntry(
		ctx, sev, ch,
		depth+1, true /* redactable */, format, args...)
	if sp, el, ok := getSpanOrEventLog(ctx); ok {
		eventInternal(sp, el, (sev >= severity.ERROR), entry.convertToLegacy())
	}
	logger.outputLogEntry(entry)
}

// shoutfDepth shouts to the specified channel.
func shoutfDepth(
	ctx context.Context, depth int, sev Severity, ch Channel, format string, args ...interface{},
) {
	if sev == severity.FATAL {
		// Fatal error handling later already tries to exit even if I/O should
		// block, but crash reporting might also be in the way.
		t := time.AfterFunc(10*time.Second, func() {
			exit.WithCode(exit.TimeoutAfterFatalError())
		})
		defer t.Stop()
	}
	if !LoggingToStderr(sev) {
		// The logging call below would not otherwise appear on stderr;
		// however this is what the Shout() contract guarantees, so we do
		// it here.
		fmt.Fprintf(OrigStderr, "*\n* %s: %s\n*\n", sev.String(),
			strings.Replace(
				FormatWithContextTags(ctx, format, args...),
				"\n", "\n* ", -1))
	}
	logfDepth(ctx, depth+1, sev, ch, format, args...)
}

func (l *loggingT) setChannelLoggers(m map[Channel]*loggerT, stderrSinkInfo *sinkInfo) {
	l.rmu.Lock()
	defer l.rmu.Unlock()
	l.rmu.currentStderrSinkInfo = stderrSinkInfo
	l.rmu.channels = m
}

func (l *loggingT) getLogger(ch Channel) *loggerT {
	l.rmu.RLock()
	defer l.rmu.RUnlock()
	if l := l.rmu.channels[ch]; l != nil {
		return l
	}
	return debugLog
}

// LoggingToStderr returns true if log messages of the given severity
// sent to the channel's logger are also visible on the process'
// external stderr. This is used e.g. by the startup code to announce
// server details both on the external stderr and to the log file.
//
// This is also the logic used by Shout calls.
func LoggingToStderr(s Severity) bool {
	return s >= logging.stderrSinkInfoTemplate.threshold.Get()
}

// MaybeSendCrashReport is injected by package logcrash
var MaybeSendCrashReport func(ctx context.Context, err error)
