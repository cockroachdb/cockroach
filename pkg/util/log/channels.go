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
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
)

//go:generate ./gen.sh

type channelSink struct {
	logger *secondaryLogger
}

// Channel aliases a type.
type Channel = logpb.Channel

var channels = map[Channel]channelSink{}

// logf creates a structured log entry to be written to the
// specified facility of the logger.
func logf(ctx context.Context, sev Severity, ch Channel, format string, args ...interface{}) {
	logfDepth(ctx, 1, sev, ch, format, args...)
}

// log creates a structured log entry to be written to the
// specified facility of the logger.
func log(ctx context.Context, sev Severity, ch Channel, msg string) {
	logfDepth(ctx, 1, sev, ch, msg)
}

// LogfDepth is like Logf but the caller can specified the
// call stack depth at which to capture caller information.
func logfDepth(
	ctx context.Context, depth int, sev Severity, ch Channel, format string, args ...interface{},
) {
	if sev == severity.FATAL {
		// We load the ReportingSettings from the a global singleton in this
		// call path. See the singleton's comment for a rationale.
		if sv := settings.TODO(); sv != nil {
			err := errors.NewWithDepthf(depth+1, "log.Fatal: "+format, args...)
			sendCrashReport(ctx, sv, err, ReportTypePanic)
		}
	}

	logger := getLogger(ch)
	entry := MakeEntry(
		ctx, sev, ch,
		&logger.logCounter, depth+1, logger.redactableLogs.Get(), format, args...)
	if sp, el, ok := getSpanOrEventLog(ctx); ok {
		eventInternal(sp, el, (sev >= severity.ERROR), entry)
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
			os.Exit(1)
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

func getLogger(ch Channel) *loggerT {
	if sink := channels[ch]; sink.logger != nil {
		return &sink.logger.logger
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
	return s >= logging.stderrThreshold
}
