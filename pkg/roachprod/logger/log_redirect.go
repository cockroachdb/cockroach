// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type logRedirect struct {
	syncutil.Mutex
	logger          *Logger
	cancelIntercept func()
	configured      bool
}

var logRedirectInst = &logRedirect{}

// InitCRDBLogConfig sets up an interceptor for the CockroachDB log in order to
// redirect logs to a roachprod logger. This is necessary as the CockroachDB log
// is used in some test utilities shared between roachtest and CockroachDB.
// Generally, CockroachDB logs should not be used explicitly in roachtest.
func InitCRDBLogConfig(logger *Logger) {
	logRedirectInst.Lock()
	defer logRedirectInst.Unlock()
	if logRedirectInst.configured {
		panic("internal error: CRDB log interceptor already configured")
	}
	if logger == nil {
		panic("internal error: specified logger is nil")
	}
	logConf := logconfig.DefaultStderrConfig()
	logConf.Sinks.Stderr.Filter = logpb.Severity_FATAL
	// Disable logging to a file. File sinks write the application arguments to
	// the log by default (see: log_entry.go), and it is best to avoid logging
	// the roachtest arguments as it may contain sensitive information.
	if err := logConf.Validate(nil); err != nil {
		panic(fmt.Errorf("internal error: could not validate CRDB log config: %w", err))
	}
	if _, err := log.ApplyConfig(logConf, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */); err != nil {
		panic(fmt.Errorf("internal error: could not apply CRDB log config: %w", err))
	}
	logRedirectInst.logger = logger
	logRedirectInst.cancelIntercept = log.InterceptWith(context.Background(), logRedirectInst)
	logRedirectInst.configured = true
}

// TestingCRDBLogConfig is meant to be used in unit tests to reset the CRDB log,
// it's interceptor, and the redirect log config. This is necessary to avoid
// leaking state between tests, that test the logging.
func TestingCRDBLogConfig(logger *Logger) {
	logRedirectInst.Lock()
	defer logRedirectInst.Unlock()
	if logRedirectInst.cancelIntercept != nil {
		logRedirectInst.cancelIntercept()
	}
	log.TestingResetActive()
	logRedirectInst = &logRedirect{}
	InitCRDBLogConfig(logger)
}

// Intercept intercepts CockroachDB log entries and redirects it to the
// appropriate roachtest test logger or stderr.
func (i *logRedirect) Intercept(logData []byte) {
	var entry logpb.Entry
	if err := json.Unmarshal(logData, &entry); err != nil {
		i.logger.Errorf("failed to unmarshal intercepted log entry: %v", err)
	}
	l := i.logger
	if l != nil && !l.Closed() {
		if entry.Severity == logpb.Severity_ERROR || entry.Severity == logpb.Severity_FATAL {
			i.writeLog(l.Stderr, entry)
			return
		}
		i.writeLog(l.Stdout, entry)
	}
}

func (i *logRedirect) writeLog(dst io.Writer, entry logpb.Entry) {
	if err := log.FormatLegacyEntry(entry, dst); err != nil {
		i.logger.Errorf("could not format and write CRDB log entry: %v", err)
	}
}
