// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	crdblog "github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// The flags used by the internal loggers.
const logFlags = log.Lshortfile | log.Ltime | log.LUTC

type loggerConfig struct {
	// prefix, if set, will be applied to lines passed to Printf()/Errorf().
	// It will not be applied to lines written directly to stdout/stderr (not for
	// a particular reason, but because it's not worth the extra code to do so).
	prefix         string
	stdout, stderr io.Writer
}

type loggerOption interface {
	apply(*loggerConfig)
}

type logPrefix string

var _ logPrefix // silence unused lint

func (p logPrefix) apply(cfg *loggerConfig) {
	cfg.prefix = string(p)
}

type quietStdoutOption struct {
}

func (quietStdoutOption) apply(cfg *loggerConfig) {
	cfg.stdout = ioutil.Discard
}

type quietStderrOption struct {
}

func (quietStderrOption) apply(cfg *loggerConfig) {
	cfg.stderr = ioutil.Discard
}

var quietStdout quietStdoutOption
var quietStderr quietStderrOption

// logger logs to a file in artifacts and stdio simultaneously. This makes it
// possible to observe progress of multiple tests from the terminal (or the
// TeamCity build log, if running in CI), while creating a non-interleaved
// record in the build artifacts.
type logger struct {
	path string
	file *os.File
	// stdoutL and stderrL are the loggers used internally by Printf()/Errorf().
	// They write to stdout/stderr (below), but prefix the messages with
	// logger-specific formatting (file/line, time), plus an optional configurable
	// prefix.
	// stderrL is nil in case stdout == stderr. In that case, stdoutL is always
	// used. We do this in order to take advantage of the logger's internal
	// synchronization so that concurrent Printf()/Error() calls don't step over
	// each other.
	stdoutL, stderrL *log.Logger
	// stdout, stderr are the raw Writers used by the loggers.
	// If path/file is set, then they might Multiwriters, outputting to both a
	// file and os.Stdout.
	// They can be used directly by clients when a writer is required (e.g. when
	// piping output from a subcommand).
	stdout, stderr io.Writer

	mu struct {
		syncutil.Mutex
		closed bool
	}
}

// newLogger constructs a new logger object. Not intended for direct
// use. Please use logger.ChildLogger instead.
//
// If path is empty, logs will go to stdout/stderr.
func (cfg *loggerConfig) newLogger(path string) (*logger, error) {
	if path == "" {
		// Log to os.Stdout/Stderr is no other options are passed in.
		stdout := cfg.stdout
		if stdout == nil {
			stdout = os.Stdout
		}
		stderr := cfg.stderr
		if stderr == nil {
			stderr = os.Stderr
		}
		return &logger{
			stdout:  stdout,
			stderr:  stderr,
			stdoutL: log.New(os.Stdout, cfg.prefix, logFlags),
			stderrL: log.New(os.Stderr, cfg.prefix, logFlags),
		}, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	newWriter := func(w io.Writer) io.Writer {
		if w == nil {
			return f
		}
		return io.MultiWriter(f, w)
	}

	stdout := newWriter(cfg.stdout)
	stderr := newWriter(cfg.stderr)
	stdoutL := log.New(stdout, cfg.prefix, logFlags)
	var stderrL *log.Logger
	if cfg.stdout != cfg.stderr {
		stderrL = log.New(stderr, cfg.prefix, logFlags)
	} else {
		stderrL = stdoutL
	}
	return &logger{
		path:    path,
		file:    f,
		stdout:  stdout,
		stderr:  stderr,
		stdoutL: stdoutL,
		stderrL: stderrL,
	}, nil
}

type teeOptType bool

const (
	teeToStdout teeOptType = true
	noTee       teeOptType = false
)

// rootLogger creates a logger.
//
// If path is empty, all logs go to stdout/stderr regardless of teeOpt.
func rootLogger(path string, teeOpt teeOptType) (*logger, error) {
	var stdout, stderr io.Writer
	if teeOpt == teeToStdout {
		stdout = os.Stdout
		stderr = os.Stderr
	}
	cfg := &loggerConfig{stdout: stdout, stderr: stderr}
	return cfg.newLogger(path)
}

// close closes the logger. It is idempotent.
func (l *logger) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.closed {
		return
	}
	l.mu.closed = true
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
}

// closed returns true if close() was previously called.
func (l *logger) closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.closed
}

// ChildLogger constructs a new logger which logs to the specified file. The
// prefix and teeing of stdout/stdout can be controlled by logger options.
// If the parent logger was logging to a file, the new logger will log to a file
// in the same dir called <name>.log.
func (l *logger) ChildLogger(name string, opts ...loggerOption) (*logger, error) {
	// If the parent logger is not logging to a file, then the child will not
	// either. However, the child will write to stdout/stderr with a prefix.
	if l.file == nil {
		p := name + ": "

		stdoutL := log.New(l.stdout, p, logFlags)
		var stderrL *log.Logger
		if l.stdout != l.stderr {
			stderrL = log.New(l.stderr, p, logFlags)
		} else {
			stderrL = stdoutL
		}
		return &logger{
			path:    l.path,
			stdout:  l.stdout,
			stderr:  l.stderr,
			stdoutL: stdoutL,
			stderrL: stderrL,
		}, nil
	}

	cfg := &loggerConfig{
		prefix: name + ": ", // might be overridden by opts
		stdout: l.stdout,
		stderr: l.stderr,
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	var path string
	if l.path != "" {
		path = filepath.Join(filepath.Dir(l.path), name+".log")
	}
	return cfg.newLogger(path)
}

// PrintfCtx prints a message to the logger's stdout. The context's log tags, if
// any, will be prepended to the message. A newline is appended if the last
// character is not already a newline.
func (l *logger) PrintfCtx(ctx context.Context, f string, args ...interface{}) {
	l.PrintfCtxDepth(ctx, 2 /* depth */, f, args...)
}

// Printf is like PrintfCtx, except it doesn't take a ctx and thus no log tags
// can be passed.
func (l *logger) Printf(f string, args ...interface{}) {
	l.PrintfCtxDepth(context.Background(), 2 /* depth */, f, args...)
}

// PrintfCtxDepth is like PrintfCtx, except that it allows the caller to control
// which stack frame is reported as the file:line in the message. depth=1 is
// equivalent to PrintfCtx. E.g. pass 2 to ignore the caller's frame.
func (l *logger) PrintfCtxDepth(ctx context.Context, depth int, f string, args ...interface{}) {
	msg := crdblog.FormatWithContextTags(ctx, f, args...)
	if err := l.stdoutL.Output(depth+1, msg); err != nil {
		// Changing our interface to return an Error from a logging method seems too
		// onerous. Let's yell to the default logger and if that fails, oh well.
		_ = log.Output(depth+1, fmt.Sprintf("failed to log message: %v: %s", err, msg))
	}
}

// ErrorfCtx is like PrintfCtx, except the logger outputs to its stderr.
func (l *logger) ErrorfCtx(ctx context.Context, f string, args ...interface{}) {
	l.ErrorfCtxDepth(ctx, 2 /* depth */, f, args...)
}

func (l *logger) ErrorfCtxDepth(ctx context.Context, depth int, f string, args ...interface{}) {
	msg := crdblog.FormatWithContextTags(ctx, f, args...)
	if err := l.stderrL.Output(depth+1, msg); err != nil {
		// Changing our interface to return an Error from a logging method seems too
		// onerous. Let's yell to the default logger and if that fails, oh well.
		_ = log.Output(depth+1, fmt.Sprintf("failed to log error: %v: %s", err, msg))
	}
}

// Errorf is like ErrorfCtx, except it doesn't take a ctx and thus no log tags
// can be passed.
func (l *logger) Errorf(f string, args ...interface{}) {
	l.ErrorfCtxDepth(context.Background(), 2 /* depth */, f, args...)
}
