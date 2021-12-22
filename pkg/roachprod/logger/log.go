// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logger

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

// Config configures a logger.
type Config struct {
	// Prefix, if set, will be applied to lines passed to Printf()/Errorf().
	// It will not be applied to lines written directly to stdout/Stderr (not for
	// a particular reason, but because it's not worth the extra code to do so).
	Prefix         string
	Stdout, Stderr io.Writer
}

type loggerOption interface {
	apply(*Config)
}

type logPrefix string

var _ logPrefix // silence unused lint

func (p logPrefix) apply(cfg *Config) {
	cfg.Prefix = string(p)
}

type quietStdoutOption struct {
}

func (quietStdoutOption) apply(cfg *Config) {
	cfg.Stdout = ioutil.Discard
}

type quietStderrOption struct {
}

func (quietStderrOption) apply(cfg *Config) {
	cfg.Stderr = ioutil.Discard
}

// QuietStdout is a logger option that suppresses Stdout.
var QuietStdout quietStdoutOption

// QuietStderr is a logger option that suppresses Stderr.
var QuietStderr quietStderrOption

// Logger logs to a file in artifacts and stdio simultaneously. This makes it
// possible to observe progress of multiple tests from the terminal (or the
// TeamCity build log, if running in CI), while creating a non-interleaved
// record in the build artifacts.
type Logger struct {
	path string
	File *os.File
	// stdoutL and stderrL are the loggers used internally by Printf()/Errorf().
	// They write to Stdout/Stderr (below), but prefix the messages with
	// Logger-specific formatting (file/line, time), plus an optional configurable
	// prefix.
	// stderrL is nil in case Stdout == Stderr. In that case, stdoutL is always
	// used. We do this in order to take advantage of the Logger's internal
	// synchronization so that concurrent Printf()/Error() calls don't step over
	// each other.
	stdoutL, stderrL *log.Logger
	// Stdout, Stderr are the raw Writers used by the loggers.
	// If path/file is set, then they might Multiwriters, outputting to both a
	// file and os.Stdout.
	// They can be used directly by clients when a writer is required (e.g. when
	// piping output from a subcommand).
	Stdout, Stderr io.Writer

	mu struct {
		syncutil.Mutex
		closed bool
	}
}

// NewLogger constructs a new Logger object. Not intended for direct
// use. Please use Logger.ChildLogger instead.
//
// If path is empty, logs will go to stdout/Stderr.
func (cfg *Config) NewLogger(path string) (*Logger, error) {
	if path == "" {
		// Log to os.Stdout/Stderr is no other options are passed in.
		stdout := cfg.Stdout
		if stdout == nil {
			stdout = os.Stdout
		}
		stderr := cfg.Stderr
		if stderr == nil {
			stderr = os.Stderr
		}
		return &Logger{
			Stdout:  stdout,
			Stderr:  stderr,
			stdoutL: log.New(os.Stdout, cfg.Prefix, logFlags),
			stderrL: log.New(os.Stderr, cfg.Prefix, logFlags),
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

	stdout := newWriter(cfg.Stdout)
	stderr := newWriter(cfg.Stderr)
	stdoutL := log.New(stdout, cfg.Prefix, logFlags)
	var stderrL *log.Logger
	if cfg.Stdout != cfg.Stderr {
		stderrL = log.New(stderr, cfg.Prefix, logFlags)
	} else {
		stderrL = stdoutL
	}
	return &Logger{
		path:    path,
		File:    f,
		Stdout:  stdout,
		Stderr:  stderr,
		stdoutL: stdoutL,
		stderrL: stderrL,
	}, nil
}

// TeeOptType is an option to RootLogger.
type TeeOptType bool

const (
	// TeeToStdout configures the logger to report both to a file and stdout.
	TeeToStdout TeeOptType = true
	// NoTee configures the logger to only output to files.
	NoTee TeeOptType = false
)

// RootLogger creates a Logger.
//
// If path is empty, all logs go to Stdout/Stderr regardless of teeOpt.
func RootLogger(path string, teeOpt TeeOptType) (*Logger, error) {
	var stdout, stderr io.Writer
	if teeOpt == TeeToStdout {
		stdout = os.Stdout
		stderr = os.Stderr
	}
	cfg := &Config{Stdout: stdout, Stderr: stderr}
	return cfg.NewLogger(path)
}

// Close closes the Logger. It is idempotent.
func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mu.closed {
		return
	}
	l.mu.closed = true
	if l.File != nil {
		l.File.Close()
		l.File = nil
	}
}

// Closed returns true if Close() was previously called.
func (l *Logger) Closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.closed
}

// ChildLogger constructs a new Logger which logs to the specified file. The
// prefix and teeing of Stdout/Stdout can be controlled by Logger options.
// If the parent Logger was logging to a file, the new Logger will log to a file
// in the same dir called <name>.log.
func (l *Logger) ChildLogger(name string, opts ...loggerOption) (*Logger, error) {
	// If the parent Logger is not logging to a file, then the child will not
	// either. However, the child will write to Stdout/Stderr with a prefix.
	if l.File == nil {
		p := name + ": "

		stdoutL := log.New(l.Stdout, p, logFlags)
		var stderrL *log.Logger
		if l.Stdout != l.Stderr {
			stderrL = log.New(l.Stderr, p, logFlags)
		} else {
			stderrL = stdoutL
		}
		return &Logger{
			path:    l.path,
			Stdout:  l.Stdout,
			Stderr:  l.Stderr,
			stdoutL: stdoutL,
			stderrL: stderrL,
		}, nil
	}

	cfg := &Config{
		Prefix: name + ": ", // might be overridden by opts
		Stdout: l.Stdout,
		Stderr: l.Stderr,
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	var path string
	if l.path != "" {
		path = filepath.Join(filepath.Dir(l.path), name+".log")
	}
	return cfg.NewLogger(path)
}

// PrintfCtx prints a message to the Logger's Stdout. The context's log tags, if
// any, will be prepended to the message. A newline is appended if the last
// character is not already a newline.
func (l *Logger) PrintfCtx(ctx context.Context, f string, args ...interface{}) {
	l.PrintfCtxDepth(ctx, 2 /* depth */, f, args...)
}

// Printf is like PrintfCtx, except it doesn't take a ctx and thus no log tags
// can be passed.
func (l *Logger) Printf(f string, args ...interface{}) {
	l.PrintfCtxDepth(context.Background(), 2 /* depth */, f, args...)
}

// PrintfCtxDepth is like PrintfCtx, except that it allows the caller to control
// which stack frame is reported as the file:line in the message. depth=1 is
// equivalent to PrintfCtx. E.g. pass 2 to ignore the caller's frame.
func (l *Logger) PrintfCtxDepth(ctx context.Context, depth int, f string, args ...interface{}) {
	msg := crdblog.FormatWithContextTags(ctx, f, args...)
	if err := l.stdoutL.Output(depth+1, msg); err != nil {
		// Changing our interface to return an Error from a logging method seems too
		// onerous. Let's yell to the default Logger and if that fails, oh well.
		_ = log.Output(depth+1, fmt.Sprintf("failed to log message: %v: %s", err, msg))
	}
}

// ErrorfCtx is like PrintfCtx, except the Logger outputs to its Stderr.
func (l *Logger) ErrorfCtx(ctx context.Context, f string, args ...interface{}) {
	l.ErrorfCtxDepth(ctx, 2 /* depth */, f, args...)
}

// ErrorfCtxDepth is like PrintfCtxDepth but to Stderr.
func (l *Logger) ErrorfCtxDepth(ctx context.Context, depth int, f string, args ...interface{}) {
	msg := crdblog.FormatWithContextTags(ctx, f, args...)
	if err := l.stderrL.Output(depth+1, msg); err != nil {
		// Changing our interface to return an Error from a logging method seems too
		// onerous. Let's yell to the default Logger and if that fails, oh well.
		_ = log.Output(depth+1, fmt.Sprintf("failed to log error: %v: %s", err, msg))
	}
}

// Errorf is like ErrorfCtx, except it doesn't take a ctx and thus no log tags
// can be passed.
func (l *Logger) Errorf(f string, args ...interface{}) {
	l.ErrorfCtxDepth(context.Background(), 2 /* depth */, f, args...)
}
