// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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
	stdoutL := log.New(cfg.stdout, cfg.prefix, logFlags)
	var stderrL *log.Logger
	if cfg.stdout != cfg.stderr {
		stderrL = log.New(cfg.stderr, cfg.prefix, logFlags)
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

func (l *logger) close() {
	if l.file != nil {
		l.file.Close()
	}
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

func (l *logger) Printf(f string, args ...interface{}) {
	if err := l.stdoutL.Output(2 /* calldepth */, fmt.Sprintf(f, args...)); err != nil {
		// Changing our interface to return an Error from a logging method seems too
		// onerous. Let's just crash.
		panic(err)
	}
}

func (l *logger) Errorf(f string, args ...interface{}) {
	if err := l.stderrL.Output(2 /* calldepth */, fmt.Sprintf(f, args...)); err != nil {
		// Changing our interface to return an Error from a logging method seems too
		// onerous. Let's just crash.
		panic(err)
	}
}
