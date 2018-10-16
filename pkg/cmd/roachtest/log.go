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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type loggerConfig struct {
	// prefix, if set, is applied to lines written to stderr/stdout. It is not
	// applied to lines written to a log file.
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
	path           string
	file           *os.File
	stdout, stderr io.Writer
}

// newLogger constructs a new logger object. Not intended for direct
// use. Please use logger.ChildLogger instead.
//
// If path is empty, logs will go to stdout/stderr.
func (cfg *loggerConfig) newLogger(path string) (*logger, error) {
	if path == "" {
		// Log to stdout/stderr if there is no artifacts directory.
		return &logger{
			stdout: os.Stdout,
			stderr: os.Stderr,
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
		if cfg.prefix != "" {
			w = &prefixWriter{out: w, prefix: []byte(cfg.prefix)}
		}
		return io.MultiWriter(f, w)
	}

	return &logger{
		path:   path,
		file:   f,
		stdout: newWriter(cfg.stdout),
		stderr: newWriter(cfg.stderr),
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
		p := []byte(name + ": ")
		return &logger{
			path:   l.path,
			stdout: &prefixWriter{out: l.stdout, prefix: p},
			stderr: &prefixWriter{out: l.stderr, prefix: p},
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
	fmt.Fprintf(l.stdout, f, args...)
}

func (l *logger) Errorf(f string, args ...interface{}) {
	fmt.Fprintf(l.stderr, f, args...)
}

type prefixWriter struct {
	out    io.Writer
	prefix []byte
	buf    []byte
}

func (w *prefixWriter) Write(data []byte) (int, error) {
	// Note that we only output data to the underlying writer when we see a
	// newline. No newline and the data is buffered. We don't have a signal for
	// when the end of data is reached, which means we won't output any trailing
	// data if it isn't terminated with a newline.
	var count int
	for len(data) > 0 {
		if len(w.buf) == 0 {
			w.buf = append(w.buf, w.prefix...)
		}

		i := bytes.IndexByte(data, '\n')
		if i == -1 {
			// No newline, buffer the partial line.
			w.buf = append(w.buf, data...)
			count += len(data)
			break
		}

		// Output the buffered line including prefix.
		w.buf = append(w.buf, data[:i+1]...)
		if _, err := w.out.Write(w.buf); err != nil {
			return 0, err
		}
		w.buf = w.buf[:0]
		data = data[i+1:]
		count += i + 1
	}
	return count, nil
}
