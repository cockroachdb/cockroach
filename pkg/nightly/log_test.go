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

package nightly

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
)

// logger logs to a file in artifacts and stdio simultaneously. This makes it
// possible to observe progress of multiple tests from the terminal (or the
// TeamCity build log, if running in CI), while creating a non-interleaved
// record in the build artifacts.
type logger struct {
	name           string
	file           *os.File
	stdout, stderr io.Writer
}

func newLogger(name, filename string, stdout, stderr io.Writer) (*logger, error) {
	filename = fmt.Sprintf("%s.log", fileutil.EscapeFilename(filename))
	f, err := os.Create(filepath.Join(*artifacts, filename))
	if err != nil {
		return nil, err
	}
	p := name + ": "
	return &logger{
		name:   name,
		file:   f,
		stdout: &dualLogger{prefix: p, shared: stdout, dedicated: f},
		stderr: &dualLogger{prefix: p, shared: stderr, dedicated: f},
	}, nil
}

func rootLogger(name string) (*logger, error) {
	return newLogger(name, name, os.Stdout, os.Stderr)
}

func stdLogger(name string) *logger {
	return &logger{
		name:   "std",
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
}

func (l *logger) childLogger(name string) (*logger, error) {
	filename := l.name + "_" + name
	return newLogger(name, filename, l.stdout, l.stderr)
}

func (l *logger) printf(f string, args ...interface{}) {
	fmt.Fprintf(l.stdout, f, args...)
}

func (l *logger) errorf(f string, args ...interface{}) {
	fmt.Fprintf(l.stderr, f, args...)
}

type dualLogger struct {
	prefix    string
	shared    io.Writer
	dedicated io.Writer
}

func (d *dualLogger) Write(data []byte) (int, error) {
	// Transform data like "a\nb" into "PREFIX: a\nPREFIX b\n" before writing it
	// to the shared channel. We always
	prefixed := []byte(d.prefix)
	prefixed = append(prefixed, data...)
	prefixed = bytes.TrimSuffix(prefixed, []byte{'\n'})
	prefixed = bytes.Replace(prefixed, []byte{'\n'}, []byte("\n"+d.prefix), -1)
	prefixed = append(prefixed, '\n')
	_, _ = d.shared.Write(prefixed)
	return d.dedicated.Write(data)
}
