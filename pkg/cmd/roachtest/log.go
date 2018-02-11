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

// TODO(peter): put all of the logs for a test in a directory named by the
// test.
func newLogger(name, filename string, stdout, stderr io.Writer) (*logger, error) {
	filename = fmt.Sprintf("%s.log", fileutil.EscapeFilename(filename))
	f, err := os.Create(filepath.Join(artifacts, filename))
	if err != nil {
		return nil, err
	}
	p := []byte(name + ": ")

	return &logger{
		name:   name,
		file:   f,
		stdout: io.MultiWriter(f, &prefixWriter{out: stdout, prefix: p}),
		stderr: io.MultiWriter(f, &prefixWriter{out: stderr, prefix: p}),
	}, nil
}

func rootLogger(name string) (*logger, error) {
	return newLogger(name, name, os.Stdout, os.Stderr)
}

func stdLogger(name string) *logger {
	return &logger{
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
}

func (l *logger) close() {
	if l.file != nil {
		l.file.Close()
	}
}

func (l *logger) childLogger(name string) (*logger, error) {
	if l.file == nil {
		p := []byte(name + ": ")
		return &logger{
			name:   name,
			stdout: &prefixWriter{out: l.stdout, prefix: p},
			stderr: &prefixWriter{out: l.stderr, prefix: p},
		}, nil
	}

	filename := l.name + "_" + name
	return newLogger(name, filename, l.stdout, l.stderr)
}

func (l *logger) printf(f string, args ...interface{}) {
	fmt.Fprintf(l.stdout, f, args...)
}

func (l *logger) errorf(f string, args ...interface{}) {
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
