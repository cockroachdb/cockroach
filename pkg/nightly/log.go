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
	"strings"
)

// logger logs to a file in artifacts and stdio simultaneously. This makes it
// possible to observe progress of multiple tests from the terminal (or the
// TeamCity build log, if running in CI), while creating a non-interleaved
// record in the build artifacts.
type logger struct {
	name           string
	file           *os.File
	stdout, stderr dualLogger
}

func newLogger(name string) (*logger, error) {
	fileName := fmt.Sprintf("%s.log", strings.Replace(name, "/", "-", -1))
	f, err := os.Create(filepath.Join("artifacts", fileName))
	if err != nil {
		return nil, err
	}
	p := name + ": "
	return &logger{
		name:   name,
		file:   f,
		stdout: dualLogger{prefix: p, shared: os.Stdout, dedicated: f},
		stderr: dualLogger{prefix: p, shared: os.Stderr, dedicated: f},
	}, nil
}

func (l *logger) Printf(f string, args ...interface{}) (int, error) {
	return fmt.Fprintf(&l.stdout, f, args...)
}

func (l *logger) Errorf(f string, args ...interface{}) (int, error) {
	return fmt.Fprintf(&l.stderr, f, args...)
}

type dualLogger struct {
	prefix    string
	shared    io.Writer
	dedicated io.Writer
}

func (d *dualLogger) Write(data []byte) (int, error) {
	prefixed := bytes.Replace(data, []byte("\n"), []byte("\n"+d.prefix), -1)
	_, _ = d.shared.Write(prefixed)
	return d.dedicated.Write(data)
}
