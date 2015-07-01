// Copyright 2015 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf

package fakeflags

import "github.com/cockroachdb/cockroach/util/log/logflags"

type fakeValue struct{}

func (*fakeValue) String() string     { return "" }
func (*fakeValue) Set(_ string) error { return nil }

// InitLogFlags sets fake flags which mimick those of the logging package.
// This is necessary since some packages are not allowed to import `util/log`,
// but still need to be able to run tests with logging flags.
func InitLogFlags() {
	var toStderr, alsoToStderr bool
	var color, logDir string
	var verbosity, vmodule, traceLocation fakeValue
	logflags.InitFlags(&toStderr, &alsoToStderr, &logDir,
		&color, &verbosity, &vmodule, &traceLocation)
}
