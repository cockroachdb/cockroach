// Go support for leveled logs, analogous to https://code.google.com/p/google-glog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Original version (c) Google.
// Author (fork from https://github.com/golang/glog): Tobias Schottdorf

// Package log implements logging analogous to the Google-internal C++ INFO/ERROR/V setup.
// It provides functions Info, Warning, Error, Fatal, plus formatting variants such as
// Infof. It also provides V-style logging and adaptions for use as a structured
// logging engine.
//
// Basic examples:
//
//	log.Info("Prepare to repel boarders")
//
//	log.Fatal("Initialization failed", err)
//
//  // Log with context.
//  log.Infoc(context, "client error: %s", err)
//
// See the documentation for the V function for an explanation of these examples:
//
//	if log.V(2) {
//		log.Info("Starting transaction...")
//	}
//
// Log output is buffered and written periodically using Flush. Programs
// should call Flush before exiting to guarantee all log output is written.
//
// By default, all log statements write to files in a temporary directory.
// This package provides several flags that modify this behavior.
// These are provided via the pflags library; see InitFlags.
//
//	--logtostderr=true
//		Logs are written to standard error instead of to files.
//	--alsologtostderr=INFO
//		Logs are written to standard error as well as to files.
//	--log-dir=""
//		Log files will be written to this directory instead of the
//		default temporary directory.
//
//	Other flags provide aids to debugging.
//
//	--log-backtrace-at=""
//		When set to a file and line number holding a logging statement,
//		such as
//			-log_backtrace_at=gopherflakes.go:234
//		a stack trace will be written to the Info log whenever execution
//		hits that statement. (Unlike with --vmodule, the ".go" must be
//		present.)
//	--verbosity=0
//		Enable V-leveled logging at the specified level.
//	--vmodule=""
//		The syntax of the argument is a comma-separated list of pattern=N,
//		where pattern is a literal file name (minus the ".go" suffix) or
//		"glob" pattern and N is a V level. For instance,
//			-vmodule=gopher*=3
//		sets the V level to 3 in all Go files whose names begin "gopher".
//
package log
