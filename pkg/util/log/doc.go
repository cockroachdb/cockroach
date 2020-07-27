// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2013 Google Inc. All Rights Reserved.
// Copyright 2015 Cockroach Labs.
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

// This code originated in the github.com/golang/glog package.

// Package log implements logging.
// There are three logging styles: named, V-style, events.
//
// Named Functions
//
// The functions Info, Warning, Error, and Fatal log their arguments at the
// severity level. All include formatting variants like Infof.
//
// Examples:
//
//	log.Dev.Info(ctx, "prepare to repel boarders")
//	log.Ops.Info(ctx, "prepare to repel boarders")
//	log.Dev.Fatal(ctx, "initialization failed", err)
//	log.Dev.Infof(ctx, "client error: %s", err)
//
// In these examples, the first word "Dev", "Ops" is the logging
// channel. Different channels are aimed at different audiences and
// can be redirected to different sinks.
//
// If the channel name is omitted (e.g. log.Info), the Dev channel is
// used.
//
// The second word e.g. "Info", "Fatal" etc is the severity level.
// For any given channel, users can filter out logging to a given
// severity level or higher.
//
// V-Style
//
// The V functions can be used to selectively enable logging at a call
// site. Invoking the binary with --vmodule=*=N will enable V functions
// at level N or higher. Invoking the binary with --vmodule="glob=N" will
// enable V functions at level N or higher with a filename matching glob.
//
// Examples:
//
//	if log.V(2) {
//		log.Info(ctx, "starting transaction...")
//	}
//
// Additionally, severity functions also exist in a V variant for
// convenience. For example:
//
//  log.Ops.VWarningf(ctx, 2, "attention!")
//
// aliases: if V(2) { log.Ops.Warningf(ctx, "attention!") }
//
// Events
//
// The Event functions log messages to an existing trace if one exists. The
// VEvent functions logs the message to a trace and also the Dev channel based
// on the V level.
//
// Examples:
//
//	log.VEventf(ctx, 2, "client error; %s", err)
//
// Output
//
// Log output is buffered and written periodically using Flush. Programs
// should call Flush before exiting to guarantee all log output is written.
//
// By default, all log statements write to files in a temporary directory.
// This package provides several flags that modify this behavior.
// These are provided via the util/log/logflags package; see InitFlags.
//
//  --logtostderr=LEVEL
//    Logs are written to standard error as well as to files.
//    Entries with severity below LEVEL are not written to stderr.
//    "true" and "false" are also supported (everything / nothing).
//  --log-dir="..."
//    Log files will be written to this directory by the main logger
//    instead of the default target directory.
//  --log-file-verbosity=LEVEL
//    Entries with severity below LEVEL are not written to the log file.
//    "true" and "false" are also supported (everything / nothing).
//  --log-file-max-size=N
//    Log files are rotated after reaching that size.
//  --log-group-max-size=N
//    Log files are removed after the total size of all files generated
//    by one logger reaches that size.
//
// Other flags provide aids to debugging.
//
//  --vmodule=""
//    The syntax of the argument is a comma-separated list of pattern=N,
//    where pattern is a literal file name (minus the ".go" suffix) or
//    "glob" pattern and N is a V level. For instance,
//      --vmodule=gopher*=3
//    sets the V level to 3 in all Go files whose names begin "gopher".
//
package log
