// Copyright 2014 The Cockroach Authors.
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

package log

// TestVAndModule tests that module-level logging isn't broken.
// It is deactivated since it messes with the logging and we don't want that.
// However, it's a good sanity check while refactoring our logging.
//
// func TestVAndVModule(t *testing.T) {
// 	clog.SetVerbosity(8)
// 	if !V(7) {
// 		t.Fatalf("verbosity is broken")
// 	}
// 	Infof("test")
// 	clog.SetVerbosity(0)
// 	clog.SetVModule("log_test=8")
// 	if !V(7) {
// 		t.Fatalf("module-level verbosity broken")
// 	}
// }
