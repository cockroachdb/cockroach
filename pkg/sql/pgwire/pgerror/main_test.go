// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.
package pgerror

// Import the logging package so that tests in this package can accept the
// --vmodule flag and friends. As of 03/2017, no test in this package needs
// logging, but we want the test binary to accept the flags for uniformity
// with the other tests.
import _ "github.com/cockroachdb/cockroach/pkg/util/log"
