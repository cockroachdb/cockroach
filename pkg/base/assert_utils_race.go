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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrei Matei (andreimatei1@gmail.com)

// +build race

package base

// ExpensiveAssertionsEnabled gates assertions in our production code that we'd
// like to always run, but are too expensive. We use race-enabled builds (and
// testrace) as hints that we don't care about performance, and thus we enable
// the assertions only in such test.
const ExpensiveAssertionsEnabled = true
