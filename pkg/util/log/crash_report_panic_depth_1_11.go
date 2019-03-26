// Copyright 2019 The Cockroach Authors.
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

// +build !go1.12

// This file exists to deal with the fact that the call depth during panics
// differs before and after go version 1.12. It holds the correct depth for use
// with go1.11 and prior and carries the appropriate build constraint.

package log

// The call stack here is usually:
// - ReportPanic
// - RecoverAndReport
// - panic.go
// - panic()
// so ReportPanic should pop four frames.
const depthForRecoverAndReportPanic = 4
