// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
