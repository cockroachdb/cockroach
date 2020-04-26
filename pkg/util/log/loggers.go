// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

// mainLog is the primary logger instance.
var mainLog loggerT

// stderrLog is the logger where writes performed directly
// to the stderr file descriptor (such as that performed
// by the go runtime) *may* be redirected.
// NB: whether they are actually redirected is determined
// by stderrLog.redirectInternalStderrWrites().
var stderrLog = &mainLog
