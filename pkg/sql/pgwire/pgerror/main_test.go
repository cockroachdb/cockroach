// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package pgerror

// Import the logging package so that tests in this package can accept the
// --vmodule flag and friends. As of 03/2017, no test in this package needs
// logging, but we want the test binary to accept the flags for uniformity
// with the other tests.
import _ "github.com/cockroachdb/cockroach/pkg/util/log"
