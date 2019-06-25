// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package pgerror

// Import the logging package so that tests in this package can accept the
// --vmodule flag and friends. As of 03/2017, no test in this package needs
// logging, but we want the test binary to accept the flags for uniformity
// with the other tests.
import _ "github.com/cockroachdb/cockroach/pkg/util/log"
